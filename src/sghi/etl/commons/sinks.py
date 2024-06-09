"""Common :class:`~sghi.etl.core.Sink` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import ExitStack
from functools import update_wrapper
from logging import Logger
from typing import Final, Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Sink
from sghi.retry import Retry, noop_retry
from sghi.task import ConcurrentExecutor, Task, task
from sghi.utils import (
    ensure_callable,
    ensure_instance_of,
    ensure_not_none_nor_empty,
    ensure_predicate,
    type_fqn,
)

from .utils import fail_fast

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Type variable representing the data type after processing."""

_T = TypeVar("_T")

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]

_SinkCallable = Callable[[_PDT], None]


# =============================================================================
# TYPES
# =============================================================================


_OF_CALLABLE_LOGGER_PREFIX: Final[str] = f"{__name__}.@sink"


# =============================================================================
# DECORATORS
# =============================================================================


def sink(f: Callable[[_PDT], None]) -> Sink[_PDT]:
    """Mark/decorate a ``Callable`` as a :class:`Sink`.

    The result is that the callable is converted into a ``Sink`` instance.
    When used as a decorator, invoking the decorated callable has the same
    effect as invoking the ``drain`` method of the resulting ``Sink`` instance.

    .. important::

        The decorated callable *MUST* accept at least one argument but have at
        *MOST* one required argument (the processed data to drain/consume).

    .. note::

        The resulting values are true ``Sink`` instances that can be disposed.
        Once disposed, any attempts to invoke these instances will
        result in an :exc:`ResourceDisposedError` being raised.

    .. admonition:: Regarding retry safety
        :class: tip

        The resulting ``Sink`` is safe to retry if and only if, the decorated
        callable is safe to retry.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*
        one required argument (the processed data to drain/consume).

    :return: A ``Sink`` instance.

    :raise ValueError: If the given value is NOT a ``Callable``.
    """
    ensure_callable(f, message="A callable object is required.")

    return _SinkOfCallable(delegate_to=f)


# =============================================================================
# SINK IMPLEMENTATIONS
# =============================================================================


@final
class NullSink(Sink[_PDT], Generic[_PDT]):
    """A :class:`Sink` that discards all the data it receives.

    Like ``dev/null`` on Unix, instances of this ``Sink`` discard all data
    drained to them but report the drain operation as successful. This is
    mostly useful as a placeholder or where further consumption of processed
    data is not required.

    .. admonition:: Regarding retry safety
        :class: tip

        Instances of this ``Sink`` are idempotent and thus inherently safe to
        retry.
    """

    __slots__ = ("_is_disposed", "_logger")

    def __init__(self) -> None:
        """Create a new ``NullSink`` instance."""
        super().__init__()
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    @override
    def __enter__(self) -> Self:
        """Return ``self`` upon entering the runtime context.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :return: This instance.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def drain(self, processed_data: _PDT) -> None:
        """Discard all the received data.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param processed_data: The processed data to consume/drain.

        :return: None.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        self._logger.info("Discarding all received data.")
        # Do nothing with the received data.

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.info("Disposal complete.")


@final
class ScatterSink(Sink[_PDT], Generic[_PDT]):
    """A :class:`Sink` that drains processed data to multiple other sinks.

    This ``Sink`` implementation drains (the same) processed data to multiple
    other sinks (embedded sinks) concurrently. A suitable :class:`Executor` can
    be specified at instantiation to control the concurrent draining to the
    embedded sinks. A :class:`retry policy<Retry>` to handle transient draining
    errors to the embedded sinks can also be provided. A result gatherer
    function can be provided to specify how to handle errors while draining.

    Instances of this class are **NOT SAFE** to retry and **SHOULD NEVER** be
    retried. However, they do support retrying their embedded sinks. This is
    disabled by default but can be enabled by providing a suitable value to
    the ``retry_policy_factory`` constructor parameter when creating new
    instances. When enabled, each embedded sink will be retried individually
    per the specified retry policy in case it fails.

    Disposing instances of this class also disposes of their embedded sinks.

    .. admonition:: Regarding retry safety
        :class: caution

        Instances of this ``Sink`` are **NOT SAFE** to retry.
    """

    __slots__ = (
        "_sinks",
        "_retry_policy_factory",
        "_executor_factory",
        "_result_gatherer",
        "_is_disposed",
        "_logger",
        "_exit_stack",
        "_prepped_sinks",
        "_executor",
    )

    def __init__(
        self,
        sinks: Sequence[Sink[_PDT]],
        retry_policy_factory: Callable[[], Retry] = noop_retry,
        executor_factory: Callable[[], Executor] = ThreadPoolExecutor,
        result_gatherer: _ResultGatherer[None] = fail_fast,
    ) -> None:
        """Create a new ``ScatterSink`` of the given properties.

        :param sinks: A ``Sequence`` of sinks to drain processed data to. These
            sinks are also referred to as the embedded sinks. The given
            ``Sequence`` *MUST NOT* be empty.
        :param retry_policy_factory: A callable that supplies retry policy
            instance(s) to apply to each embedded sink. This *MUST* be a valid
            callable object. Defaults to a factory that returns retry policies
            that do nothing.
        :param executor_factory: A callable that suppliers suitable
            ``Executor`` instance(s) to use for the concurrent draining. This
            *MUST* be a valid callable object. Defaults to a factory that
            returns ``ThreadPoolExecutor`` instances.
        :param result_gatherer: A function that specifies how to handle
            draining errors. This *MUST* be a valid callable object. Defaults
            to a gatherer that fails if draining to any of the embedded sinks
            failed, or returns silently otherwise.

        :raise TypeError: If ``sinks`` is NOT a ``Sequence``.
        :raise ValueError: If ``sinks`` is empty or if
            ``retry_policy_factory``, ``executor_factory`` and
            ``result_gatherer`` are NOT callable objects.
        """
        super().__init__()
        ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=sinks,
                message="'sinks' MUST be a collections.abc.Sequence object.",
                klass=Sequence,
            ),
            message="'sinks' MUST NOT be empty.",
        )
        self._sinks: Sequence[Sink[_PDT]] = tuple(sinks)
        self._retry_policy_factory: Callable[[], Retry] = ensure_callable(
            value=retry_policy_factory,
            message="'retry_policy_factory' MUST be a callable.",
        )
        self._executor_factory: Callable[[], Executor] = ensure_callable(
            value=executor_factory,
            message="'executor_factory' MUST be a callable.",
        )
        self._result_gatherer: _ResultGatherer[None] = ensure_callable(
            value=result_gatherer,
            message="'result_gatherer' MUST be a callable.",
        )
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._exit_stack: ExitStack = ExitStack()

        # Prepare embedded sinks for execution by ensuring that they are all
        # disposed of properly once this object is disposed.
        self._prepped_sinks: Sequence[Task[_PDT, None]] = tuple(
            self._sink_to_task(self._exit_stack.push(_sink))
            for _sink in self._sinks
        )
        self._executor: ConcurrentExecutor[_PDT, None]
        self._executor = ConcurrentExecutor(
            *self._prepped_sinks, executor=self._executor_factory()
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        """Return ``self`` upon entering the runtime context.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :return: This instance.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def drain(self, processed_data: _PDT) -> None:
        """Consume the supplied processed data using all embedded sinks.

        This method drains the provided processed data to all embedded sinks
        concurrently. It then applies the result-gatherer function assigned to
        this instance (at creation) to the :class:`Future` objects resulting
        from the concurrent execution. Each of these ``Future`` objects wraps
        the result of draining each data part to an embedded sink contained in
        this ``ScatterSink``, and they preserve the same order.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param processed_data: The processed data to consume.

        :return: None.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        self._logger.info("Draining processed data to all available sinks.")

        executor = self._executor.__enter__()
        futures = executor.execute(processed_data)

        self._result_gatherer(futures)

    @override
    def dispose(self) -> None:
        """Release any underlying resources contained by this sink.

        All embedded sinks are also disposed. After this method returns
        successfully, the :attr:`is_disposed` property should return ``True``.

        .. note::
            Unless otherwise specified, trying to use methods of a
            ``Disposable`` instance decorated with the
            :func:`~sghi.disposable.not_disposed` decorator after this method
            returns should generally be considered a programming error and
            should result in a :exc:`~sghi.disposable.ResourceDisposedError`
            being raised.

            This method should be idempotent allowing it to be called more
            than once; only the first call, however, should have an effect.

        :return: None.
        """
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.info("Disposal complete.")

    def _sink_to_task(self, s: Sink[_PDT]) -> Task[_PDT, None]:
        @task
        def do_drain(processed_data: _PDT) -> None:
            _s = s.__enter__()
            drain = self._retry_policy_factory().retry(_s.drain)
            return drain(processed_data)

        return do_drain


@final
class SplitSink(Sink[Sequence[_PDT]], Generic[_PDT]):
    """A :class:`Sink` that splits processed data and drains the split data to
    multiple sinks.

    This ``Sink`` implementation takes aggregated processed data, splits it
    into its constituent data parts, and then drains each data part to each
    embedded sink concurrently. That is, each data part is mapped to each
    embedded sink before executing all the embedded sinks concurrently. As
    such, the supplied processed data's size must equal the number of embedded
    sinks contained by a sink of this type. A result gatherer function can be
    provided to specify how to handle errors while draining. A :class:`retry
    policy<Retry>` to handle transient draining errors to the embedded sinks
    can also be provided. A suitable :class:`Executor` can be specified at
    instantiation to control the concurrent draining to the embedded sinks.

    Instances of this class are **NOT SAFE** to retry and **SHOULD NEVER** be
    retried. However, they do support retrying their embedded sinks. This is
    disabled by default but can be enabled by providing a suitable value to
    the ``retry_policy_factory`` constructor parameter when creating new
    instances. When enabled, each embedded sink will be retried individually
    per the specified retry policy in case it fails.

    Disposing instances of this class also disposes of their embedded sinks.

    .. admonition:: Regarding retry safety
        :class: caution

        Instances of this ``Sink`` are **NOT SAFE** to retry.
    """  # noqa: D205

    __slots__ = (
        "_sinks",
        "_retry_policy_factory",
        "_executor_factory",
        "_result_gatherer",
        "_is_disposed",
        "_logger",
        "_exit_stack",
        "_prepped_sinks",
        "_executor",
    )

    def __init__(
        self,
        sinks: Sequence[Sink[_PDT]],
        retry_policy_factory: Callable[[], Retry] = noop_retry,
        executor_factory: Callable[[], Executor] = ThreadPoolExecutor,
        result_gatherer: _ResultGatherer[None] = fail_fast,
    ) -> None:
        """Create a new ``SplitSink`` of the given properties.

        :param sinks: A ``Sequence`` of sinks to drain each processed data part
            to. These sinks are also referred to as the embedded sinks. The
            given ``Sequence`` *MUST NOT* be empty.
        :param retry_policy_factory: A callable that supplies retry policy
            instance(s) to apply to each embedded sink. This *MUST* be a valid
            callable object. Defaults to a factory that returns retry policies
            that do nothing.
        :param executor_factory: A callable that suppliers suitable
            ``Executor`` instance(s) to use for the concurrent draining. This
            *MUST* be a valid callable object. Defaults to a factory that
            returns ``ThreadPoolExecutor`` instances.
        :param result_gatherer: A function that specifies how to handle
            draining errors. This *MUST* be a valid callable object. Defaults
            to a gatherer that fails if draining to any of the embedded sinks
            failed, or returns silently otherwise.

        :raise TypeError: If ``sinks`` is NOT a ``Sequence``.
        :raise ValueError: If ``sinks`` is empty or if
            ``retry_policy_factory``, ``executor_factory`` and
            ``result_gatherer`` are NOT callable objects.
        """
        super().__init__()
        ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=sinks,
                message="'sinks' MUST be a collections.abc.Sequence object.",
                klass=Sequence,
            ),
            message="'sinks' MUST NOT be empty.",
        )
        self._sinks: Sequence[Sink[_PDT]] = tuple(sinks)
        self._retry_policy_factory: Callable[[], Retry] = ensure_callable(
            value=retry_policy_factory,
            message="'retry_policy_factory' MUST be a callable.",
        )
        self._executor_factory: Callable[[], Executor] = ensure_callable(
            value=executor_factory,
            message="'executor_factory' MUST be a callable.",
        )
        self._result_gatherer: _ResultGatherer[None] = ensure_callable(
            value=result_gatherer,
            message="'result_gatherer' MUST be a callable.",
        )
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._exit_stack: ExitStack = ExitStack()

        # Prepare embedded sinks for execution by ensuring that they are all
        # disposed of properly once this object is disposed.
        self._prepped_sinks: Sequence[Task[Sequence[_PDT], None]] = tuple(
            self._sink_to_task(self._exit_stack.push(_sink), _i)
            for _i, _sink in enumerate(self._sinks)
        )
        self._executor: ConcurrentExecutor[Sequence[_PDT], None]
        self._executor = ConcurrentExecutor(
            *self._prepped_sinks, executor=self._executor_factory()
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        """Return ``self`` upon entering the runtime context.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :return: This instance.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def drain(self, processed_data: Sequence[_PDT]) -> None:
        """Split the supplied processed data and consume each data part.

        This method decomposes the provided aggregated processed data into its
        constituent data parts and maps each data part to an embedded sink;
        before executing all the embedded sinks concurrently. It then applies
        the result-gatherer function assigned to this instance (at creation) to
        the :class:`Future` objects resulting from the concurrent execution.
        Each of these ``Future`` objects wraps the result of draining each data
        part to an embedded sink contained in this ``SplitSink``, and they
        preserve the same order.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param processed_data: The aggregated processed data to consume. This
            *MUST* be a ``Sequence`` of processed data values whose size *MUST
            EQUAL* the number of embedded sinks contained by this
            ``SplitSink``.

        :return: None.

        :raise ResourceDisposedError: If this sink has already been disposed.
        :raise TypeError: If ``processed_data`` *IS NOT* a ``Sequence``.
        :raise ValueError: If the size of ``processed_data`` *DOES NOT EQUAL*
            the number of embedded sinks in this ``SplitSink``.
        """
        ensure_instance_of(
            value=processed_data,
            klass=Sequence,
            message=(
                "'processed_data' MUST be a collections.abc.Sequence object."
            ),
        )
        ensure_predicate(
            test=len(processed_data) == len(self._sinks),
            message=(
                f"Expected 'processed_data' to be of size {len(self._sinks)} "
                f"but got size {len(processed_data)} instead."
            ),
            exc_factory=ValueError,
        )
        self._logger.info(
            "Splitting aggregated processed data and draining each data part "
            "to all available sinks."
        )

        executor = self._executor.__enter__()
        futures = executor.execute(processed_data)

        self._result_gatherer(futures)

    @override
    def dispose(self) -> None:
        """Release any underlying resources contained by this sink.

        All embedded sinks are also disposed. After this method returns
        successfully, the :attr:`is_disposed` property should return ``True``.

        .. note::
            Unless otherwise specified, trying to use methods of a
            ``Disposable`` instance decorated with the
            :func:`~sghi.disposable.not_disposed` decorator after this method
            returns should generally be considered a programming error and
            should result in a :exc:`~sghi.disposable.ResourceDisposedError`
            being raised.

            This method should be idempotent allowing it to be called more
            than once; only the first call, however, should have an effect.

        :return: None.
        """
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.info("Disposal complete.")

    def _sink_to_task(
        self,
        s: Sink[_PDT],
        i: int,
    ) -> Task[Sequence[_PDT], None]:
        @task
        def do_drain(processed_data: Sequence[_PDT]) -> None:
            _s = s.__enter__()
            drain = self._retry_policy_factory().retry(_s.drain)
            return drain(processed_data[i])

        return do_drain


@final
class _SinkOfCallable(Sink[_PDT], Generic[_PDT]):
    __slots__ = ("_delegate_to", "_is_disposed", "_logger")

    def __init__(self, delegate_to: _SinkCallable[_PDT]) -> None:
        super().__init__()
        ensure_callable(
            value=delegate_to,
            message="'delegate_to' MUST be a callable object.",
        )
        self._delegate_to: _SinkCallable[_PDT] = delegate_to
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(
            f"{_OF_CALLABLE_LOGGER_PREFIX}({type_fqn(self._delegate_to)})"
        )
        update_wrapper(self, self._delegate_to)

    @not_disposed
    @override
    def __enter__(self) -> Self:
        """Return ``self`` upon entering the runtime context.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :return: This instance.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def drain(self, processed_data: _PDT) -> None:
        """Delegate consumption of the processed data to the wrapped callable.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param processed_data: The processed data to consume/drain.

        :return: None.

        :raise ResourceDisposedError: If this sink has already been disposed.
        """
        self._logger.info("Delegating to '%s'.", type_fqn(self._delegate_to))
        self._delegate_to(processed_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.info("Disposal complete.")


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "NullSink",
    "ScatterSink",
    "SplitSink",
    "sink",
]
