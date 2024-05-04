"""Common :class:`~sghi.etl.core.Source` implementations."""

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
from sghi.etl.core import Source
from sghi.retry import Retry, noop_retry
from sghi.task import ConcurrentExecutor, Supplier, supplier
from sghi.utils import (
    ensure_callable,
    ensure_instance_of,
    ensure_not_none_nor_empty,
    type_fqn,
)

from .utils import fail_fast

# =============================================================================
# TYPES
# =============================================================================


_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_T = TypeVar("_T")

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]

_SourceCallable = Callable[[], _RDT]


# =============================================================================
# TYPES
# =============================================================================


_OF_CALLABLE_LOGGER_PREFIX: Final[str] = f"{__name__}.@source"


# =============================================================================
# DECORATORS
# =============================================================================


def source(f: Callable[[], _RDT]) -> Source[_RDT]:
    """Mark/decorate a ``Callable`` as a :class:`Source`.

    The result is that the callable is converted into a ``Source`` instance.
    When used as a decorator, invoking the decorated callable has the same
    effect as invoking the ``draw`` method of the resulting ``Source``
    instance.

    .. important::

        The decorated callable *MUST NOT* have any required arguments but MUST
        return a value (the drawn data).

    .. note::

        The resulting values are true ``Source`` instances that can be
        disposed. Once disposed, any attempts to invoke these instances will
        result in an :exc:`ResourceDisposedError` being raised.

    .. admonition:: Regarding retry safety
        :class: tip

        The resulting ``Source`` is safe to retry if and only if, the
        decorated callable is safe to retry.

    :param f: The callable to be decorated. The callable *MUST NOT* have any
        required arguments but *MUST* return a value (the drawn data).

    :return: A ``Source`` instance.

    :raise ValueError: If the given value is NOT a ``Callable``.
    """
    ensure_callable(f, message="A callable object is required.")

    return _SourceOfCallable(delegate_to=f)


# =============================================================================
# SOURCE IMPLEMENTATIONS
# =============================================================================


@final
class GatherSource(Source[Sequence[_RDT]], Generic[_RDT]):
    """A :class:`Source` that aggregates raw data from multiple sources.

    This ``Source`` implementation asynchronously draws data from multiple
    other sources (embedded sources) and returns the aggregated results. A
    result gatherer function can be provided to specify how to handle draw
    errors and/or which results from the embedded sources to pick. A
    :class:`retry policy<Retry>` to handle transient draw errors of the
    embedded sources can also be provided. A suitable :class:`Executor` can be
    specified at instantiation to control the asynchronous draw from the
    embedded sources.

    Instances of this class are **NOT SAFE** to retry and **SHOULD NEVER** be
    retried. However, they do support retrying their embedded sources. This
    is disabled by default but can be enabled by providing a suitable value to
    the ``retry_policy_factory`` constructor parameter when creating new
    instances. When enabled, each embedded source will be retried individually
    per the specified retry policy in case it fails.

    Disposing instances of this class also disposes of their embedded sources.

    .. admonition:: Regarding retry safety
        :class: tip

        Instances of this ``Source`` are **NOT SAFE** to retry.
    """

    __slots__ = (
        "_sources",
        "_retry_policy_factory",
        "_executor_factory",
        "_result_gatherer",
        "_is_disposed",
        "_logger",
        "_exit_stack",
        "_prepped_sources",
        "_executor",
    )

    def __init__(
        self,
        sources: Sequence[Source[_RDT]],
        retry_policy_factory: Callable[[], Retry] = noop_retry,
        executor_factory: Callable[[], Executor] = ThreadPoolExecutor,
        result_gatherer: _ResultGatherer[_RDT] = fail_fast,
    ) -> None:
        """Create a new ``GatherSource`` instance with the given properties.

        :param sources: A ``Sequence`` of sources to asynchronously draw data
            from. These sources are also referred to as the embedded sources.
            The given ``Sequence`` *MUST NOT* be empty.
        :param retry_policy_factory: A callable that supplies retry policy
            instance(s) to apply to each embedded source. This MUST be a valid
            callable object. Defaults to a factory that returns retry policies
            that do nothing.
        :param executor_factory: A callable that suppliers suitable
            ``Executor`` instance(s) to use for the asynchronous draws. This
            MUST be a valid callable object. Defaults to a factory that returns
            ``ThreadPoolExecutor`` instances.
        :param result_gatherer: A function that specifies how to handle draw
            errors and/or which results from the embedded sources to pick. This
            MUST be a valid callable object. Defaults to a gatherer that fails
            if drawing from any of the embedded sources failed, or returns all
            the drawn data otherwise.

        :raise TypeError: If ``source`` is NOT a ``Sequence``.
        :raise ValueError: If ``sources`` is empty or if
            ``retry_policy_factory``, ``executor_factory`` and
            ``result_gatherer`` are NOT callable objects.
        """
        super().__init__()
        ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=sources,
                message="'sources' MUST be a collections.abc.Sequence object.",
                klass=Sequence,
            ),
            message="'sources' MUST NOT be empty.",
        )
        self._sources: Sequence[Source[_RDT]] = tuple(sources)
        self._retry_policy_factory: Callable[[], Retry] = ensure_callable(
            value=retry_policy_factory,
            message="'retry_policy_factory' MUST be a callable.",
        )
        self._executor_factory: Callable[[], Executor] = ensure_callable(
            value=executor_factory,
            message="'executor_factory' MUST be a callable.",
        )
        self._result_gatherer: _ResultGatherer[_RDT] = ensure_callable(
            value=result_gatherer,
            message="'result_gatherer' MUST be a callable.",
        )
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._exit_stack: ExitStack = ExitStack()

        # Prepare embedded sources for execution by ensuring that they are all
        # disposed of properly once this object is disposed.
        self._prepped_sources: Sequence[Supplier[_RDT]] = tuple(
            self._source_to_task(self._exit_stack.push(_source))
            for _source in self._sources
        )
        self._executor: ConcurrentExecutor[None, _RDT] = ConcurrentExecutor(
            *self._prepped_sources, executor=self._executor_factory()
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

        :raise ResourceDisposedError: If this source has already been disposed.
        """
        return super(Source, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> Sequence[_RDT]:
        """Draw data from embedded sources and return the aggregated results.

        The contents of the resulting aggregate, and their ordering, are
        determined by the result-gatherer function provided at this object's
        instantiation. That is, the contents of the returned ``Sequence``, will
        be the same as those returned by this object's result-gatherer function
        and in the same order.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :return: The aggregated results of drawing from each embedded source.

        :raise ResourceDisposedError: If this source has already been disposed.
        """
        self._logger.info("Aggregating data from all available sources.")

        with self._executor as executor:
            futures = executor.execute(None)

        return tuple(self._result_gatherer(futures))

    @override
    def dispose(self) -> None:
        """Release any underlying resources contained by this source.

        All embedded sources are also disposed. After this method returns
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

    def _source_to_task(self, s: Source[_RDT]) -> Supplier[_RDT]:
        @supplier
        def do_draw() -> _RDT:
            with s as _s:
                draw = self._retry_policy_factory().retry(_s.draw)
                return draw()

        # noinspection PyTypeChecker
        return do_draw


@final
class _SourceOfCallable(Source[_RDT], Generic[_RDT]):
    __slots__ = ("_delegate_to", "_is_disposed", "_logger")

    def __init__(self, delegate_to: _SourceCallable[_RDT]) -> None:
        super().__init__()
        ensure_callable(
            value=delegate_to,
            message="'delegate_to' MUST be a callable object.",
        )
        self._delegate_to: _SourceCallable[_RDT] = delegate_to
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

        :raise ResourceDisposedError: If this source has already been disposed.
        """
        return super(Source, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> _RDT:
        """Delegate data retrival to the wrapped callable.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :return: The drawn, raw data as returned by the wrapped callable.

        :raise ResourceDisposedError: If this source has already been disposed.
        """
        self._logger.info("Delegating to '%s'.", type_fqn(self._delegate_to))
        return self._delegate_to()

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.info("Disposal complete.")


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "GatherSource",
    "source",
]
