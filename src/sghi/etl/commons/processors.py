"""Common :class:`~sghi.etl.core.Processor` implementations."""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import ExitStack
from functools import update_wrapper
from logging import Logger
from typing import Any, Final, Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Processor
from sghi.retry import Retry, noop_retry
from sghi.task import ConcurrentExecutor, Task, pipe, task
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

_RDT = TypeVar("_RDT")
"""Type variable representing the raw data type."""

_T = TypeVar("_T")

_ProcessorCallable = Callable[[_RDT], _PDT]

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]


# =============================================================================
# CONSTANTS
# =============================================================================


_OF_CALLABLE_LOGGER_PREFIX: Final[str] = f"{__name__}.@processor"


# =============================================================================
# DECORATORS
# =============================================================================


def processor(f: Callable[[_RDT], _PDT]) -> Processor[_RDT, _PDT]:
    """Mark/decorate a ``Callable`` as a :class:`Processor`.

    The result is that the callable is converted into a ``Processor`` instance.
    When used as a decorator, invoking the decorated callable has the same
    effect as invoking the ``apply`` method of the resulting ``Processor``
    instance.

    .. important::

        The decorated callable *MUST* accept at least one argument but have
        at *MOST* one required argument.

    .. note::

        The resulting values are true ``Processor`` instances that can be
        disposed. Once disposed, any attempts to invoke these instances will
        result in an :exc:`ResourceDisposedError` being raised.

    .. admonition:: Regarding retry safety
        :class: tip

        The resulting ``Processor`` is safe to retry if and only if, the
        decorated callable is safe to retry.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*
        one required argument (the raw data to be processed).

    :return: A ``Processor`` instance.

    :raise ValueError: If the given value is NOT a ``Callable``.
    """
    ensure_callable(f, message="A callable object is required.")

    return _ProcessorOfCallable(delegate_to=f)


# =============================================================================
# PROCESSOR IMPLEMENTATIONS
# =============================================================================


@final
class NOOPProcessor(Processor[_RDT, _RDT], Generic[_RDT]):
    """A ``Processor`` that simply returns the received raw data as it is.

    This :class:`~sghi.etl.core.Processor` subclass is a "no-operation" (NOOP)
    processor. When the :meth:`apply` method is invoked on its instances, it
    returns the received raw data (without any modifications) as processed data
    to its caller. This can be useful as a placeholder processor or for
    situations where data transformation is not needed.

    .. admonition:: Regarding retry safety
        :class: tip

        Instances of this ``Processor`` are idempotent and thus inherently safe
        to retry.
    """

    __slots__ = ("_is_disposed", "_logger")

    def __init__(self) -> None:
        """Create a ``NOOPProcessor`` instance."""
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

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def apply(self, raw_data: _RDT) -> _RDT:
        """Apply the processing logic (NOOP in this case).

        Return the received raw data without any modifications to the caller.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param raw_data: The data to be processed.

        :return: The raw data itself (without any modifications).

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        self._logger.info("Skipping data processing. Return raw data as is.")
        return raw_data

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.info("Disposal complete.")


@final
class ProcessorPipe(Processor[_RDT, _PDT], Generic[_RDT, _PDT]):
    """A :class:`Processor` that pipes raw data to other embedded processors.

    This ``Processor`` pipes the raw data applied to it through a series of
    other ``Processor`` instances, passing the output of one ``Processor`` as
    the input to the next. If an unhandled error occurs in one of the embedded
    processors, the entire pipeline fails and propagates the error to the
    caller.

    Instances of this class are **NOT SAFE** to retry and **SHOULD NEVER** be
    retried. However, they do support retrying their embedded processors. This
    is disabled by default but can be enabled by providing a suitable value to
    the ``retry_policy_factory`` constructor parameter when creating new
    instances. When enabled, each embedded processor will be retried
    individually per the specified retry policy in case it fails.

    Disposing instances of this class also disposes of their embedded
    processors.

    .. admonition:: Regarding retry safety
        :class: caution

        Instances of this ``Processor`` are **NOT SAFE** to retry.
    """

    __slots__ = (
        "_exit_stack",
        "_is_disposed",
        "_logger",
        "_prepped_processors",
        "_processors",
        "_retry_policy_factory",
    )

    def __init__(
        self,
        processors: Sequence[Processor[Any, Any]],
        retry_policy_factory: Callable[[], Retry] = noop_retry,
    ) -> None:
        """Create a new ``ProcessorPipe`` instance with the given properties.

        :param processors: A ``Sequence`` of processors to pipe the raw data
            applied to this processor. This *MUST NOT* be empty.
        :param retry_policy_factory: A function that supplies retry policy
            instance(s) to apply to each embedded processor. This MUST be a
            callable object. Defaults to a factory that returns retry policies
            that do nothing.

        :raise ValueError: If ``processors`` is ``None`` or empty, or if
            ``retry_policy_factory`` is NOT a callable object.
        """
        super().__init__()
        ensure_not_none_nor_empty(
            value=processors,
            message="'processors' MUST NOT be None or empty.",
        )
        self._processors: Sequence[Processor[Any, Any]]
        self._processors = tuple(processors)
        self._retry_policy_factory: Callable[[], Retry] = ensure_callable(
            value=retry_policy_factory,
            message="'retry_policy_factory' MUST be a callable.",
        )
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._exit_stack: ExitStack = ExitStack()

        # Prepare embedded processors for execution by ensuring that they are
        # all disposed of properly once this object is disposed.
        self._prepped_processors: Sequence[Task[Any, Any]] = tuple(
            self._processor_to_task(self._exit_stack.push(_processor))
            for _processor in self._processors
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

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def apply(self, raw_data: _RDT) -> _PDT:
        """Pipe the given raw data through all the embedded processors.

        The output of each embedded ``Processor`` becomes the input to the next
        one. The result of the final ``Processor`` is the output of this apply
        operation. If an unhandled error occurs in one of the embedded
        processors, the entire operation fails and propagates the error to the
        caller.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param raw_data: The data to be processed.

        :return: The processed data after being piped through the embedded
            processors.

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        self._logger.info("Piping received data through all processors.")
        return pipe(*self._prepped_processors).execute(raw_data)

    @override
    def dispose(self) -> None:
        """Release any underlying resources contained by this processor.

        All embedded processors are also disposed. After this method returns
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
        self._logger.info("Disposal complete.")

    def _processor_to_task(self, p: Processor[_RDT, _PDT]) -> Task[_RDT, _PDT]:
        @task
        def do_apply(raw_data: _RDT) -> _PDT:
            _p = p.__enter__()
            apply = self._retry_policy_factory().retry(_p.apply)
            return apply(raw_data)

        return do_apply


pipe_processors = ProcessorPipe


@final
class ScatterGatherProcessor(
    Processor[_RDT, Sequence[_PDT]], Generic[_RDT, _PDT]
):
    """A :class:`Processor` that applies raw data to multiple other processors.

    This ``Processor`` implementation applies multiple other processors
    (embedded processors) to the same raw data, and then returns the aggregated
    outputs of these processors. The applications of the embedded processors to
    the raw data are performed concurrently. A suitable :class:`Executor` can
    be specified at instantiation to control the concurrent execution of the
    embedded processors. A result gatherer function can be provided to specify
    how to handle processing errors and/or which results from the embedded
    processors to pick. A :class:`retry policy<Retry>` to handle transient
    processing errors of the embedded processors can also be provided.

    Instances of this class are **NOT SAFE** to retry and **SHOULD NEVER** be
    retried. However, they do support retrying their embedded processors. This
    is disabled by default but can be enabled by providing a suitable value to
    the ``retry_policy_factory`` constructor parameter when creating new
    instances. When enabled, each embedded processor will be retried
    individually per the specified retry policy in case it fails.

    Disposing instances of this class also disposes of their embedded
    processors.

    .. admonition:: Regarding retry safety
        :class: caution

        Instances of this ``Processor`` are **NOT SAFE** to retry.
    """

    __slots__ = (
        "_executor",
        "_executor_factory",
        "_exit_stack",
        "_is_disposed",
        "_logger",
        "_prepped_processors",
        "_processors",
        "_result_gatherer",
        "_retry_policy_factory",
    )

    def __init__(
        self,
        processors: Sequence[Processor[_RDT, _PDT]],
        retry_policy_factory: Callable[[], Retry] = noop_retry,
        executor_factory: Callable[[], Executor] = ThreadPoolExecutor,
        result_gatherer: _ResultGatherer[_PDT] = fail_fast,
    ) -> None:
        """Create a new ``ScatterGatherProcessor`` of the given properties.

        :param processors: A ``Sequence`` of processors to be applied to the
            raw data. These processors are also referred to as the embedded
            processors. The given ``Sequence`` *MUST NOT* be empty.
        :param retry_policy_factory: A callable that supplies retry policy
            instance(s) to apply to each embedded processor. This *MUST* be a
            valid callable object. Defaults to a factory that returns retry
            policies that do nothing.
        :param executor_factory: A callable that suppliers suitable
            ``Executor`` instance(s) to use for the concurrent processing. This
            *MUST* be a valid callable object. Defaults to a factory that
            returns ``ThreadPoolExecutor`` instances.
        :param result_gatherer: A function that specifies how to handle
            processing errors and/or which results from the embedded processors
            to pick. This *MUST* be a valid callable object. Defaults to a
            gatherer that fails if applying any of the embedded processors
            failed, or returns all the processed data otherwise.

        :raise TypeError: If ``processors`` is NOT a ``Sequence``.
        :raise ValueError: If ``processors`` is empty or if
            ``retry_policy_factory``, ``executor_factory`` and
            ``result_gatherer`` are NOT callable objects.
        """
        super().__init__()
        ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=processors,
                message=(
                    "'processors' MUST be a collections.abc.Sequence object."
                ),
                klass=Sequence,
            ),
            message="'processors' MUST NOT be empty.",
        )
        self._processors: Sequence[Processor[_RDT, _PDT]] = tuple(processors)
        self._retry_policy_factory: Callable[[], Retry] = ensure_callable(
            value=retry_policy_factory,
            message="'retry_policy_factory' MUST be a callable.",
        )
        self._executor_factory: Callable[[], Executor] = ensure_callable(
            value=executor_factory,
            message="'executor_factory' MUST be a callable.",
        )
        self._result_gatherer: _ResultGatherer[_PDT] = ensure_callable(
            value=result_gatherer,
            message="'result_gatherer' MUST be a callable.",
        )
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._exit_stack: ExitStack = ExitStack()

        # Prepare embedded processors for execution by ensuring that they are
        # all disposed of properly once this object is disposed.
        self._prepped_processors: Sequence[Task[Any, Any]] = tuple(
            self._processor_to_task(self._exit_stack.push(_processor))
            for _processor in self._processors
        )
        self._executor: ConcurrentExecutor[_RDT, _PDT]
        self._executor = ConcurrentExecutor(
            *self._prepped_processors, executor=self._executor_factory()
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

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def apply(self, raw_data: _RDT) -> Sequence[_PDT]:
        """Process the supplied raw data using all embedded processors and
        return the results.

        This method concurrently applies all the embedded processors to the
        supplied raw data. It then applies the results-gatherer function
        assigned to this instance (at creation) to the :class:`Future` objects
        resulting from the concurrent execution. Each of these ``Future``
        objects wraps the result of processing the supplied raw data using an
        embedded processor contained in this ``ScatterGatherProcessor``, and
        they preserve the same order.

        The contents of the resulting aggregate, and their ordering, are
        determined by the provided result-gatherer function. That is, the
        contents of the returned ``Sequence``, will be the same as those
        returned by this object's result-gatherer function and in the same
        order.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param raw_data: The data to be processed.

        :return: The aggregated results of applying all the embedded processors
            to the provided raw data.

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """  # noqa: D205
        self._logger.info(
            "Forking processing of the received data to all embedded "
            "processors."
        )
        executor = self._executor.__enter__()
        futures = executor.execute(raw_data)

        return tuple(self._result_gatherer(futures))

    @override
    def dispose(self) -> None:
        """Release any underlying resources contained by this processor.

        All embedded processors are also disposed. After this method returns
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

    def _processor_to_task(self, p: Processor[_RDT, _PDT]) -> Task[_RDT, _PDT]:
        @task
        def do_apply(raw_data: _RDT) -> _PDT:
            _p = p.__enter__()
            apply = self._retry_policy_factory().retry(_p.apply)
            return apply(raw_data)

        return do_apply


@final
class SplitGatherProcessor(
    Processor[Sequence[_RDT], Sequence[_PDT]], Generic[_RDT, _PDT]
):
    """A :class:`Processor` that splits raw data and applies multiple
    processors to the split data.

    This ``Processor`` implementation takes aggregated raw data, splits it into
    its constituent data parts, and then processes each data part concurrently.
    This is achieved by mapping each data part to an embedded processor before
    executing all the embedded processors concurrently. As such, the supplied
    raw data's size must equal the number of embedded processors contained by a
    processor of this type. A result gatherer function can be provided to
    specify how to handle processing errors and/or which results from the
    embedded processors to pick. A :class:`retry policy<Retry>` to handle
    transient processing errors of the embedded processors can also be
    provided. A suitable :class:`Executor` can be specified at instantiation to
    control the concurrent application of the embedded processors.

    Instances of this class are **NOT SAFE** to retry and **SHOULD NEVER** be
    retried. However, they do support retrying their embedded processors. This
    is disabled by default but can be enabled by providing a suitable value to
    the ``retry_policy_factory`` constructor parameter when creating new
    instances. When enabled, each embedded processor will be retried
    individually per the specified retry policy in case it fails.

    Disposing instances of this class also disposes of their embedded
    processors.

    .. admonition:: Regarding retry safety
        :class: caution

        Instances of this ``Processor`` are **NOT SAFE** to retry.
    """  # noqa: D205

    __slots__ = (
        "_executor",
        "_executor_factory",
        "_exit_stack",
        "_is_disposed",
        "_logger",
        "_prepped_processors",
        "_processors",
        "_result_gatherer",
        "_retry_policy_factory",
    )

    def __init__(
        self,
        processors: Sequence[Processor[_RDT, _PDT]],
        retry_policy_factory: Callable[[], Retry] = noop_retry,
        executor_factory: Callable[[], Executor] = ThreadPoolExecutor,
        result_gatherer: _ResultGatherer[_PDT] = fail_fast,
    ) -> None:
        """Create a new ``SplitGatherProcessor`` of the given properties.

        :param processors: A ``Sequence`` of processors to apply to each raw
            data part. These processors are also referred to as the embedded
            processors. The given ``Sequence`` *MUST NOT* be empty.
        :param retry_policy_factory: A callable that supplies retry policy
            instance(s) to apply to each embedded processor. This *MUST* be a
            valid callable object. Defaults to a factory that returns retry
            policies that do nothing.
        :param executor_factory: A callable that suppliers suitable
            ``Executor`` instance(s) to use for the concurrent processing. This
            *MUST* be a valid callable object. Defaults to a factory that
            returns ``ThreadPoolExecutor`` instances.
        :param result_gatherer: A function that specifies how to handle
            processing errors and/or which results from the embedded processors
            to pick. This *MUST* be a valid callable object. Defaults to a
            gatherer that fails if applying any of the embedded processors
            failed, or returns all the processed data otherwise.

        :raise TypeError: If ``processors`` is NOT a ``Sequence``.
        :raise ValueError: If ``processors`` is empty or if
            ``retry_policy_factory``, ``executor_factory`` and
            ``result_gatherer`` are NOT callable objects.
        """
        super().__init__()
        ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=processors,
                message=(
                    "'processors' MUST be a collections.abc.Sequence object."
                ),
                klass=Sequence,
            ),
            message="'processors' MUST NOT be empty.",
        )
        self._processors: Sequence[Processor[_RDT, _PDT]] = tuple(processors)
        self._retry_policy_factory: Callable[[], Retry] = ensure_callable(
            value=retry_policy_factory,
            message="'retry_policy_factory' MUST be a callable.",
        )
        self._executor_factory: Callable[[], Executor] = ensure_callable(
            value=executor_factory,
            message="'executor_factory' MUST be a callable.",
        )
        self._result_gatherer: _ResultGatherer[_PDT] = ensure_callable(
            value=result_gatherer,
            message="'result_gatherer' MUST be a callable.",
        )
        self._is_disposed: bool = False
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._exit_stack: ExitStack = ExitStack()

        # Prepare embedded processors for execution by ensuring that they are
        # all disposed of properly once this object is disposed.
        self._prepped_processors: Sequence[Task[Any, Any]] = tuple(
            self._processor_to_task(self._exit_stack.push(_processor), _i)
            for _i, _processor in enumerate(self._processors)
        )
        self._executor: ConcurrentExecutor[Sequence[_RDT], _PDT]
        self._executor = ConcurrentExecutor(
            *self._prepped_processors, executor=self._executor_factory()
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

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def apply(self, raw_data: Sequence[_RDT]) -> Sequence[_PDT]:
        """Split the supplied raw data, process it and return the results.

        This method decomposes the provided aggregated raw data into its
        constituent data parts and maps each data part to an embedded
        processor; before executing all the embedded processors concurrently.
        It then applies the result-gatherer function assigned to this instance
        (at creation) to the :class:`Future` objects resulting from the
        concurrent execution. Each of these ``Future`` objects wraps the result
        of processing each data part using an embedded processor contained in
        this ``SplitGatherProcessor``, and they preserve the same order.

        The contents of the resulting aggregate, and their ordering, are
        determined by the provided result-gatherer function. That is, the
        contents of the returned ``Sequence``, will be the same as those
        returned by this object's result-gatherer function and in the same
        order.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param raw_data: The aggregated raw data to process. This *MUST* be a
            ``Sequence`` of raw data values whose size *MUST EQUAL* the number
            of embedded processors contained by this ``SplitGatherProcessor``.

        :return: The aggregated results of applying the embedded processors to
            the provided raw data.

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        :raise TypeError: If ``raw_data`` *IS NOT* a ``Sequence``.
        :raise ValueError: If the size of ``raw_data`` *DOES NOT EQUAL* the
            number of embedded processors in this ``SplitGatherProcessor``.
        """
        ensure_instance_of(
            value=raw_data,
            klass=Sequence,
            message="'raw_data' MUST be a collections.abc.Sequence object.",
        )
        ensure_predicate(
            test=len(raw_data) == len(self._processors),
            message=(
                f"Expected 'raw_data' to be of size {len(self._processors)} "
                f"but got size {len(raw_data)} instead."
            ),
            exc_factory=ValueError,
        )
        self._logger.info(
            "Splitting aggregated raw data and applying available processors "
            "to each data part."
        )

        executor = self._executor.__enter__()
        futures = executor.execute(raw_data)

        return tuple(self._result_gatherer(futures))

    @override
    def dispose(self) -> None:
        """Release any underlying resources contained by this processor.

        All embedded processors are also disposed. After this method returns
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

    def _processor_to_task(
        self,
        p: Processor[_RDT, _PDT],
        i: int,
    ) -> Task[Sequence[_RDT], _PDT]:
        @task
        def do_apply(raw_data: Sequence[_RDT]) -> _PDT:
            _p = p.__enter__()
            apply = self._retry_policy_factory().retry(_p.apply)
            return apply(raw_data[i])

        return do_apply


@final
class _ProcessorOfCallable(Processor[_RDT, _PDT], Generic[_RDT, _PDT]):
    # See: https://github.com/python/cpython/pull/106771
    if sys.version_info[:3] >= (3, 13, 0):  # pragma: no cover
        __slots__ = ("__dict__", "_delegate_to", "_is_disposed", "_logger")
    else:  # pragma: no cover
        __slots__ = ("_delegate_to", "_is_disposed", "_logger")

    def __init__(self, delegate_to: _ProcessorCallable[_RDT, _PDT]) -> None:
        super().__init__()
        ensure_callable(
            value=delegate_to,
            message="'delegate_to' MUST be a callable object.",
        )
        self._delegate_to: _ProcessorCallable[_RDT, _PDT] = delegate_to
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

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def apply(self, raw_data: _RDT) -> _PDT:
        """Delegate processing to the wrapped callable.

        .. admonition:: Don't use after dispose
            :class: error

            Invoking this method on an instance that is disposed(i.e. the
            :attr:`is_disposed` property on the instance is ``True``) will
            result in a :exc:`ResourceDisposedError` being raised.

        :param raw_data: The data to be processed.

        :return: The processed data as returned by the wrapped callable.

        :raise ResourceDisposedError: If this processor has already been
            disposed.
        """
        self._logger.info("Delegating to '%s'.", type_fqn(self._delegate_to))
        return self._delegate_to(raw_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.info("Disposal complete.")


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "NOOPProcessor",
    "ProcessorPipe",
    "ScatterGatherProcessor",
    "SplitGatherProcessor",
    "pipe_processors",
    "processor",
]
