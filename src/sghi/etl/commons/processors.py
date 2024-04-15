"""Common :class:`~sghi.etl.core.Processor` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from functools import update_wrapper
from logging import Logger
from typing import Any, Final, Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Processor
from sghi.retry import Retry, noop_retry
from sghi.task import Task, pipe, task
from sghi.utils import ensure_callable, ensure_not_none_nor_empty, type_fqn

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
""""Type variable representing the data type after processing."""

_RDT = TypeVar("_RDT")
"""Type variable representing the raw data type."""

_ProcessorCallable = Callable[[_RDT], _PDT]


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
        :class: tip

        Instances of this ``Processor`` are **NOT SAFE** to retry.
    """

    __slots__ = (
        "_processors",
        "_retry_policy_factory",
        "_is_disposed",
        "_logger",
        "_exit_stack",
        "_prepped_processors",
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
            with p as _p:
                apply = self._retry_policy_factory().retry(_p.apply)
                return apply(raw_data)

        return do_apply


pipe_processors = ProcessorPipe


@final
class _ProcessorOfCallable(Processor[_RDT, _PDT], Generic[_RDT, _PDT]):
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
    "pipe_processors",
    "processor",
]
