"""Common :class:`~sghi.etl.core.Processor` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import update_wrapper
from logging import Logger
from typing import Final, Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Processor
from sghi.utils import ensure_callable, type_fqn

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
    "processor",
]
