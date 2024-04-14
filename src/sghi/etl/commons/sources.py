"""Common :class:`~sghi.etl.core.Source` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import update_wrapper
from logging import Logger
from typing import Final, Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Source
from sghi.utils import ensure_callable, type_fqn

# =============================================================================
# TYPES
# =============================================================================


_RDT = TypeVar("_RDT")
"""Raw Data Type."""

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
    "source",
]
