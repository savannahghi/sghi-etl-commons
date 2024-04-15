"""Common :class:`~sghi.etl.core.Sink` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import update_wrapper
from logging import Logger
from typing import Final, Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Sink
from sghi.utils import ensure_callable, type_fqn

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
""""Type variable representing the data type after processing."""

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

    Like to ``dev/null`` on Unix, instances of this ``Sink`` discard all data
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
    "sink",
]
