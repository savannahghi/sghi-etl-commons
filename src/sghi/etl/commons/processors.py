"""Common :class:`~sghi.etl.core.Processor` implementations."""

from __future__ import annotations

import logging
from logging import Logger
from typing import Generic, Self, TypeVar, final

from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Processor
from sghi.utils import type_fqn

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")  # noqa: PYI018
""""Type variable representing the data type after processing."""

_RDT = TypeVar("_RDT")
"""Type variable representing the raw data type."""


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


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "NOOPProcessor",
]
