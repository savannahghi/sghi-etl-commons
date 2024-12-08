"""Common :class:`sghi.etl.core.WorkflowDefinition` implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar, final

from typing_extensions import override

from sghi.etl.core import Processor, Sink, Source, WorkflowDefinition
from sghi.utils import (
    ensure_callable,
    ensure_instance_of,
    ensure_not_none_nor_empty,
    ensure_optional_instance_of,
)

from .processors import NOOPProcessor
from .sinks import NullSink

if TYPE_CHECKING:
    from collections.abc import Callable


# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Type variable representing the data type after processing."""

_RDT = TypeVar("_RDT")
"""Type variable representing the raw data type."""


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@final
class SimpleWorkflowDefinition(
    WorkflowDefinition[_RDT, _PDT],
    Generic[_RDT, _PDT],
):
    """A simple :class:`WorkflowDefinition` implementation."""

    def __init__(  # noqa: PLR0913
        self,
        id: str,  # noqa: A002
        name: str,
        source_factory: Callable[[], Source[_RDT]],
        description: str | None = None,
        processor_factory: Callable[[], Processor[_RDT, _PDT]] = NOOPProcessor,
        sink_factory: Callable[[], Sink[_PDT]] = NullSink,
    ) -> None:
        """Create a new ``WorkflowDefinition`` with the provided properties.

        The ``id``, ``name`` and ``source_factory`` parameters are mandatory
        and MUST be valid (see individual parameter docs for details).
        Providing invalid parameters will lead to either a :exc:`TypeError` (
        for values of the wrong type) or :exc:`ValueError` being raised.

        :param id: A unique identifier to assign to the created workflow.
            This MUST be a non-empty string.
        :param name: A name to assign to the created workflow. This MUST be
            a non-empty string.
        :param source_factory: A function that suppliers the ``Source``
            associated with the created workflow. This MUST be a valid
            callable.
        :param description: An optional description to assign to the created
            workflow. This MUST be a valid string when NOT ``None``. Defaults
            to ``None`` when not provided.
        :param processor_factory: An optional function that suppliers the
            ``Processor`` associated with the created workflow. This MUST be a
            valid callable. Defaults to ``NOOPProcessor`` when not provided.
        :param sink_factory: A function that suppliers the ``Sink`` associated
            with the created workflow. This MUST be a valid callable. Defaults
            to ``NullSink`` when not provided.

        :raise TypeError: If ``id`` or ``name`` are NOT strings, or if
            ``description`` is provided but is NOT a string.
        :raise ValueError: If one of the following is ``True``; ``id`` is an
            empty string, ``name`` is an empty string, ``source_factory`` is
            NOT a valid callable, ``processor_factory`` is NOT a valid callable
            or ``sink_factory`` is NOT a valid callable.
        """
        super().__init__()
        self._id: str = ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=id,
                klass=str,
                message="'id' MUST be a string.",
            ),
            message="'id' MUST NOT be an empty string.",
        )
        self._name: str = ensure_not_none_nor_empty(
            value=ensure_instance_of(
                value=name,
                klass=str,
                message="'name' MUST be a string.",
            ),
            message="'name' MUST NOT be an empty string.",
        )
        self._source_factory: Callable[[], Source[_RDT]] = ensure_callable(
            value=source_factory,
            message="'source_factory' MUST be a callable object.",
        )
        self._description: str | None = ensure_optional_instance_of(
            value=description,
            klass=str,
            message="'description' MUST be a string when NOT None.",
        )
        self._processor_factory: Callable[[], Processor[_RDT, _PDT]]
        self._processor_factory = ensure_callable(
            value=processor_factory,
            message="'processor_factory' MUST be a callable object.",
        )
        self._sink_factory: Callable[[], Sink[_PDT]] = ensure_callable(
            value=sink_factory,
            message="'sink_factory' MUST be a callable object.",
        )

    @property
    @override
    def id(self) -> str:
        return self._id

    @property
    @override
    def name(self) -> str:
        return self._name

    @property
    @override
    def description(self) -> str | None:
        return self._description

    @property
    @override
    def source_factory(self) -> Callable[[], Source[_RDT]]:
        return self._source_factory

    @property
    @override
    def processor_factory(self) -> Callable[[], Processor[_RDT, _PDT]]:
        return self._processor_factory

    @property
    @override
    def sink_factory(self) -> Callable[[], Sink[_PDT]]:
        return self._sink_factory


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "SimpleWorkflowDefinition",
]
