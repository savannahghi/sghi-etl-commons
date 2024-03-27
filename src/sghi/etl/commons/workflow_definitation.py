"""Common :class:`sghi.etl.core.WorkflowDefinition` implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar, override

from attrs import field, frozen, validators

from sghi.etl.core import Processor, Sink, Source, WorkflowDefinition

from .processors import NoOpProcessor
from .sinks import NullSink

if TYPE_CHECKING:
    from collections.abc import Callable

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@frozen
class SimpleWorkflowDefinitions(
    WorkflowDefinition[_RDT, _PDT],
    Generic[_RDT, _PDT],
):
    """:class:A simple `WorkflowDefinition` implementation."""

    _id: str = field(
        alias="id",
        validator=[validators.instance_of(str), validators.min_len(2)],
    )
    _name: str = field(alias="name", validator=validators.instance_of(str))
    _source_factory: Callable[[], Source[_RDT]] = field(
        alias="source_factory",
        repr=False,
        validator=validators.is_callable(),
    )
    _description: str | None = field(
        alias="description",
        default=None,
        kw_only=True,
        validator=validators.optional(validator=validators.instance_of(str)),
    )
    _processor_factory: Callable[[], Processor[_RDT, _PDT]] = field(
        alias="processor_factory",
        default=NoOpProcessor,
        kw_only=True,
        repr=False,
        validator=validators.is_callable(),
    )
    _sink_factory: Callable[[], Sink[_PDT]] = field(
        alias="sink_factory",
        default=NullSink,
        kw_only=True,
        repr=False,
        validator=validators.is_callable(),
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
