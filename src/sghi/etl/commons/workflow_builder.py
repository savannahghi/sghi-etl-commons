from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from attrs import define

if TYPE_CHECKING:
    from sghi.etl.core import Processor, Sink, Source, WorkflowDefinition

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""


# =============================================================================
# WORKFLOW BUILDER
# =============================================================================


@define
class WorkflowBuilder(Generic[_RDT, _PDT]):

    def __call__(self) -> WorkflowDefinition[_RDT, _PDT]:
        return self.build()

    def build(self) -> WorkflowDefinition[_RDT, _PDT]: ...

    def draw_from(self, source: Source[_RDT]) -> Source[_RDT]: ...

    def drain_to(self, sink: Sink[_PDT]) -> Sink[_PDT]: ...

    def process_using(
        self,
        processor: Processor[_RDT, _PDT],
    ) -> Processor[_RDT, _PDT]: ...
