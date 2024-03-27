"""Collection of utilities for working with SGHI ETL Worflows."""

from .processors import (
    NoOpProcessor,
    ProcessorPipe,
    ProcessorSet,
    ScatterProcessor,
    processor,
)
from .sinks import NullSink, ScatterSink, SinkSet, sink
from .sources import SourceSet, source
from .workflow_builder import WorkflowBuilder

__all__ = [
    "NoOpProcessor",
    "NullSink",
    "ProcessorPipe",
    "ProcessorSet",
    "ScatterProcessor",
    "ScatterSink",
    "SinkSet",
    "SourceSet",
    "WorkflowBuilder",
    "processor",
    "sink",
    "source",
]
