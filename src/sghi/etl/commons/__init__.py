"""Collection of utilities for working with SGHI ETL Workflows."""

from .processors import (
    NOOPProcessor,
    ProcessorPipe,
    ScatterGatherProcessor,
    SplitGatherProcessor,
    pipe_processors,
    processor,
)
from .sinks import NullSink, sink
from .sources import GatherSource, source
from .utils import fail_fast, fail_fast_factory, ignored_failed
from .workflow_definitions import SimpleWorkflowDefinition

__all__ = [
    "GatherSource",
    "NOOPProcessor",
    "NullSink",
    "ProcessorPipe",
    "SimpleWorkflowDefinition",
    "ScatterGatherProcessor",
    "SplitGatherProcessor",
    "fail_fast",
    "fail_fast_factory",
    "ignored_failed",
    "pipe_processors",
    "processor",
    "sink",
    "source",
]
