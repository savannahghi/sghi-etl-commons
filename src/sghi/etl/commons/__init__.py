"""Collection of utilities for working with SGHI ETL Workflows."""

from .processors import (
    NOOPProcessor,
    ProcessorPipe,
    ScatterGatherProcessor,
    SplitGatherProcessor,
    pipe_processors,
    processor,
)
from .sinks import NullSink, ScatterSink, SplitSink, sink
from .sources import GatherSource, source
from .utils import fail_fast, fail_fast_factory, ignored_failed, run_workflow
from .workflow_builder import (
    NoSourceProvidedError,
    SoleValueAlreadyRetrievedError,
    WorkflowBuilder,
)
from .workflow_definitions import SimpleWorkflowDefinition

__all__ = [
    "GatherSource",
    "NOOPProcessor",
    "NoSourceProvidedError",
    "NullSink",
    "ProcessorPipe",
    "ScatterGatherProcessor",
    "ScatterSink",
    "SimpleWorkflowDefinition",
    "SoleValueAlreadyRetrievedError",
    "SplitGatherProcessor",
    "SplitSink",
    "WorkflowBuilder",
    "fail_fast",
    "fail_fast_factory",
    "ignored_failed",
    "pipe_processors",
    "processor",
    "run_workflow",
    "sink",
    "source",
]
