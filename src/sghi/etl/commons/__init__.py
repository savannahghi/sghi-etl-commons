"""Collection of utilities for working with SGHI ETL Workflows."""

from .processors import (
    NOOPProcessor,
    ProcessorPipe,
    pipe_processors,
    processor,
)
from .sinks import NullSink, sink
from .sources import source
from .utils import fail_fast, fail_fast_factory, ignored_failed

__all__ = [
    "NOOPProcessor",
    "NullSink",
    "ProcessorPipe",
    "fail_fast",
    "fail_fast_factory",
    "ignored_failed",
    "pipe_processors",
    "processor",
    "sink",
    "source",
]
