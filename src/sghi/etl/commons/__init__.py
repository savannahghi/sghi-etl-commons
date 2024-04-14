"""Collection of utilities for working with SGHI ETL Workflows."""

from .processors import NOOPProcessor, processor
from .sources import source
from .utils import fail_fast, fail_fast_factory, ignored_failed

__all__ = [
    "NOOPProcessor",
    "fail_fast",
    "fail_fast_factory",
    "ignored_failed",
    "processor",
    "source",
]
