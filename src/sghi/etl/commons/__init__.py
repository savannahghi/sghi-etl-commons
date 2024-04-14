"""Collection of utilities for working with SGHI ETL Workflows."""

from .processors import NOOPProcessor
from .utils import fail_fast, fail_fast_factory, ignored_failed

__all__ = [
    "NOOPProcessor",
    "fail_fast",
    "fail_fast_factory",
    "ignored_failed",
]
