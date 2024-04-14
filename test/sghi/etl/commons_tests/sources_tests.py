# ruff: noqa: D205
"""Tests for the :module:`sghi.etl.commons.sources` module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sghi.disposable import ResourceDisposedError
from sghi.etl.commons import source
from sghi.etl.core import Source

if TYPE_CHECKING:
    from collections.abc import Iterable


def test_source_decorator_delegates_to_the_wrapped_callable() -> None:
    """:func:`source` should delegate to the wrapped callable when invoked."""

    def supply_ints(count: int = 4) -> Iterable[int]:
        yield from range(count)

    int_supplier_source: Source[Iterable[int]] = source(supply_ints)

    assert list(int_supplier_source()) == list(supply_ints()) == [0, 1, 2, 3]


def test_source_decorator_fails_on_non_callable_input_value() -> None:
    """:func:`source` should raise a :exc:`ValueError` when given a
    non-callable` value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        source("Not a function")  # type: ignore

    assert exc_info.value.args[0] == "A callable object is required."


def test_source_decorator_fails_on_a_none_input_value() -> None:
    """:func:`source` should raise a :exc:`ValueError` when given a ``None``
    value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        source(None)  # type: ignore

    assert exc_info.value.args[0] == "A callable object is required."


def test_source_decorator_returns_expected_value() -> None:
    """:func:`source` should return a ``Source`` instance."""

    @source
    def supply_ints(count: int = 5) -> Iterable[int]:
        yield from range(count)

    empty_string_supplier: Source[str] = source(str)

    assert isinstance(supply_ints, Source)
    assert isinstance(empty_string_supplier, Source)


def test_source_decorated_value_usage_as_a_context_manager() -> None:
    """:func:`source` decorated callables are valid context managers and
    should behave correctly when used as so.
    """

    def supply_ints(count: int = 5) -> Iterable[int]:
        yield from range(count)

    with source(supply_ints) as int_supplier:
        result: tuple[int, ...] = tuple(int_supplier())

    assert result == (0, 1, 2, 3, 4)
    assert int_supplier.is_disposed


def test_source_decorated_value_usage_when_is_disposed_fails() -> None:
    """Usage of a :func:`source` decorated callable should raise
    :exc:`ResourceDisposedError` when invoked after being disposed.
    """

    @source
    def supply_ints(count: int = 5) -> Iterable[int]:
        yield from range(count)

    supply_ints.dispose()

    with pytest.raises(ResourceDisposedError):
        supply_ints()

    with pytest.raises(ResourceDisposedError):
        supply_ints.draw()

    with pytest.raises(ResourceDisposedError):
        supply_ints.__enter__()
