# ruff: noqa: D205
"""Tests for the :module:`sghi.etl.commons.sinks` module."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase

import pytest

from sghi.disposable import ResourceDisposedError
from sghi.etl.commons import NullSink, sink
from sghi.etl.core import Sink

if TYPE_CHECKING:
    from collections.abc import Iterable, MutableSequence


def test_sink_decorator_delegates_to_the_wrapped_callable() -> None:
    """:func:`sink` should delegate to the wrapped callable when invoked."""
    repository: MutableSequence[int] = []

    def save_ints(values: Iterable[int]) -> None:
        repository.extend(values)

    ints_consumer: Sink[Iterable[int]] = sink(save_ints)
    ints_consumer(range(5))

    assert repository == [0, 1, 2, 3, 4]


def test_sink_decorator_fails_on_non_callable_input_value() -> None:
    """:func:`sink` should raise a :exc:`ValueError` when given a
    non-callable` value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        sink("Not a function")  # type: ignore

    assert exc_info.value.args[0] == "A callable object is required."


def test_sink_decorator_fails_on_a_none_input_value() -> None:
    """:func:`sink` should raise a :exc:`ValueError` when given a ``None``
    value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        sink(None)  # type: ignore

    assert exc_info.value.args[0] == "A callable object is required."


def test_sink_decorator_returns_expected_value() -> None:
    """:func:`sink` should return a ``Sink`` instance."""
    repository: MutableSequence[int] = []

    @sink
    def save_ints(values: Iterable[int]) -> None:
        repository.extend(values)

    print_all: Sink[str] = sink(print)

    assert isinstance(save_ints, Sink)
    assert isinstance(print_all, Sink)


def test_sink_decorated_value_usage_as_a_context_manager() -> None:
    """:func:`sink` decorated callables are valid context managers and
    should behave correctly when used as so.
    """
    repository: MutableSequence[int] = []

    def save_ints(values: Iterable[int]) -> None:
        repository.extend(values)

    with sink(save_ints) as ints_consumer:
        ints_consumer(range(5))

    assert repository == [0, 1, 2, 3, 4]
    assert ints_consumer.is_disposed


def test_sink_decorated_value_usage_when_is_disposed_fails() -> None:
    """Usage of a :func:`sink` decorated callable should raise
    :exc:`ResourceDisposedError` when invoked after being disposed.
    """
    repository: MutableSequence[int] = []

    @sink
    def save_ints(values: Iterable[int]) -> None:
        repository.extend(values)

    save_ints.dispose()

    with pytest.raises(ResourceDisposedError):
        save_ints(range(5))

    with pytest.raises(ResourceDisposedError):
        save_ints.drain(range(5))

    with pytest.raises(ResourceDisposedError):
        save_ints.__enter__()


class TestNullSink(TestCase):
    """Tests for the :class:`sghi.etl.commons.NullSInk` class."""

    def test_dispose_has_the_intended_side_effects(self) -> None:
        """Calling :meth:`NullSink.dispose` should result in the
        :attr:`NullSink.is_disposed` property being set to ``True``.
        """
        instance = NullSink()
        instance.dispose()

        assert instance.is_disposed

    def test_multiple_dispose_invocations_is_okay(self) -> None:
        """Calling :meth:`NullSink.dispose` multiple times should be okay.

        No errors should be raised and the object should remain disposed.
        """
        instance = NullSink()

        for _ in range(10):
            try:
                instance.dispose()
            except Exception as exc:  # noqa: BLE001
                fail_reason: str = (
                    "Calling 'NullSink.dispose()' multiple times should be "
                    f"okay. But the following error was raised: {exc!s}"
                )
                pytest.fail(fail_reason)

            assert instance.is_disposed

    def test_usage_as_a_context_manager_behaves_as_expected(self) -> None:
        """:class:`NullSink` instances are valid context managers and should
        behave correctly when used as so.
        """
        processed_data: list[str] = [
            "some",
            "very",
            "important",
            "processed",
            "data",
        ]
        with NullSink() as _sink:
            _sink.drain(processed_data)

        assert _sink.is_disposed

    def test_usage_when_is_disposed_fails(self) -> None:
        """Invoking "resource-aware" methods of a disposed instance should
        result in an :exc:`ResourceDisposedError` being raised.

        Specifically, invoking the following two methods on a disposed instance
        should fail:

        - :meth:`NullSink.__enter__`
        - :meth:`NullSink.apply`
        """
        instance = NullSink()
        instance.dispose()

        with pytest.raises(ResourceDisposedError):
            instance.drain("some processed data.")

        with pytest.raises(ResourceDisposedError):
            instance.__enter__()
