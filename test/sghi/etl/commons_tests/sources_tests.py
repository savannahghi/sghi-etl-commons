# ruff: noqa: D205
"""Tests for the :module:`sghi.etl.commons.sources` module."""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from typing import Any
from unittest import TestCase

import pytest
from typing_extensions import override

from sghi.disposable import ResourceDisposedError, not_disposed
from sghi.etl.commons import GatherSource, source
from sghi.etl.core import Source

# =============================================================================
# HELPERS
# =============================================================================


class _StreamingSource(Source[Iterable[int]]):
    def __init__(self) -> None:
        super().__init__()
        self._yielded: int = 0
        self._is_disposed: bool = False

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> Iterable[int]:
        for _ in range(3):
            # noinspection PyArgumentList
            yield from self._do_yield()
        self._yielded = 0

    @override
    def dispose(self) -> None:
        self._is_disposed = True

    @not_disposed
    def _do_yield(self) -> Iterable[int]:
        yield from range(self._yielded, self._yielded + 4)
        self._yielded += 4


# =============================================================================
# TESTS
# =============================================================================


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
        source("Not a function")  # type: ignore[reportArgumentType]

    assert exc_info.value.args[0] == "A callable object is required."


def test_source_decorator_fails_on_a_none_input_value() -> None:
    """:func:`source` should raise a :exc:`ValueError` when given a ``None``
    value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        source(None)  # type: ignore[reportArgumentType]

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


class TestGatherSource(TestCase):
    """Tests for the :class:`sghi.etl.commons.GatherSource` class."""

    @override
    def setUp(self) -> None:
        super().setUp()

        @source
        def get_greeting() -> str:
            return "Hello, World!"

        @source
        def supply_ints(count: int = 5) -> Iterable[int]:
            yield from range(count)

        @source
        def supply_ints_slowly(
            count: int = 5, delay: float = 0.5
        ) -> Iterable[int]:
            for i in range(count):
                time.sleep(delay)
                yield i

        self._embedded_sources: Sequence[Source[Any]] = [
            get_greeting,
            supply_ints,
            supply_ints_slowly,
            _StreamingSource(),
        ]
        self._instance: Source[Sequence[Any]] = GatherSource(
            sources=self._embedded_sources,
        )

    @override
    def tearDown(self) -> None:
        super().tearDown()
        self._instance.dispose()

    def test_dispose_has_the_intended_side_effects(self) -> None:
        """Calling :meth:`GatherSource.dispose` should result in the
        :attr:`GatherSource.is_disposed` property being set to ``True``.

        Each embedded ``Source`` should also be disposed.
        """
        self._instance.dispose()

        assert self._instance.is_disposed
        for _source in self._embedded_sources:
            assert _source.is_disposed

    def test_draw_returns_the_expected_value(self) -> None:
        """:meth:`GatherSource.draw` should return the aggregated raw data
        after drawing from each embedded source.
        """
        result = self._instance.draw()
        assert isinstance(result, Sequence)
        assert len(result) == len(self._embedded_sources)
        assert result[0] == "Hello, World!"
        assert tuple(result[1]) == (0, 1, 2, 3, 4)
        assert tuple(result[2]) == (0, 1, 2, 3, 4)
        assert tuple(result[3]) == (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

    def test_instantiation_fails_on_an_empty_sources_arg(self) -> None:
        """Instantiating a :class:`GatherSource` with an empty ``sources``
        argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="NOT be empty") as exp_info:
            GatherSource(sources=[])

        assert exp_info.value.args[0] == "'sources' MUST NOT be empty."

    def test_instantiation_fails_on_non_callable_executor_factory_arg(
        self,
    ) -> None:
        """Instantiating a :class:`GatherSource` with a non-callable value for
        the ``executor_factory`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            GatherSource(
                sources=self._embedded_sources,
                executor_factory=None,  # type: ignore[reportArgumentType]
            )

        assert (
            exp_info.value.args[0] == "'executor_factory' MUST be a callable."
        )

    def test_instantiation_fails_on_non_callable_result_gatherer_arg(
        self,
    ) -> None:
        """Instantiating a :class:`GatherSource` with a non-callable value for
        the ``result_gatherer`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            GatherSource(
                sources=self._embedded_sources,
                result_gatherer=None,  # type: ignore[reportArgumentType]
            )

        assert (
            exp_info.value.args[0] == "'result_gatherer' MUST be a callable."
        )

    def test_instantiation_fails_on_non_callable_retry_policy_factory_arg(
        self,
    ) -> None:
        """Instantiating a :class:`GatherSource` with a non-callable value for
        the ``retry_policy_factory`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            GatherSource(
                sources=self._embedded_sources,
                retry_policy_factory=None,  # type: ignore[reportArgumentType]
            )

        assert (
            exp_info.value.args[0]
            == "'retry_policy_factory' MUST be a callable."
        )

    def test_instantiation_fails_on_non_sequence_sources_arg(self) -> None:
        """Instantiating a :class:`GatherSource` with a non ``Sequence``
        ``sources``  argument should raise a :exc:`TypeError`.
        """
        values = (None, 67, self._embedded_sources[0])
        for value in values:
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                GatherSource(sources=value)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'sources' MUST be a collections.abc.Sequence object."
            )

    def test_multiple_dispose_invocations_is_okay(self) -> None:
        """Calling :meth:`GatherSource.dispose` multiple times should be okay.

        No errors should be raised and the object should remain disposed.
        """
        for _ in range(10):
            try:
                self._instance.dispose()
            except Exception as exc:  # noqa: BLE001
                fail_reason: str = (
                    "Calling 'GatherSource.dispose()' multiple times should "
                    f"be okay. But the following error was raised: {exc!s}"
                )
                pytest.fail(fail_reason)

            assert self._instance.is_disposed
            for _source in self._embedded_sources:
                assert _source.is_disposed

    def test_usage_as_a_context_manager_behaves_as_expected(self) -> None:
        """:class:`GatherSource` instances are valid context managers and
        should behave correctly when used as so.
        """
        with self._instance:
            result = self._instance.draw()
            assert isinstance(result, Sequence)
            assert len(result) == len(self._embedded_sources)

        assert self._instance.is_disposed
        for _source in self._embedded_sources:
            assert _source.is_disposed

    def test_usage_when_is_disposed_fails(self) -> None:
        """Invoking "resource-aware" methods of a disposed instance should
        result in an :exc:`ResourceDisposedError` being raised.

        Specifically, invoking the following two methods on a disposed instance
        should fail:

        - :meth:`GatherSource.__enter__`
        - :meth:`GatherSource.draw`
        """
        self._instance.dispose()

        with pytest.raises(ResourceDisposedError):
            self._instance.draw()

        with pytest.raises(ResourceDisposedError):
            self._instance.__enter__()
