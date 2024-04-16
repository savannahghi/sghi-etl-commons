# ruff: noqa: D205
"""Tests for the :module:`sghi.etl.commons.processors` module."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase

import pytest
from typing_extensions import override

from sghi.disposable import ResourceDisposedError
from sghi.etl.commons import NOOPProcessor, ProcessorPipe, processor
from sghi.etl.core import Processor
from sghi.task import task

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


def test_processor_decorator_delegates_to_the_wrapped_callable() -> None:
    """:func:`processor` should delegate to the wrapped callable when
    invoked.
    """
    int_to_str: Processor[int, str] = processor(str)

    def add_100(value: int) -> int:
        return value + 100

    add_100_processor: Processor[int, int] = processor(add_100)

    assert int_to_str(3) == str(3) == "3"
    assert int_to_str(10) == str(10) == "10"
    assert add_100_processor(10) == add_100(10) == 110
    assert add_100_processor(-10) == add_100(-10) == 90


def test_processor_decorator_fails_on_non_callable_input_value() -> None:
    """:func:`processor` should raise a :exc:`ValueError` when given a
    non-callable` value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        processor("Not a function")  # type: ignore

    assert exc_info.value.args[0] == "A callable object is required."


def test_processor_decorator_fails_on_a_none_input_value() -> None:
    """:func:`processor` should raise a :exc:`ValueError` when given a ``None``
    value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        processor(None)  # type: ignore

    assert exc_info.value.args[0] == "A callable object is required."


def test_processor_decorator_returns_expected_value() -> None:
    """:func:`processor` should return a ``Processor`` instance."""

    @processor
    def int_to_str(value: int) -> str:
        return str(value)

    def add_100(value: int) -> int:
        return value + 100

    add_100_processor: Processor[int, int] = processor(add_100)

    assert isinstance(int_to_str, Processor)
    assert isinstance(add_100_processor, Processor)


def test_processor_decorated_value_usage_as_a_context_manager() -> None:
    """:func:`processor` decorated callables are valid context managers and
    should behave correctly when used as so.
    """

    @task
    def add_100(value: int) -> int:
        return value + 100

    @task
    def int_to_str(value: int) -> str:
        return str(value)

    with processor(add_100 >> int_to_str) as _processor:
        result: str = _processor(10)

    assert result == "110"
    assert _processor.is_disposed


def test_processor_decorated_value_usage_when_is_disposed_fails() -> None:
    """Usage of a :func:`processor` decorated callable should raise
    :exc:`ResourceDisposedError` when invoked after being disposed.
    """

    @processor
    def int_to_str(value: int) -> str:
        return str(value)

    int_to_str.dispose()

    with pytest.raises(ResourceDisposedError):
        int_to_str(10)

    with pytest.raises(ResourceDisposedError):
        int_to_str.apply(10)

    with pytest.raises(ResourceDisposedError):
        int_to_str.__enter__()


class TestNOOPProcessor(TestCase):
    """Tests for the :class:`sghi.etl.commons.NOOPProcessor` class."""

    def test_apply_returns_the_expected_value(self) -> None:
        """:meth:`NOOPProcessor.apply` should return its argument without any
        modifications.
        """
        raw_data1: list[str] = ["some", "very", "important", "raw", "data"]
        raw_data2: str = "some very important raw data"
        raw_data3: int = 37
        raw_data4: str | None = None

        instance = NOOPProcessor()

        assert instance.apply(raw_data1) is raw_data1
        assert instance.apply(raw_data2) is raw_data2
        assert instance.apply(raw_data3) == raw_data3
        assert instance.apply(raw_data4) is None

        instance.dispose()

    def test_dispose_has_the_intended_side_effects(self) -> None:
        """Calling :meth:`NOOPProcessor.dispose` should result in the
        :attr:`NOOPProcessor.is_disposed` property being set to ``True``.
        """
        instance = NOOPProcessor()
        instance.dispose()

        assert instance.is_disposed

    def test_multiple_dispose_invocations_is_okay(self) -> None:
        """Calling :meth:`NOOPProcessor.dispose` multiple times should be okay.

        No errors should be raised and the object should remain disposed.
        """
        instance = NOOPProcessor()

        for _ in range(10):
            try:
                instance.dispose()
            except Exception as exc:  # noqa: BLE001
                fail_reason: str = (
                    "Calling 'NOOPProcessor.dispose()' multiple times should "
                    f"be okay. But the following error was raised: {exc!s}"
                )
                pytest.fail(fail_reason)

            assert instance.is_disposed

    def test_usage_as_a_context_manager_behaves_as_expected(self) -> None:
        """:class:`NOOPProcessor` instances are valid context managers and
        should behave correctly when used as so.
        """
        raw_data: list[str] = ["some", "very", "important", "raw", "data"]
        with NOOPProcessor() as _processor:
            clean_data = _processor.apply(raw_data)
            assert clean_data is raw_data

        assert _processor.is_disposed

    def test_usage_when_is_disposed_fails(self) -> None:
        """Invoking "resource-aware" methods of a disposed instance should
        result in an :exc:`ResourceDisposedError` being raised.

        Specifically, invoking the following two methods on a disposed instance
        should fail:

        - :meth:`NOOPProcessor.__enter__`
        - :meth:`NOOPProcessor.apply`
        """
        instance = NOOPProcessor()
        instance.dispose()

        with pytest.raises(ResourceDisposedError):
            instance.apply("some raw data.")

        with pytest.raises(ResourceDisposedError):
            instance.__enter__()


class TestProcessorPipe(TestCase):
    """Tests for the :class:`sghi.etl.commons.ProcessorPipe` class."""

    @override
    def setUp(self) -> None:
        super().setUp()

        @processor
        def add_65(ints: Iterable[int]) -> Iterable[int]:
            yield from (v + 65 for v in ints)

        @processor
        def ints_to_chars(ints: Iterable[int]) -> Iterable[str]:
            yield from map(chr, ints)

        @processor
        def join_chars(values: Iterable[str]) -> str:
            return "".join(list(values))

        self._embedded_processors: Sequence[Processor] = [
            add_65,
            ints_to_chars,
            join_chars,
        ]
        self._instance: Processor[Iterable[int], str] = ProcessorPipe(
            processors=self._embedded_processors,
        )

    @override
    def tearDown(self) -> None:
        super().tearDown()
        self._instance.dispose()

    def test_apply_returns_the_expected_value(self) -> None:
        """:meth:`ProcessorPipe.apply` should return the result after applying
        the given raw data through its embedded processors.
        """
        assert self._instance.apply(range(10)) == "ABCDEFGHIJ"

    def test_instantiation_fails_on_none_processors_argument(self) -> None:
        """Instantiating a :class:`ProcessorPipe` with a ``None``
        ``processors`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="None or empty") as exp_info:
            ProcessorPipe(processors=None)  # type: ignore

        assert (
            exp_info.value.args[0] == "'processors' MUST NOT be None or empty."
        )

    def test_instantiation_fails_on_an_empty_processors_argument(self) -> None:
        """Instantiating a :class:`ProcessorPipe` with an empty
        ``processors`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="None or empty") as exp_info:
            ProcessorPipe(processors=[])

        assert (
            exp_info.value.args[0] == "'processors' MUST NOT be None or empty."
        )

    def test_dispose_has_the_intended_side_effects(self) -> None:
        """Calling :meth:`ProcessorPipe.dispose` should result in the
        :attr:`ProcessorPipe.is_disposed` property being set to ``True``.

        Each embedded ``Processor`` should also be disposed.
        """
        self._instance.dispose()

        assert self._instance.is_disposed
        for _processor in self._embedded_processors:
            assert _processor.is_disposed

    def test_multiple_dispose_invocations_is_okay(self) -> None:
        """Calling :meth:`ProcessorPipe.dispose` multiple times should be okay.

        No errors should be raised and the object should remain disposed.
        """
        for _ in range(10):
            try:
                self._instance.dispose()
            except Exception as exc:  # noqa: BLE001
                fail_reason: str = (
                    "Calling 'ProcessorPipe.dispose()' multiple times should "
                    f"be okay. But the following error was raised: {exc!s}"
                )
                pytest.fail(fail_reason)

            assert self._instance.is_disposed
            for _processor in self._embedded_processors:
                assert _processor.is_disposed

    def test_usage_as_a_context_manager_behaves_as_expected(self) -> None:
        """:class:`ProcessorPipe` instances are valid context managers and
        should behave correctly when used as so.
        """
        with self._instance:
            assert self._instance.apply(range(5, 10)) == "FGHIJ"

        assert self._instance.is_disposed
        for _processor in self._embedded_processors:
            assert _processor.is_disposed

    def test_usage_when_is_disposed_fails(self) -> None:
        """Invoking "resource-aware" methods of a disposed instance should
        result in an :exc:`ResourceDisposedError` being raised.

        Specifically, invoking the following two methods on a disposed instance
        should fail:

        - :meth:`ProcessorPipe.__enter__`
        - :meth:`ProcessorPipe.apply`
        """
        self._instance.dispose()

        with pytest.raises(ResourceDisposedError):
            self._instance.apply(range(5))

        with pytest.raises(ResourceDisposedError):
            self._instance.__enter__()
