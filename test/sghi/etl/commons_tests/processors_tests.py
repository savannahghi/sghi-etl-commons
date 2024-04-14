# ruff: noqa: D205
"""Tests for the :module:`sghi.etl.commons.processors` module."""

from __future__ import annotations

from unittest import TestCase

import pytest

from sghi.disposable import ResourceDisposedError
from sghi.etl.commons import NOOPProcessor


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
        """Calling :meth:`NOOPProcessor.dispose` should be okay.

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
        with NOOPProcessor() as processor:
            clean_data = processor.apply(raw_data)
            assert clean_data is raw_data

        assert processor.is_disposed

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
