# ruff: noqa: D205, PLR2004
"""Tests for the :module:`sghi.etl.commons.sinks` module."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase

import pytest
from typing_extensions import override

from sghi.disposable import ResourceDisposedError
from sghi.etl.commons import NullSink, ScatterSink, SplitSink, sink
from sghi.etl.core import Sink

if TYPE_CHECKING:
    from collections.abc import Iterable, MutableSequence, MutableSet, Sequence


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
        sink("Not a function")  # type: ignore[reportArgumentType]

    assert exc_info.value.args[0] == "A callable object is required."


def test_sink_decorator_fails_on_a_none_input_value() -> None:
    """:func:`sink` should raise a :exc:`ValueError` when given a ``None``
    value.
    """
    with pytest.raises(ValueError, match="callable object") as exc_info:
        sink(None)  # type: ignore[reportArgumentType]

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
            # noinspection PyArgumentList
            instance.__enter__()


class TestScatterSink(TestCase):
    """Tests for the :class:`sghi.etl.commons.ScatterSink` class."""

    @override
    def setUp(self) -> None:
        super().setUp()
        self._repository1: MutableSequence[int] = []
        self._repository2: MutableSet[int] = set()

        @sink
        def save_ordered(values: Iterable[int]) -> None:
            self._repository1.extend(values)

        @sink
        def save_randomly(values: Iterable[int]) -> None:
            for value in values:
                self._repository2.add(value)

        self._embedded_sinks: Sequence[Sink[Iterable[int]]] = [
            save_ordered,
            save_randomly,
        ]
        self._instance: ScatterSink[Iterable[int]]
        self._instance = ScatterSink(self._embedded_sinks)

    @override
    def tearDown(self) -> None:
        super().tearDown()
        self._instance.dispose()

    def test_dispose_has_the_intended_side_effects(self) -> None:
        """Calling :meth:`ScatterSink.dispose` should result in the
        :attr:`ScatterSink.is_disposed` property being set to ``True``.

        Each embedded ``Sink`` should also be disposed.
        """
        self._instance.dispose()

        assert self._instance.is_disposed
        for _sink in self._embedded_sinks:
            assert _sink.is_disposed

    def test_drain_side_effects(self) -> None:
        """:meth:`ScatterSink.drain` should drain the supplied processed data
        to all embedded sinks.
        """
        self._instance.drain(list(range(5)))

        assert len(self._repository1) == 5
        assert len(self._repository2) == 5
        assert self._repository1[0] == 0
        assert self._repository1[1] == 1
        assert self._repository1[2] == 2
        assert self._repository1[3] == 3
        assert 0 in self._repository2
        assert 1 in self._repository2
        assert 2 in self._repository2
        assert 3 in self._repository2

    def test_instantiation_fails_on_an_empty_processors_arg(self) -> None:
        """Instantiating a :class:`ScatterSink` with an empty ``sinks``
        argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="NOT be empty") as exp_info:
            ScatterSink(sinks=[])

        assert exp_info.value.args[0] == "'sinks' MUST NOT be empty."

    def test_instantiation_fails_on_non_callable_executor_factory_arg(
        self,
    ) -> None:
        """Instantiating a :class:`ScatterSink` with a non-callable value for
        the ``executor_factory`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            ScatterSink(sinks=self._embedded_sinks, executor_factory=None)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0] == "'executor_factory' MUST be a callable."
        )

    def test_instantiation_fails_on_non_callable_result_gatherer_arg(
        self,
    ) -> None:
        """Instantiating a :class:`ScatterSink` with a non-callable value for
        the ``result_gatherer`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            ScatterSink(sinks=self._embedded_sinks, result_gatherer=None)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0] == "'result_gatherer' MUST be a callable."
        )

    def test_instantiation_fails_on_non_callable_retry_policy_factory_arg(
        self,
    ) -> None:
        """Instantiating a :class:`ScatterSink` with a non-callable value for
        the ``retry_policy_factory`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            ScatterSink(sinks=self._embedded_sinks, retry_policy_factory=None)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0]
            == "'retry_policy_factory' MUST be a callable."
        )

    def test_instantiation_fails_on_non_sequence_sinks_arg(self) -> None:
        """Instantiating a :class:`ScatterSink` with a non ``Sequence``
        ``sinks`` argument should raise a :exc:`TypeError`.
        """
        values = (None, 67, self._embedded_sinks[0])
        for value in values:
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                ScatterSink(sinks=value)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'sinks' MUST be a collections.abc.Sequence object."
            )

    def test_multiple_dispose_invocations_is_okay(self) -> None:
        """Calling :meth:`ScatterSink.dispose` multiple times should be okay.

        No errors should be raised and the object should remain disposed.
        """
        for _ in range(10):
            try:
                self._instance.dispose()
            except Exception as exc:  # noqa: BLE001
                fail_reason: str = (
                    "Calling 'ScatterSink.dispose()' multiple times should be "
                    f"okay. But the following error was raised: {exc!s}"
                )
                pytest.fail(fail_reason)

            assert self._instance.is_disposed
            for _sinks in self._embedded_sinks:
                assert _sinks.is_disposed

    def test_usage_as_a_context_manager_behaves_as_expected(self) -> None:
        """:class:`ScatterSink` instances are valid context managers and should
        behave correctly when used as so.
        """
        with self._instance:
            self._instance.drain([-100, 60, 0])
            assert len(self._repository1) == 3
            assert len(self._repository2) == 3
            assert self._repository1[0] == -100
            assert self._repository1[1] == 60
            assert self._repository1[2] == 0
            assert -100 in self._repository2
            assert 60 in self._repository2
            assert 0 in self._repository2

        assert self._instance.is_disposed
        for _sink in self._embedded_sinks:
            assert _sink.is_disposed

    def test_usage_when_is_disposed_fails(self) -> None:
        """Invoking "resource-aware" methods of a disposed instance should
        result in an :exc:`ResourceDisposedError` being raised.

        Specifically, invoking the following two methods on a disposed instance
        should fail:

        - :meth:`ScatterSink.__enter__`
        - :meth:`ScatterSink.drain`
        """
        self._instance.dispose()

        with pytest.raises(ResourceDisposedError):
            self._instance.drain([10, 10, 0])

        with pytest.raises(ResourceDisposedError):
            # noinspection PyArgumentList
            self._instance.__enter__()


class TestSplitSink(TestCase):
    """Tests for the :class:`sghi.etl.commons.SplitSink` class."""

    @override
    def setUp(self) -> None:
        super().setUp()
        self._repository1: MutableSequence[int] = []
        self._repository2: MutableSet[int] = set()

        @sink
        def save_ordered(value: int) -> None:
            self._repository1.append(value)

        @sink
        def save_randomly(value: int) -> None:
            self._repository2.add(value)

        self._embedded_sinks: Sequence[Sink[int]] = [
            save_ordered,
            save_randomly,
        ]
        self._instance: SplitSink[int] = SplitSink(self._embedded_sinks)

    @override
    def tearDown(self) -> None:
        super().tearDown()
        self._instance.dispose()

    def test_dispose_has_the_intended_side_effects(self) -> None:
        """Calling :meth:`SplitSink.dispose` should result in the
        :attr:`SplitSink.is_disposed` property being set to ``True``.

        Each embedded ``Sink`` should also be disposed.
        """
        self._instance.dispose()

        assert self._instance.is_disposed
        for _sink in self._embedded_sinks:
            assert _sink.is_disposed

    def test_drain_fails_on_non_sequence_input_value(self) -> None:
        """:meth:`SplitSink.drain` should raise a :exc:`TypeError` when invoked
        with a non ``Sequence`` argument.
        """
        values = (None, 67, self._embedded_sinks[0])
        for value in values:
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                self._instance.drain(value)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'processed_data' MUST be a collections.abc.Sequence "
                "object."
            )

    def test_drain_fails_when_input_size_not_equal_no_of_embedded_sinks(
        self,
    ) -> None:
        """:meth:`SplitSink.drain` should raise a :exc:`ValueError` when
        invoked with an argument whose size does not equal the number of
        embedded sinks.
        """
        values = ([], list(range(5)))
        for value in values:
            with pytest.raises(ValueError, match="but got size") as exp_info:
                self._instance.drain(value)

            assert exp_info.value.args[0] == (
                "Expected 'processed_data' to be of size "
                f"{len(self._embedded_sinks)} "
                f"but got size {len(value)} instead."
            )

    def test_drain_side_effects(self) -> None:
        """:meth:`SplitSink.drain` should split the supplied processed data
        into constituent data parts; then drain each data part to each embedded
        sink.
        """
        self._instance.drain([10, 50])

        assert len(self._repository1) == 1
        assert len(self._repository2) == 1
        assert self._repository1[0] == 10
        assert 50 in self._repository2

    def test_instantiation_fails_on_an_empty_processors_arg(self) -> None:
        """Instantiating a :class:`SplitSink` with an empty ``sinks`` argument
        should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="NOT be empty") as exp_info:
            SplitSink(sinks=[])

        assert exp_info.value.args[0] == "'sinks' MUST NOT be empty."

    def test_instantiation_fails_on_non_callable_executor_factory_arg(
        self,
    ) -> None:
        """Instantiating a :class:`SplitSink` with a non-callable value for the
        ``executor_factory`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            SplitSink(sinks=self._embedded_sinks, executor_factory=None)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0] == "'executor_factory' MUST be a callable."
        )

    def test_instantiation_fails_on_non_callable_result_gatherer_arg(
        self,
    ) -> None:
        """Instantiating a :class:`SplitSink` with a non-callable value for the
        ``result_gatherer`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            SplitSink(sinks=self._embedded_sinks, result_gatherer=None)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0] == "'result_gatherer' MUST be a callable."
        )

    def test_instantiation_fails_on_non_callable_retry_policy_factory_arg(
        self,
    ) -> None:
        """Instantiating a :class:`SplitSink` with a non-callable value for the
        ``retry_policy_factory`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
            SplitSink(sinks=self._embedded_sinks, retry_policy_factory=None)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0]
            == "'retry_policy_factory' MUST be a callable."
        )

    def test_instantiation_fails_on_non_sequence_sinks_arg(self) -> None:
        """Instantiating a :class:`SplitSink` with a non ``Sequence`` ``sinks``
        argument should raise a :exc:`TypeError`.
        """
        values = (None, 67, self._embedded_sinks[0])
        for value in values:
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                SplitSink(sinks=value)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'sinks' MUST be a collections.abc.Sequence object."
            )

    def test_multiple_dispose_invocations_is_okay(self) -> None:
        """Calling :meth:`SplitSink.dispose` multiple times should be okay.

        No errors should be raised and the object should remain disposed.
        """
        for _ in range(10):
            try:
                self._instance.dispose()
            except Exception as exc:  # noqa: BLE001
                fail_reason: str = (
                    "Calling 'SplitSink.dispose()' multiple times should be "
                    f"okay. But the following error was raised: {exc!s}"
                )
                pytest.fail(fail_reason)

            assert self._instance.is_disposed
            for _sinks in self._embedded_sinks:
                assert _sinks.is_disposed

    def test_usage_as_a_context_manager_behaves_as_expected(self) -> None:
        """:class:`SplitSink` instances are valid context managers and should
        behave correctly when used as so.
        """
        with self._instance:
            self._instance.drain([-100, 0])
            assert len(self._repository1) == 1
            assert len(self._repository2) == 1
            assert self._repository1[0] == -100
            assert 0 in self._repository2

        assert self._instance.is_disposed
        for _sink in self._embedded_sinks:
            assert _sink.is_disposed

    def test_usage_when_is_disposed_fails(self) -> None:
        """Invoking "resource-aware" methods of a disposed instance should
        result in an :exc:`ResourceDisposedError` being raised.

        Specifically, invoking the following two methods on a disposed instance
        should fail:

        - :meth:`SplitSink.__enter__`
        - :meth:`SplitSink.drain`
        """
        self._instance.dispose()

        with pytest.raises(ResourceDisposedError):
            self._instance.drain([10, 10])

        with pytest.raises(ResourceDisposedError):
            # noinspection PyArgumentList
            self._instance.__enter__()
