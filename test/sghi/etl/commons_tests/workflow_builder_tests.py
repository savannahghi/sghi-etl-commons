# ruff: noqa: D205, PLR2004
"""Tests for the :module:`sghi.etl.commons.workflow_builder` module."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Self
from unittest import TestCase

import pytest
from typing_extensions import override

from sghi.etl.commons import (
    GatherSource,
    NOOPProcessor,
    NoSourceProvidedError,
    NullSink,
    ProcessorPipe,
    ScatterGatherProcessor,
    ScatterSink,
    SoleValueAlreadyRetrievedError,
    SplitGatherProcessor,
    SplitSink,
    WorkflowBuilder,
    processor,
    sink,
    source,
)
from sghi.etl.core import Source, WorkflowDefinition
from sghi.task import not_disposed

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sghi.etl.core import Processor, Sink

# =============================================================================
# TESTS HELPERS
# =============================================================================


def _noop() -> None:
    """Do nothing."""


class Zero(Source[Iterator[int]]):
    """A :class:`Source` that provides as many zeros as drawn.

    This is similar to https://en.wikipedia.org/wiki//dev/zero.
    """

    __slots__ = ("_is_disposed",)

    def __init__(self) -> None:
        """Create a new instance of ``Zero`` source."""
        super().__init__()
        self._is_disposed: bool = False

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Source, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @override
    def dispose(self) -> None:
        self._is_disposed = True

    @override
    def draw(self) -> Iterator[int]:
        while True:
            yield 0


# =============================================================================
# TESTS
# =============================================================================


class TestWorkflowBuilder(TestCase):
    """Tests for the :class:`sghi.etl.commons.WorkflowBuilder` class."""

    @override
    def setUp(self) -> None:
        super().setUp()
        self._instance1 = WorkflowBuilder(
            id="test_workflow_1",
            name="Test Workflow One",
        )
        self._instance2 = WorkflowBuilder(
            id="test_workflow_2",
            name="Test Workflow Two",
            description="A sample ETL workflow.",
            composite_processor_factory=ProcessorPipe,
            composite_sink_factory=SplitSink,
            processor_factories=[NOOPProcessor],
            sink_factories=[NullSink],
            source_factories=[Zero],
            epilogue=_noop,
            prologue=_noop,
        )

    def test_applies_processor_fails_when_given_invalid_input(self) -> None:
        """The decorator :meth:`WorkflowBuilder.applies_processor` should raise
        a :exc:`TypeError` when the given value IS NOT a :class:`Processor`
        instance.
        """
        for non_processor in (None, 1, 5.2, str, NullSink()):
            with pytest.raises(TypeError, match="Processor") as exp_info:
                self._instance1.applies_processor(non_processor)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'processor' MUST be an 'sghi.etl.core.Processor' instance."
            )

    def test_applies_processor_return_value(self) -> None:
        """The decorator meth:`WorkflowBuilder.applies_processor` should return
        the :class:`Processor` instance given to it.
        """
        noop = NOOPProcessor()

        @processor
        def double(values: Iterator[int]) -> Iterator[int]:
            for value in values:
                yield value * 2

        assert self._instance1.applies_processor(noop) is noop
        assert self._instance2.applies_processor(double) is double

    def test_applies_processor_side_effects(self) -> None:
        """The decorator :meth:`WorkflowBuilder.applies_processor` should
        convert the given :class:`Processor` instance into a factory function
        that returns the ``Processor`` once and then append the factory
        function to the list of available processor factories.
        """
        noop = NOOPProcessor()

        self._instance1.applies_processor(noop)
        assert len(self._instance1.processor_factories) == 1
        assert callable(self._instance1.processor_factories[0])

        noop_factory = self._instance1.processor_factories[0]
        # The 1st call should yield the original value
        assert noop_factory() is noop
        # Later calls should result in `SoleValueAlreadyRetrievedError` being
        # raised.
        for _ in range(10):
            with pytest.raises(SoleValueAlreadyRetrievedError) as exp_info:
                noop_factory()

            assert (
                exp_info.value.message
                == "This factory's sole value has already been retrieved."
            )

    def test_apply_processor_fails_when_given_invalid_input(self) -> None:
        """The method :meth:`WorkflowBuilder.apply_processor` should raise
        a :exc:`ValueError` when the given value IS NOT a :class:`Processor`
        instance or a callable object.
        """
        for non_processor in (None, 1, 5.2, {}, (), []):
            with pytest.raises(ValueError, match="Processor") as exp_info:
                self._instance1.apply_processor(non_processor)  # type: ignore[reportArgumentType]

            assert exp_info.value.args[0] == (
                "'processor' MUST be an 'sghi.etl.core.Processor' "
                "instance or a factory function that returns an instance "
                "of the same type."
            )

    def test_apply_processor_return_value(self) -> None:
        """The method meth:`WorkflowBuilder.apply_processor` should return
        the current ``WorkflowBuilder`` instance, i.e. ``self``, on exit.
        """

        @processor
        def double(values: Iterator[int]) -> Iterator[int]:
            for value in values:
                yield value * 2

        np = NOOPProcessor
        assert self._instance1.apply_processor(np) is self._instance1
        assert self._instance2.apply_processor(double) is self._instance2

    def test_apply_processor_side_effects(self) -> None:
        """The method :meth:`WorkflowBuilder.apply_processor` should
        convert the given :class:`Processor` instance into a factory function
        that returns the ``Processor`` once and then append the factory
        function to the list of available processor factories.

        If the given value is a callable object, then append it to the list of
        available processor factories as is.
        """
        noop = NOOPProcessor()

        self._instance1.apply_processor(noop)
        self._instance1.apply_processor(NOOPProcessor)
        assert len(self._instance1.processor_factories) == 2
        assert callable(self._instance1.processor_factories[0])
        assert callable(self._instance1.processor_factories[1])
        assert self._instance1.processor_factories[1] is NOOPProcessor

        noop_factory = self._instance1.processor_factories[0]
        # The 1st call should yield the original value
        assert noop_factory() is noop
        # Later calls should result in `SoleValueAlreadyRetrievedError` being
        # raised.
        for _ in range(10):
            with pytest.raises(SoleValueAlreadyRetrievedError) as exp_info:
                noop_factory()

            assert (
                exp_info.value.message
                == "This factory's sole value has already been retrieved."
            )

    def test_build_fails_when_no_source_is_provided(self) -> None:
        """:meth:`WorkflowBuilder.build` should raise a
        :exc:`NoSourceProvidedError` when no ``Source`` or ``Source`` factory
        has been provided.
        """
        assert len(self._instance1.source_factories) == 0

        with pytest.raises(NoSourceProvidedError) as exp_info:
            self._instance1.build()

        assert exp_info.value.message == (
            "No sources available. At least once 'Source' or one "
            "factory function that supplies 'Source' instances MUST "
            "be provided."
        )

    def test_build_with_defaults(self) -> None:
        """:meth:`WorkflowBuilder.build` should use
        :attr:`WorkflowBuilder.default_processor_factory` and
        :attr:`WorkflowBuilder.default_sink_factory` factories to create a
        :class:`Processor` and :class:`Sink` respectively, when they aren't
        provided.
        """
        assert len(self._instance1.processor_factories) == 0
        assert len(self._instance1.sink_factories) == 0
        assert len(self._instance1.source_factories) == 0

        workflow_def: WorkflowDefinition[Any, Any]
        workflow_def = self._instance1.draw_from(Zero).build()

        assert isinstance(workflow_def, WorkflowDefinition)
        assert isinstance(workflow_def.processor_factory(), NOOPProcessor)
        assert isinstance(workflow_def.sink_factory(), NullSink)
        assert isinstance(workflow_def.source_factory(), Zero)

    def test_build_with_multiple_components(self) -> None:
        r""":meth:`WorkflowBuilder.build` should use the factories returned
        by the :attr:`WorkflowBuilder.composite_processor_factory`,
        :attr:`WorkflowBuilder.composite_sink_factory` and
        :attr:`WorkflowBuilder.composite_source_factory` to combine multiple
        ``Processor``\ s, ``Sink``\ s and ``Source``\ s respectively when they
        are given.
        """
        wb: WorkflowBuilder[Any, Any] = self._instance1
        assert len(wb.processor_factories) == 0
        assert len(wb.sink_factories) == 0
        assert len(wb.source_factories) == 0

        @wb.applies_processor
        @processor
        def add_10(values: Sequence[Iterator[int]]) -> Iterator[int]:  # pyright: ignore[reportUnusedFunction]
            for value in values[0]:
                yield value + 10

        @wb.applies_processor
        @processor
        def mul_10(values: Sequence[Iterator[int]]) -> Iterator[int]:  # pyright: ignore[reportUnusedFunction]
            for value in values[1]:
                yield value * 10

        db1: list[int] = []
        db2: dict[str, int] = {}

        @wb.drains_to
        @sink
        def save_to_db1(values: Sequence[Iterator[int]]) -> None:  # pyright: ignore[reportUnusedFunction]
            db1.extend(values[0])

        @wb.drains_to
        @sink
        def save_to_db2(values: Sequence[Iterator[int]]) -> None:  # pyright: ignore[reportUnusedFunction]
            for value in values[1]:
                db2[str(value)] = value

        wb.composite_processor_factory = SplitGatherProcessor
        wb.composite_source_factory = GatherSource
        wb.composite_sink_factory = SplitSink

        workflow_def: WorkflowDefinition[Any, Any]
        workflow_def = wb.draw_from(Zero).draw_from(Zero).build()

        assert isinstance(workflow_def, WorkflowDefinition)
        assert isinstance(
            workflow_def.processor_factory(),
            SplitGatherProcessor,
        )
        assert isinstance(workflow_def.sink_factory(), SplitSink)
        assert isinstance(workflow_def.source_factory(), GatherSource)

    def test_build_with_single_components(self) -> None:
        """:meth:`WorkflowBuilder.build` should return a ``WorkflowDefinition``
        whose ``processor_factory``, ``sink_factory`` or ``source_factory``
        are the same as those given to the builder if only one of the factory
        for each of the component is given.
        """
        wb: WorkflowBuilder[Any, Any] = self._instance1
        assert len(wb.processor_factories) == 0
        assert len(wb.sink_factories) == 0
        assert len(wb.source_factories) == 0

        @wb.applies_processor
        @processor
        def add_10(values: Iterator[int]) -> Iterator[int]:
            for value in values:
                yield value + 10

        @wb.drains_to
        @sink
        def discard(values: Iterator[int]) -> None:
            for _ in values:
                ...

        workflow_def: WorkflowDefinition[Any, Any]
        workflow_def = wb.draw_from(Zero).build()

        assert isinstance(workflow_def, WorkflowDefinition)
        assert workflow_def.processor_factory() is add_10
        assert workflow_def.sink_factory() is discard
        assert isinstance(workflow_def.source_factory(), Zero)

    def test_clear_processor_factories_side_effects(self) -> None:
        r""":meth:`WorkflowBuilder.clear_processor_factories` should remove all
        registered :class:`Processor`\ s from the builder.
        """
        assert len(self._instance1.processor_factories) == 0
        assert len(self._instance2.processor_factories) > 0

        self._instance1.clear_processor_factories()
        self._instance2.clear_processor_factories()

        assert len(self._instance1.processor_factories) == 0
        assert len(self._instance2.processor_factories) == 0

    def test_clear_processor_factories_return_value(self) -> None:
        """:meth:`WorkflowBuilder.clear_processor_factories` should return the
        current ``WorkflowBuilder`` instance, i.e. ``self``.
        """
        assert self._instance1.clear_processor_factories() is self._instance1
        assert self._instance2.clear_processor_factories() is self._instance2

    def test_clear_sink_factories_side_effects(self) -> None:
        r""":meth:`WorkflowBuilder.clear_sink_factories` should remove all
        registered :class:`Sink`\ s from the builder.
        """
        assert len(self._instance1.sink_factories) == 0
        assert len(self._instance2.sink_factories) > 0

        self._instance1.clear_sink_factories()
        self._instance2.clear_sink_factories()

        assert len(self._instance1.sink_factories) == 0
        assert len(self._instance2.sink_factories) == 0

    def test_clear_sink_factories_return_value(self) -> None:
        """:meth:`WorkflowBuilder.clear_sink_factories` should return the
        current ``WorkflowBuilder`` instance, i.e. ``self``.
        """
        assert self._instance1.clear_sink_factories() is self._instance1
        assert self._instance2.clear_sink_factories() is self._instance2

    def test_clear_source_factories_side_effects(self) -> None:
        r""":meth:`WorkflowBuilder.clear_source_factories` should remove all
        registered :class:`Source`\ s from the builder.
        """
        assert len(self._instance1.source_factories) == 0
        assert len(self._instance2.source_factories) > 0

        self._instance1.clear_source_factories()
        self._instance2.clear_source_factories()

        assert len(self._instance1.source_factories) == 0
        assert len(self._instance2.source_factories) == 0

    def test_clear_source_factories_return_value(self) -> None:
        """:meth:`WorkflowBuilder.clear_source_factories` should return the
        current ``WorkflowBuilder`` instance, i.e. ``self``.
        """
        assert self._instance1.clear_source_factories() is self._instance1
        assert self._instance2.clear_source_factories() is self._instance2

    def test_composite_processor_factory_modification_with_valid_value_succeeds(  # noqa: E501
        self,
    ) -> None:
        """Setting a callable object to the
        :attr:`WorkflowBuilder.composite_processor_factory` attribute should
        succeed.
        """

        def _composite_processor_factory(
            processors: Sequence[Processor[Any, Any]],
        ) -> Processor[Any, Any]:
            return ProcessorPipe(processors)

        try:
            self._instance1.composite_processor_factory = ProcessorPipe
            self._instance2.composite_processor_factory = (
                _composite_processor_factory
            )
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.composite_processor_factory' "
                "attribute with a callable object SHOULD succeed. However, "
                f"the following exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.composite_processor_factory is ProcessorPipe
        assert (
            self._instance2.composite_processor_factory
            is _composite_processor_factory
        )

    def test_composite_processor_factory_modification_with_an_invalid_value_fails(  # noqa: E501
        self,
    ) -> None:
        """Setting a non-callable value to the
        :attr:`WorkflowBuilder.composite_processor_factory` attribute should
        raise a :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.composite_processor_factory = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'composite_processor_factory' MUST be a callable object."
            )

    def test_composite_processor_factory_return_value(self) -> None:
        """:attr:`WorkflowBuilder.composite_processor_factory` should return
        the factory function used to combine multiple processors.
        """
        assert (
            self._instance1.composite_processor_factory
            is ScatterGatherProcessor
        )
        assert self._instance2.composite_processor_factory is ProcessorPipe

    def test_composite_sink_factory_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a callable object to the
        :attr:`WorkflowBuilder.composite_sink_factory` attribute should
        succeed.
        """

        def _composite_sink_factory(sinks: Sequence[Sink[Any]]) -> Sink[Any]:
            return SplitSink(sinks)

        try:
            self._instance1.composite_sink_factory = SplitSink
            self._instance2.composite_sink_factory = _composite_sink_factory
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.composite_sink_factory' "
                "attribute with a callable object SHOULD succeed. However, "
                f"the following exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.composite_sink_factory is SplitSink
        assert (
            self._instance2.composite_sink_factory is _composite_sink_factory
        )

    def test_composite_sink_factory_modification_with_an_invalid_value_fails(
        self,
    ) -> None:
        """Setting a non-callable value to the
        :attr:`WorkflowBuilder.composite_sink_factory` attribute should raise a
        :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.composite_sink_factory = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'composite_sink_factory' MUST be a callable object."
            )

    def test_composite_sink_factory_return_value(self) -> None:
        """:attr:`WorkflowBuilder.composite_sink_factory` should return
        the factory function used to combine multiple sinks.
        """
        assert self._instance1.composite_sink_factory is ScatterSink
        assert self._instance2.composite_sink_factory is SplitSink

    def test_composite_source_factory_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a callable object to the
        :attr:`WorkflowBuilder.composite_source_factory` attribute should
        succeed.
        """

        def _composite_source_factory(
            sources: Sequence[Source[Any]],
        ) -> Source[Any]:
            return GatherSource(sources)

        try:
            self._instance1.composite_source_factory = GatherSource
            self._instance2.composite_source_factory = (
                _composite_source_factory
            )
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.composite_source_factory' "
                "attribute with a callable object SHOULD succeed. However, "
                f"the following exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.composite_source_factory is GatherSource
        assert (
            self._instance2.composite_source_factory
            is _composite_source_factory
        )

    def test_composite_source_factory_modification_with_an_invalid_value_fails(
        self,
    ) -> None:
        """Setting a non-callable value to the
        :attr:`WorkflowBuilder.composite_source_factory` attribute should raise
        a :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.composite_source_factory = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'composite_source_factory' MUST be a callable object."
            )

    def test_composite_source_factory_return_value(self) -> None:
        """:attr:`WorkflowBuilder.composite_source_factory` should return
        the factory function used to combine multiple sources.
        """
        assert self._instance1.composite_source_factory is GatherSource
        assert self._instance2.composite_source_factory is GatherSource

    def test_default_processor_factory_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a callable object to the
        :attr:`WorkflowBuilder.default_processor_factory` attribute should
        succeed.
        """

        def _processor_factory() -> Processor[Any, Any]:
            return NOOPProcessor()

        try:
            self._instance1.default_processor_factory = NOOPProcessor
            self._instance2.default_processor_factory = _processor_factory
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.default_processor_factory' "
                "attribute with a callable object SHOULD succeed. However, "
                f"the following exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.default_processor_factory is NOOPProcessor
        assert self._instance2.default_processor_factory is _processor_factory

    def test_default_processor_factory_modification_with_an_invalid_value_fails(  # noqa: E501
        self,
    ) -> None:
        """Setting a non-callable value to the
        :attr:`WorkflowBuilder.default_processor_factory` attribute should
        raise a :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.default_processor_factory = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'default_processor_factory' MUST be a callable object."
            )

    def test_default_processor_factory_return_value(self) -> None:
        """:attr:`WorkflowBuilder.default_processor_factory` should return
        the factory function used to create the default ``Processor`` when
        none is provided.
        """
        assert self._instance1.default_processor_factory is NOOPProcessor
        assert self._instance2.default_processor_factory is NOOPProcessor

    def test_default_sink_factory_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a callable object to the
        :attr:`WorkflowBuilder.default_sink_factory` attribute should succeed.
        """

        def _sink_factory() -> Sink[Any]:
            return NullSink()

        try:
            self._instance1.default_sink_factory = NullSink
            self._instance2.default_sink_factory = _sink_factory
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.default_sink_factory' "
                "attribute with a callable object SHOULD succeed. However, "
                f"the following exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.default_sink_factory is NullSink
        assert self._instance2.default_sink_factory is _sink_factory

    def test_default_sink_factory_modification_with_an_invalid_value_fails(
        self,
    ) -> None:
        """Setting a non-callable value to the
        :attr:`WorkflowBuilder.default_sink_factory` attribute should
        raise a :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.default_sink_factory = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'default_sink_factory' MUST be a callable object."
            )

    def test_default_sink_factory_return_value(self) -> None:
        """:attr:`WorkflowBuilder.default_sink_factory` should return the
        factory function used to create the default ``Sink`` when none is
        provided.
        """
        assert self._instance1.default_sink_factory is NullSink
        assert self._instance2.default_sink_factory is NullSink

    def test_description_modification_with_valid_value_succeeds(self) -> None:
        """Setting ``None`` or a non-empty string to the
        :attr:`WorkflowBuilder.description` attribute should succeed.
        """
        try:
            self._instance1.description = "A test ETL workflow."
            self._instance2.description = None
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.description' attribute with "
                "None or a non-empty string SHOULD succeed. However, the "
                f"following exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.description == "A test ETL workflow."
        assert self._instance2.description is None

    def test_description_modification_with_a_non_str_value_fails(self) -> None:
        """Setting a non-none or non-string value to the
        :attr:`WorkflowBuilder.description` should raise a :exc:`TypeError`.
        """
        for non_str in (1, 5.2, self._instance2):
            with pytest.raises(TypeError, match="be a string") as exp_info:
                self._instance1.name = non_str  # type: ignore[reportArgumentType]

            assert exp_info.value.args[0] == "'name' MUST be a string."

    def test_description_return_value(self) -> None:
        """:attr:`WorkflowBuilder.description` should return the description of
        the workflow.
        """
        assert self._instance1.description is None
        assert self._instance2.description == "A sample ETL workflow."

    def test_drain_to_fails_when_given_invalid_input(self) -> None:
        """The method :meth:`WorkflowBuilder.drain_to` should raise a
        :exc:`ValueError` when the given value IS NOT a :class:`Sink` instance
        or a callable object.
        """
        for non_sink in (None, 1, 5.2, {}, (), []):
            with pytest.raises(ValueError, match="Sink") as exp_info:
                self._instance1.drain_to(non_sink)  # type: ignore[reportArgumentType]

            assert exp_info.value.args[0] == (
                "'sink' MUST be an 'sghi.etl.core.Sink' instance or a "
                "factory function that returns an instance of the same "
                "type."
            )

    def test_drain_to_return_value(self) -> None:
        """The method meth:`WorkflowBuilder.drain_to` should return the current
        ``WorkflowBuilder`` instance, i.e. ``self``, on exit.
        """

        @sink
        def discard(values: Iterator[int]) -> None:
            for _ in values:
                ...

        assert self._instance1.drain_to(NullSink) is self._instance1
        assert self._instance2.drain_to(discard) is self._instance2

    def test_drain_to_side_effects(self) -> None:
        """The method :meth:`WorkflowBuilder.drain_to` should convert the
        given :class:`Sink` instance into a factory function that returns the
        ``Sink`` once and then append the factory function to the list of
        available sink factories.

        If the given value is a callable object, then append it to the list of
        available sink factories as is.
        """
        null = NullSink()

        self._instance1.drain_to(null)
        self._instance1.drain_to(NullSink)
        assert len(self._instance1.sink_factories) == 2
        assert callable(self._instance1.sink_factories[0])
        assert callable(self._instance1.sink_factories[1])
        assert self._instance1.sink_factories[1] is NullSink

        null_factory = self._instance1.sink_factories[0]
        # The 1st call should yield the original value
        assert null_factory() is null
        # Later calls should result in `SoleValueAlreadyRetrievedError` being
        # raised.
        for _ in range(10):
            with pytest.raises(SoleValueAlreadyRetrievedError) as exp_info:
                null_factory()

            assert (
                exp_info.value.message
                == "This factory's sole value has already been retrieved."
            )

    def test_drains_to_fails_when_given_invalid_input(self) -> None:
        """The decorator :meth:`WorkflowBuilder.drains_to` should raise a
        :exc:`TypeError` when the given value IS NOT a :class:`Sink` instance.
        """
        for non_sink in (None, 1, 5.2, str, Zero()):
            with pytest.raises(TypeError, match="Sink") as exp_info:
                self._instance1.drains_to(non_sink)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'sink' MUST be an 'sghi.etl.core.Sink' instance."
            )

    def test_drains_to_return_value(self) -> None:
        """The decorator meth:`WorkflowBuilder.drains_to` should return the
        :class:`Sink` instance given to it.
        """
        null = NullSink()

        @sink
        def discard(values: Iterator[int]) -> None:
            for _ in values:
                ...

        assert self._instance1.drains_to(null) is null
        assert self._instance2.drains_to(discard) is discard

    def test_drains_to_side_effects(self) -> None:
        """The decorator :meth:`WorkflowBuilder.drains_to` should convert the
        given :class:`Sink` instance into a factory function that returns the
        ``Sink`` once and then append the factory function to the list of
        available sink factories.
        """
        null = NullSink()

        self._instance1.drains_to(null)
        assert len(self._instance1.sink_factories) == 1
        assert callable(self._instance1.sink_factories[0])

        null_factory = self._instance1.sink_factories[0]
        # The 1st call should yield the original value
        assert null_factory() is null
        # Later calls should result in `SoleValueAlreadyRetrievedError` being
        # raised.
        for _ in range(10):
            with pytest.raises(SoleValueAlreadyRetrievedError) as exp_info:
                null_factory()

            assert (
                exp_info.value.message
                == "This factory's sole value has already been retrieved."
            )

    def test_draw_from_fails_when_given_invalid_input(self) -> None:
        """The method :meth:`WorkflowBuilder.draw_from` should raise a
        :exc:`ValueError` when the given value IS NOT a :class:`Source`
        instance or a callable object.
        """
        for non_source in (None, 1, 5.2, {}, (), []):
            with pytest.raises(ValueError, match="Source") as exp_info:
                self._instance1.draw_from(non_source)  # type: ignore[reportArgumentType]

            assert exp_info.value.args[0] == (
                "'source' MUST be an 'sghi.etl.core.Source' instance or a "
                "factory function that returns an instance of the same "
                "type."
            )

    def test_draw_from_return_value(self) -> None:
        """The method meth:`WorkflowBuilder.draw_from` should return the
        current ``WorkflowBuilder`` instance, i.e. ``self``, on exit.
        """

        @source
        def empty() -> Iterator[None]:
            while True:
                yield None

        assert self._instance1.draw_from(Zero) is self._instance1
        assert self._instance2.draw_from(empty) is self._instance2

    def test_draw_from_side_effects(self) -> None:
        """The method :meth:`WorkflowBuilder.draw_from` should convert the
        given :class:`Source` instance into a factory function that returns the
        ``Source`` once and then append the factory function to the list of
        available source factories.

        If the given value is a callable object, then append it to the list of
        available source factories as is.
        """
        zero = Zero()

        self._instance1.draw_from(zero)
        self._instance1.draw_from(Zero)
        assert len(self._instance1.source_factories) == 2
        assert callable(self._instance1.source_factories[0])
        assert callable(self._instance1.source_factories[1])
        assert self._instance1.source_factories[1] is Zero

        zero_factory = self._instance1.source_factories[0]
        # The 1st call should yield the original value
        assert zero_factory() is zero
        # Later calls should result in `SoleValueAlreadyRetrievedError` being
        # raised.
        for _ in range(10):
            with pytest.raises(SoleValueAlreadyRetrievedError) as exp_info:
                zero_factory()

            assert (
                exp_info.value.message
                == "This factory's sole value has already been retrieved."
            )

    def test_draws_from_fails_when_given_invalid_input(self) -> None:
        """The decorator :meth:`WorkflowBuilder.draws_from` should raise a
        :exc:`TypeError` when the given value IS NOT a :class:`Source`
        instance.
        """
        for non_source in (None, 1, 5.2, str, NOOPProcessor()):
            with pytest.raises(TypeError, match="Source") as exp_info:
                self._instance1.draws_from(non_source)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'source' MUST be an 'sghi.etl.core.Source' instance."
            )

    def test_epilogue_modification_with_valid_value_succeeds(self) -> None:
        """Setting a callable object to the :attr:`WorkflowBuilder.epilogue`
        attribute should succeed.
        """

        def _remove_temp_directories() -> None:
            shutil.rmtree(
                path="/tmp/sghi-etl-workflow-tmp-dir",  # noqa: S108
                ignore_errors=True,
            )

        try:
            self._instance1.epilogue = _noop
            self._instance2.epilogue = _remove_temp_directories
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.epilogue' attribute with a "
                "callable object SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.epilogue is _noop
        assert self._instance2.epilogue is _remove_temp_directories

    def test_epilogue_modification_with_an_invalid_value_fails(self) -> None:
        """Setting a non-callable value to the :attr:`WorkflowBuilder.epilogue`
        attribute should raise a :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.epilogue = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'epilogue' MUST be a callable object."
            )

    def test_epilogue_return_value(self) -> None:
        r""":attr:`WorkflowBuilder.epilogue` should return the callable object
        to execute at the end of the assembled ``WorkflowDefinition``\ (s).
        """
        assert callable(self._instance1.epilogue)
        assert self._instance2.epilogue is _noop

    def test_draws_from_return_value(self) -> None:
        """The decorator meth:`WorkflowBuilder.draws_from` should return the
        :class:`Source` instance given to it.
        """
        zero = Zero()

        @source
        def empty() -> Iterator[None]:
            while True:
                yield None

        assert self._instance1.draws_from(zero) is zero
        assert self._instance2.draws_from(empty) is empty

    def test_draws_from_side_effects(self) -> None:
        """The decorator :meth:`WorkflowBuilder.draws_from` should convert the
        given :class:`Source` instance into a factory function that returns the
        ``Source`` once and then append the factory function to the list of
        available sink factories.
        """
        zero = Zero()

        self._instance1.draws_from(zero)
        assert len(self._instance1.source_factories) == 1
        assert callable(self._instance1.source_factories[0])

        zero_factory = self._instance1.source_factories[0]
        # The 1st call should yield the original value
        assert zero_factory() is zero
        # Later calls should result in `SoleValueAlreadyRetrievedError` being
        # raised.
        for _ in range(10):
            with pytest.raises(SoleValueAlreadyRetrievedError) as exp_info:
                zero_factory()

            assert (
                exp_info.value.message
                == "This factory's sole value has already been retrieved."
            )

    def test_id_modification_with_valid_value_succeeds(self) -> None:
        """Setting a non-empty string to the :attr:`WorkflowBuilder.id`
        attribute should succeed.
        """
        try:
            self._instance1.id = "sample_workflow_1"
            self._instance2.id = "sample_workflow_2"
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.id' attribute with a non-empty "
                "string SHOULD succeed. However, the following exception was "
                f"raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.id == "sample_workflow_1"
        assert self._instance2.id == "sample_workflow_2"

    def test_id_modification_with_a_non_str_value_fails(self) -> None:
        """Setting a non-string value to the :attr:`WorkflowBuilder.id` should
        raise a :exc:`TypeError`.
        """
        for non_str in (None, 1, 5.2, self._instance2):
            with pytest.raises(TypeError, match="be a string") as exp_info:
                self._instance1.id = non_str  # type: ignore[reportArgumentType]

            assert exp_info.value.args[0] == "'id' MUST be a string."

    def test_id_modification_with_an_empty_str_value_fails(self) -> None:
        """Setting an empty string to the :attr:`WorkflowBuilder.id` attribute
        should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="an empty string") as exp_info:
            self._instance2.id = ""

        assert exp_info.value.args[0] == "'id' MUST NOT be an empty string."

    def test_id_return_value(self) -> None:
        """:attr:`WorkflowBuilder.id` should return the unique identifier of
        the workflow.
        """
        assert self._instance1.id == "test_workflow_1"
        assert self._instance2.id == "test_workflow_2"

    def test_invoking_instances_as_callable_return_value(self) -> None:
        """Invoking instances of ``WorkflowBuilder`` as callables shoyld return
        the same value as invoking :meth:`WorkflowBuilder.build`.
        """
        wb: WorkflowBuilder[Any, Any] = self._instance1
        wb.draw_from(Zero).apply_processor(NOOPProcessor).drain_to(NullSink)

        workflow_def: WorkflowDefinition[Any, Any] = wb()
        assert isinstance(workflow_def, WorkflowDefinition)
        assert workflow_def.id == "test_workflow_1"
        assert workflow_def.name == "Test Workflow One"
        assert workflow_def.description is None
        assert workflow_def.source_factory is Zero
        assert workflow_def.processor_factory is NOOPProcessor
        assert workflow_def.sink_factory is NullSink

    def test_mark_epilogue_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Invoking the :meth:`WorkflowBuilder.mark_epilogue` method with a
        valid callable should succeed.
        """
        try:
            self._instance1.mark_epilogue(_noop)
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Invoking the 'WorkflowBuilder.mark_epilogue' method with "
                "a callable object SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        @self._instance2.mark_epilogue
        def _remove_temp_directories() -> None:
            shutil.rmtree(
                path="/tmp/sghi-etl-workflow-tmp-dir",  # noqa: S108
                ignore_errors=True,
            )

        assert self._instance1.epilogue is _noop
        assert self._instance2.epilogue is _remove_temp_directories

    def test_mark_epilogue_modification_with_an_invalid_value_fails(
        self,
    ) -> None:
        """Passing a non-callable value to the
        :meth:`WorkflowBuilder.mark_epilogue` method should raise a
        :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.mark_epilogue(non_callable)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'epilogue' MUST be a callable object."
            )

    def test_mark_epilogue_return_value(self) -> None:
        """:meth:`WorkflowBuilder.mark_epilogue` should return the callable
        object passed to it.
        """

        def _remove_temp_directories() -> None:
            shutil.rmtree(
                path="/tmp/sghi-etl-workflow-tmp-dir",  # noqa: S108
                ignore_errors=True,
            )

        assert self._instance1.mark_epilogue(_noop) is _noop
        assert (
            self._instance2.mark_epilogue(_remove_temp_directories)
            is _remove_temp_directories
        )

    def test_mark_prologue_modification_with_an_invalid_value_fails(
        self,
    ) -> None:
        """Passing a non-callable value to the
        :meth:`WorkflowBuilder.mark_prologue` method should raise a
        exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.mark_prologue(non_callable)  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'prologue' MUST be a callable object."
            )

    def test_mark_prologue_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Invoking the :meth:`WorkflowBuilder.mark_prologue` method with a
        valid callable should succeed.
        """
        try:
            self._instance1.mark_prologue(_noop)
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Invoking the 'WorkflowBuilder.prologue' method with a "
                "callable object SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        @self._instance2.mark_prologue
        def _check_properly_configured() -> None:
            if not os.environ.get("DB_PASSWORD", None):
                _err_msg: str = "'DB_PASSWORD' not specified."
                raise RuntimeError(_err_msg)

        assert self._instance1.prologue is _noop
        assert self._instance2.prologue is _check_properly_configured

    def test_mark_prologue_return_value(self) -> None:
        """:meth:`WorkflowBuilder.mark_prologue` should return the callable
        object passed to it.
        """

        def _check_properly_configured() -> None:
            if not os.environ.get("DB_PASSWORD", None):
                _err_msg: str = "'DB_PASSWORD' not specified."
                raise RuntimeError(_err_msg)

        assert self._instance1.mark_prologue(_noop) is _noop
        assert (
            self._instance2.mark_epilogue(_check_properly_configured)
            is _check_properly_configured
        )

    def test_name_modification_with_valid_value_succeeds(self) -> None:
        """Setting a non-empty string to the :attr:`WorkflowBuilder.name`
        attribute should succeed.
        """
        try:
            self._instance1.name = "Sample Workflow One"
            self._instance2.name = "Sample Workflow Two"
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.name' attribute with a "
                "non-empty string SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.name == "Sample Workflow One"
        assert self._instance2.name == "Sample Workflow Two"

    def test_name_modification_with_a_non_str_value_fails(self) -> None:
        """Setting a non-string value to the :attr:`WorkflowBuilder.name`
        should raise a :exc:`TypeError`.
        """
        for non_str in (None, 1, 5.2, self._instance2):
            with pytest.raises(TypeError, match="be a string") as exp_info:
                self._instance1.name = non_str  # type: ignore[reportArgumentType]

            assert exp_info.value.args[0] == "'name' MUST be a string."

    def test_name_modification_with_an_empty_str_value_fails(self) -> None:
        """Setting an empty string to the :attr:`WorkflowBuilder.name`
        attribute should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="an empty string") as exp_info:
            self._instance2.name = ""

        assert exp_info.value.args[0] == "'name' MUST NOT be an empty string."

    def test_name_return_value(self) -> None:
        """:attr:`WorkflowBuilder.name` should return the name of the workflow."""  # noqa: E501
        assert self._instance1.name == "Test Workflow One"
        assert self._instance2.name == "Test Workflow Two"

    def test_processor_factories_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a ``Sequence`` to the
        :attr:`WorkflowBuilder.processor_factories` attribute should succeed.
        """
        try:
            self._instance1.processor_factories = [NOOPProcessor]
            self._instance2.processor_factories = ()
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.processor_factories' attribute "
                "with a Sequence SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert len(self._instance1.processor_factories) == 1
        assert len(self._instance2.processor_factories) == 0
        assert self._instance1.processor_factories[0] is NOOPProcessor

    def test_processor_factories_modification_with_a_non_str_value_fails(
        self,
    ) -> None:
        """Setting a non-Sequence value to the
        :attr:`WorkflowBuilder.processor_factories` should raise a
        :exc:`TypeError`.
        """
        for non_str in (None, 1, 5.2, self._instance2, {}):
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                self._instance1.processor_factories = non_str  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'processor_factories' MUST be a collections.abc.Sequence."
            )

    def test_processor_factories_return_value(self) -> None:
        r""":attr:`WorkflowBuilder.processor_factories` should return a
        ``Sequence`` of the factories registered to create ``Processor``\ s
        for the assembled ``WorkflowDefinition``\ (s).
        """
        assert len(self._instance1.processor_factories) == 0
        assert len(self._instance2.processor_factories) == 1
        assert self._instance2.processor_factories[0] == NOOPProcessor

    def test_prologue_modification_with_an_invalid_value_fails(self) -> None:
        """Setting a non-callable value to the :attr:`WorkflowBuilder.prologue`
        attribute should raise a :exc:`ValueError`.
        """
        for non_callable in (None, 1, 5.2, "not a callable"):
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                self._instance1.prologue = non_callable  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'prologue' MUST be a callable object."
            )

    def test_prologue_modification_with_valid_value_succeeds(self) -> None:
        """Setting a callable object to the :attr:`WorkflowBuilder.prologue`
        attribute should succeed.
        """

        def _check_properly_configured() -> None:
            if not os.environ.get("DB_PASSWORD", None):
                _err_msg: str = "'DB_PASSWORD' not specified."
                raise RuntimeError(_err_msg)

        try:
            self._instance1.prologue = _noop
            self._instance2.prologue = _check_properly_configured
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.prologue' attribute with a "
                "callable object SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert self._instance1.prologue is _noop
        assert self._instance2.prologue is _check_properly_configured

    def test_prologue_return_value(self) -> None:
        r""":attr:`WorkflowBuilder.prologue` should return the callable object
        to execute at the end of the assembled ``WorkflowDefinition``\ (s).
        """
        assert callable(self._instance1.prologue)
        assert self._instance2.prologue is _noop

    def test_sink_factories_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a ``Sequence`` to the :attr:`WorkflowBuilder.sink_factories`
        attribute should succeed.
        """
        try:
            self._instance1.sink_factories = [NullSink]
            self._instance2.sink_factories = ()
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.sink_factories' attribute with "
                "a Sequence SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert len(self._instance1.sink_factories) == 1
        assert len(self._instance2.sink_factories) == 0
        assert self._instance1.sink_factories[0] is NullSink

    def test_sink_factories_modification_with_a_non_str_value_fails(
        self,
    ) -> None:
        """Setting a non-Sequence value to the
        :attr:`WorkflowBuilder.sink_factories` should raise a :exc:`TypeError`.
        """
        for non_str in (None, 1, 5.2, self._instance2, {}):
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                self._instance1.sink_factories = non_str  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'sink_factories' MUST be a collections.abc.Sequence."
            )

    def test_sink_factories_return_value(self) -> None:
        r""":attr:`WorkflowBuilder.sink_factories` should return a
        ``Sequence`` of the factories registered to create ``Sink``\ s
        for the assembled ``WorkflowDefinition``\ (s).
        """
        assert len(self._instance1.sink_factories) == 0
        assert len(self._instance2.sink_factories) == 1
        assert self._instance2.sink_factories[0] == NullSink

    def test_source_factories_modification_with_valid_value_succeeds(
        self,
    ) -> None:
        """Setting a ``Sequence`` to the
        :attr:`WorkflowBuilder.source_factories` attribute should succeed.
        """
        try:
            self._instance1.source_factories = [Zero]
            self._instance2.source_factories = ()
        except Exception as exp:  # noqa: BLE001
            _fail_reason: str = (
                "Setting the 'WorkflowBuilder.source_factories' attribute "
                "with a Sequence SHOULD succeed. However, the following "
                f"exception was raised: '{exp!r}'."
            )
            pytest.fail(reason=_fail_reason)

        assert len(self._instance1.source_factories) == 1
        assert len(self._instance2.source_factories) == 0
        assert self._instance1.source_factories[0] is Zero

    def test_source_factories_modification_with_a_non_str_value_fails(
        self,
    ) -> None:
        """Setting a non-Sequence value to the
        :attr:`WorkflowBuilder.source_factories` should raise a
        :exc:`TypeError`.
        """
        for non_str in (None, 1, 5.2, self._instance2, {}):
            with pytest.raises(TypeError, match="Sequence") as exp_info:
                self._instance1.source_factories = non_str  # type: ignore[reportArgumentType]

            assert (
                exp_info.value.args[0]
                == "'source_factories' MUST be a collections.abc.Sequence."
            )

    def test_source_factories_return_value(self) -> None:
        r""":attr:`WorkflowBuilder.source_factories` should return a
        ``Sequence`` of the factories registered to create ``Source``\ s
        for the assembled ``WorkflowDefinition``\ (s).
        """
        assert len(self._instance1.source_factories) == 0
        assert len(self._instance2.source_factories) == 1
        assert self._instance2.source_factories[0] == Zero

    def test_string_representation(self) -> None:
        """The :meth:`WorkflowBuilder.__str__` and
        :meth:`WorkflowBuilder.__repr__` methods should return a string
        representation of the current ``WorkflowBuilder`` instance.
        """
        instance1_str: str = str(self._instance1)
        instance1_rpr: str = repr(self._instance1)
        instance2_str: str = str(self._instance2)
        instance2_rpr: str = repr(self._instance2)

        assert (
            instance1_str
            == instance1_rpr
            == (
                "WorkflowBuilder(id=test_workflow_1, name=Test Workflow One, "
                "description=None)"
            )
        )
        assert (
            instance2_str
            == instance2_rpr
            == (
                "WorkflowBuilder(id=test_workflow_2, name=Test Workflow Two, "
                "description=A sample ETL workflow.)"
            )
        )
