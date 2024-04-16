# ruff: noqa: D205
"""Tests for the :module:`sghi.etl.commons.workflow_definitions` module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest import TestCase

import pytest

from sghi.etl.commons import SimpleWorkflowDefinition, processor, sink, source

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sghi.etl.core import Processor, Sink, Source, WorkflowDefinition

# =============================================================================
# TESTS HELPERS
# =============================================================================


def _ints_supplier_factory() -> Source[Iterable[int]]:
    @source
    def supply_ints(count: int = 5) -> Iterable[int]:
        yield from range(count)

    return supply_ints


def _ints_to_str_mapper_factory() -> Processor[Iterable[int], Iterable[str]]:
    @processor
    def ints_to_str(ints: Iterable[int]) -> Iterable[str]:
        yield from map(chr, ints)

    return ints_to_str


def _printer_factory() -> Sink[Iterable[Any]]:
    return sink(print)


# =============================================================================
# TESTS
# =============================================================================


class TestSimpleWorkflowDefinitions(TestCase):
    """Tests for :class:`sghi.etl.commons.SimpleWorkflowDefinition` class."""

    def test_accessors_return_the_expected_values(self) -> None:
        """:class:`SimpleWorkflowDefinition` accessors should return the
        assigned values at creation.
        """
        wf_def: WorkflowDefinition[Iterable[int], Iterable[str]]
        wf_def = SimpleWorkflowDefinition(
            id="test_workflow",
            name="Test Workflow",
            source_factory=_ints_supplier_factory,
            description="A test workflow that takes ints and prints all them.",
            processor_factory=_ints_to_str_mapper_factory,
            sink_factory=_printer_factory,
        )

        assert wf_def.id == "test_workflow"
        assert wf_def.name == "Test Workflow"
        assert (
            wf_def.description
            == "A test workflow that takes ints and prints all them."
        )
        assert wf_def.source_factory is _ints_supplier_factory
        assert wf_def.processor_factory is _ints_to_str_mapper_factory
        assert wf_def.sink_factory is _printer_factory

    def test_instantiation_fails_on_none_str_id_argument(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with a non-string
        ``id`` argument should raise a :exc:`TypeError`.
        """
        invalid_ids = (None, 1, [], _ints_supplier_factory)

        for invalid_id in invalid_ids:
            with pytest.raises(TypeError, match="be a string") as exp_info:
                SimpleWorkflowDefinition(
                    id=invalid_id,  # type: ignore
                    name="Test Workflow",
                    source_factory=_ints_supplier_factory,
                )

            assert exp_info.value.args[0] == "'id' MUST be a string."

    def test_instantiation_fails_on_an_empty_str_id_argument(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with an empty
        ``id`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="NOT be an empty") as exp_info:
            SimpleWorkflowDefinition(
                id="",
                name="Test Workflow",
                source_factory=_ints_supplier_factory,
            )

        assert exp_info.value.args[0] == "'id' MUST NOT be an empty string."

    def test_instantiation_fails_on_none_str_name_argument(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with a non-string
        ``name`` argument should raise a :exc:`TypeError`.
        """
        invalid_names = (None, 1, [], _ints_supplier_factory)

        for invalid_name in invalid_names:
            with pytest.raises(TypeError, match="be a string") as exp_info:
                SimpleWorkflowDefinition(
                    id="test_workflow",
                    name=invalid_name,  # type: ignore
                    source_factory=_ints_supplier_factory,
                )

            assert exp_info.value.args[0] == "'name' MUST be a string."

    def test_instantiation_fails_on_an_empty_str_name_argument(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with an empty
        ``name`` argument should raise a :exc:`ValueError`.
        """
        with pytest.raises(ValueError, match="NOT be an empty") as exp_info:
            SimpleWorkflowDefinition(
                id="test_workflow",
                name="",
                source_factory=_ints_supplier_factory,
            )

        assert exp_info.value.args[0] == "'name' MUST NOT be an empty string."

    def test_instantiation_fails_on_invalid_source_factory_arg(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with a
        non-callable ``source_factory`` argument should raise a
        :exc:`ValueError`.
        """
        invalid_src_factories = (None, 1, [], (), "not a callable")

        for invalid_src_factory in invalid_src_factories:
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                SimpleWorkflowDefinition(
                    id="test_workflow",
                    name="Test Workflow",
                    source_factory=invalid_src_factory,  # type: ignore
                )

            assert (
                exp_info.value.args[0]
                == "'source_factory' MUST be a callable object."
            )

    def test_instantiation_fails_on_invalid_description_argument(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with a non-string
        or ``None`` ``description`` fails.
        """
        invalid_descriptions = (1, (), [], _ints_supplier_factory)

        for invalid_description in invalid_descriptions:
            with pytest.raises(TypeError, match="be a string") as exp_info:
                SimpleWorkflowDefinition(
                    id="test_workflow",
                    name="Test Workflow",
                    source_factory=_ints_supplier_factory,
                    description=invalid_description,  # type: ignore
                )

            assert (
                exp_info.value.args[0]
                == "'description' MUST be a string when NOT None."
            )

    def test_instantiation_fails_on_invld_processor_factory_arg(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with a
        non-callable ``processor_factory`` argument should raise a
        :exc:`ValueError`.
        """
        invalid_prc_factories = (None, 1, [], (), "not a callable")

        for invalid_prc_factory in invalid_prc_factories:
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                SimpleWorkflowDefinition(
                    id="test_workflow",
                    name="Test Workflow",
                    source_factory=_ints_supplier_factory,
                    processor_factory=invalid_prc_factory,  # type: ignore
                )

            assert (
                exp_info.value.args[0]
                == "'processor_factory' MUST be a callable object."
            )

    def test_instantiation_fails_on_invalid_sink_factory_arg(self) -> None:
        """Instantiating a :class:`SimpleWorkflowDefinition` with a
        non-callable ``sink_factory`` argument should raise a
        :exc:`ValueError`.
        """
        invalid_sink_factories = (None, 1, [], (), "not a callable")

        for invalid_sink_factory in invalid_sink_factories:
            with pytest.raises(ValueError, match="be a callable") as exp_info:
                SimpleWorkflowDefinition(
                    id="test_workflow",
                    name="Test Workflow",
                    source_factory=_ints_supplier_factory,
                    sink_factory=invalid_sink_factory,  # type: ignore
                )

            assert (
                exp_info.value.args[0]
                == "'sink_factory' MUST be a callable object."
            )
