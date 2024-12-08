# ruff: noqa: D205
"""Tests for the ``sghi.etl.commons.utils.others`` module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sghi.etl.commons import (
    NOOPProcessor,
    NullSink,
    ProcessorPipe,
    SimpleWorkflowDefinition,
    processor,
    run_workflow,
    sink,
    source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, MutableSequence

    from sghi.etl.core import WorkflowDefinition

# =============================================================================
# HELPERS
# =============================================================================


def _workflow_factory_generator(
    repository: MutableSequence[str],
    start: int = 0,
    stop: int = 5,
    step: int = 1,
) -> Callable[[], WorkflowDefinition[Iterable[int], Iterable[str]]]:
    @source
    def supply_ints() -> Iterable[int]:
        yield from range(start, stop, step)

    @processor
    def add_100(values: Iterable[int]) -> Iterable[int]:
        for v in values:
            yield v + 100

    @processor
    def ints_as_strings(ints: Iterable[int]) -> Iterable[str]:
        yield from map(str, ints)

    @sink
    def save_strings_to_repo(strings: Iterable[str]) -> None:
        repository.extend(strings)

    def _create_workflow() -> WorkflowDefinition[Iterable[int], Iterable[str]]:
        return SimpleWorkflowDefinition(
            id="test_workflow",
            name="Test Workflow",
            source_factory=lambda: supply_ints,
            processor_factory=lambda: ProcessorPipe(
                [add_100, ints_as_strings],
            ),
            sink_factory=lambda: save_strings_to_repo,
        )

    return _create_workflow


# =============================================================================
# TEST CASES
# =============================================================================


def test_run_workflow_fails_on_non_callable_input() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should raise a
    :exc:`ValueError` when given a non-callable input value.
    """
    wf = _workflow_factory_generator([])
    for non_callable in (None, wf()):
        with pytest.raises(ValueError, match="callable object.") as exp_info:
            run_workflow(wf=non_callable)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0] == "'wf' MUST be a valid callable object."
        )


def test_run_workflow_side_effects_on_failed_execution() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should dispose all the
    workflow components (source, processor and sink) if an error occurs during
    execution.
    """

    @source
    def failing_source() -> str:
        _err_msg: str = "Oops, something failed."
        raise RuntimeError(_err_msg)

    _processor = NOOPProcessor()
    _sink = NullSink()

    def create_failing_workflow() -> WorkflowDefinition[str, str]:
        return SimpleWorkflowDefinition(
            id="failing_workflow",
            name="Failing Workflow",
            source_factory=lambda: failing_source,
            processor_factory=lambda: _processor,
            sink_factory=lambda: _sink,
        )

    with pytest.raises(RuntimeError, match="Oops, something failed."):
        run_workflow(wf=create_failing_workflow)

    assert failing_source.is_disposed
    assert _processor.is_disposed
    assert _sink.is_disposed


def test_run_workflow_side_effects_on_successful_execution() -> None:
    """func:`sghi.etl.commons.utils.run_workflow` should execute an ETL
    Workflow when given a factory function that returns the workflow.
    """
    repository1: list[str] = []
    repository2: list[str] = []
    wf1 = _workflow_factory_generator(repository1)
    wf2 = _workflow_factory_generator(repository2, 10, 60, 10)

    run_workflow(wf1)
    run_workflow(wf2)

    assert repository1 == ["100", "101", "102", "103", "104"]
    assert repository2 == ["110", "120", "130", "140", "150"]
