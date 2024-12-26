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
    WorkflowBuilder,
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


def _noop() -> None:
    """Do nothing."""


def _create_workflow_factory(  # noqa: PLR0913
    repository: MutableSequence[str],
    start: int = 0,
    stop: int = 5,
    step: int = 1,
    prologue: Callable[[], None] = _noop,
    epilogue: Callable[[], None] = _noop,
) -> Callable[[], WorkflowDefinition[Iterable[int], Iterable[str]]]:
    wb: WorkflowBuilder[Iterable[int], Iterable[str]]
    wb = WorkflowBuilder(
        id="test_workflow",
        name="Test Workflow",
        composite_processor_factory=ProcessorPipe,
        prologue=prologue,
        epilogue=epilogue,
    )

    @wb.draws_from
    @source
    def supply_ints() -> Iterable[int]:  # pyright: ignore[reportUnusedFunction]
        yield from range(start, stop, step)

    @wb.applies_processor
    @processor
    def add_100(values: Iterable[int]) -> Iterable[int]:  # pyright: ignore[reportUnusedFunction]
        for v in values:
            yield v + 100

    @wb.applies_processor
    @processor
    def ints_as_strings(ints: Iterable[int]) -> Iterable[str]:  # pyright: ignore[reportUnusedFunction]
        yield from map(str, ints)

    @wb.drains_to
    @sink
    def save_strings_to_repo(strings: Iterable[str]) -> None:  # pyright: ignore[reportUnusedFunction]
        repository.extend(strings)

    return wb


# =============================================================================
# TEST CASES
# =============================================================================


def test_run_workflow_fails_on_non_callable_input() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should raise a
    :exc:`ValueError` when given a non-callable input value.
    """
    wf = _create_workflow_factory([])
    for non_callable in (None, wf()):
        with pytest.raises(ValueError, match="callable object.") as exp_info:
            run_workflow(wf=non_callable)  # type: ignore[reportArgumentType]

        assert (
            exp_info.value.args[0] == "'wf' MUST be a valid callable object."
        )


def test_run_workflow_side_effects_on_failed_drain() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should dispose all the
    workflow components (source, processor and sink) and invoke the
    ``epilogue`` callable of the workflow if an error occurs while draining
    data to a :class:`Sink`.
    """

    @source
    def greate_the_world() -> str:
        return "Hello World!!"

    @sink
    def failing_sink(_: str) -> None:
        _err_msg: str = "Oops, something failed."
        raise RuntimeError(_err_msg)

    _processor = NOOPProcessor()
    _has_cleaned_up: bool = False

    def clean_up() -> None:
        nonlocal _has_cleaned_up
        _has_cleaned_up = True

    def create_failing_workflow() -> WorkflowDefinition[str, str]:
        return SimpleWorkflowDefinition(
            id="failing_workflow",
            name="Failing Workflow",
            source_factory=lambda: greate_the_world,
            processor_factory=lambda: _processor,
            sink_factory=lambda: failing_sink,
            epilogue=clean_up,
        )

    with pytest.raises(RuntimeError, match="Oops, something failed."):
        run_workflow(wf=create_failing_workflow)

    assert greate_the_world.is_disposed
    assert _processor.is_disposed
    assert failing_sink.is_disposed
    assert _has_cleaned_up


def test_run_workflow_side_effects_on_failed_draw() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should dispose all the
    workflow components (source, processor and sink) and invoke the
    ``epilogue`` callable of the workflow if an error occurs while drawing data
    rom a :class:`Source`.
    """

    # noinspection PyTypeChecker
    @source
    def failing_source() -> str:
        _err_msg: str = "Oops, something failed."
        raise RuntimeError(_err_msg)

    _processor = NOOPProcessor()
    _sink = NullSink()
    _has_cleaned_up: bool = False

    def clean_up() -> None:
        nonlocal _has_cleaned_up
        _has_cleaned_up = True

    def create_failing_workflow() -> WorkflowDefinition[str, str]:
        return SimpleWorkflowDefinition(
            id="failing_workflow",
            name="Failing Workflow",
            source_factory=lambda: failing_source,
            processor_factory=lambda: _processor,
            sink_factory=lambda: _sink,
            epilogue=clean_up,
        )

    with pytest.raises(RuntimeError, match="Oops, something failed."):
        run_workflow(wf=create_failing_workflow)

    assert failing_source.is_disposed
    assert _processor.is_disposed
    assert _sink.is_disposed
    assert _has_cleaned_up


def test_run_workflow_side_effects_on_failed_processing() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should dispose all the
    workflow components (source, processor and sink) and invoke the
    ``epilogue`` callable of the workflow if an error occurs while processing
    data.
    """

    @source
    def greate_the_world() -> str:
        return "Hello World!!"

    # noinspection PyTypeChecker
    @processor
    def failing_processor(_: str) -> str:
        _err_msg: str = "Oops, something failed."
        raise RuntimeError(_err_msg)

    _sink = NullSink()
    _has_cleaned_up: bool = False

    def clean_up() -> None:
        nonlocal _has_cleaned_up
        _has_cleaned_up = True

    def create_failing_workflow() -> WorkflowDefinition[str, str]:
        return SimpleWorkflowDefinition(
            id="failing_workflow",
            name="Failing Workflow",
            source_factory=lambda: greate_the_world,
            processor_factory=lambda: failing_processor,
            sink_factory=lambda: _sink,
            epilogue=clean_up,
        )

    with pytest.raises(RuntimeError, match="Oops, something failed."):
        run_workflow(wf=create_failing_workflow)

    assert greate_the_world.is_disposed
    assert failing_processor.is_disposed
    assert _sink.is_disposed
    assert _has_cleaned_up


def test_run_workflow_side_effects_on_failed_prologue_execution() -> None:
    """:func:`sghi.etl.commons.utils.run_workflow` should  invoke the
    ``epilogue`` callable of the workflow if an error occurs while executing
    the ``prologue`` callable of the workflow.
    """

    def failing_prologue() -> None:
        _err_msg: str = "Oops, something failed."
        raise RuntimeError(_err_msg)

    _has_cleaned_up: bool = False

    def clean_up() -> None:
        nonlocal _has_cleaned_up
        _has_cleaned_up = True

    wf = _create_workflow_factory(
        repository=[],
        prologue=failing_prologue,
        epilogue=clean_up,
    )

    with pytest.raises(RuntimeError, match="Oops, something failed."):
        run_workflow(wf)

    assert _has_cleaned_up


def test_run_workflow_side_effects_on_successful_execution() -> None:
    """func:`sghi.etl.commons.utils.run_workflow` should execute an ETL
    Workflow when given a factory function that returns the workflow.
    """
    has_set_up: bool = False
    has_cleaned_up: bool = False

    def set_up() -> None:
        nonlocal has_set_up
        has_set_up = True

    def clean_up() -> None:
        nonlocal has_cleaned_up
        has_cleaned_up = True

    repository1: list[str] = []
    repository2: list[str] = []

    wf1 = _create_workflow_factory(repository1)
    wf2 = _create_workflow_factory(repository2, 10, 60, 10, set_up, clean_up)

    run_workflow(wf1)
    run_workflow(wf2)

    assert repository1 == ["100", "101", "102", "103", "104"]
    assert repository2 == ["110", "120", "130", "140", "150"]
    assert has_cleaned_up
    assert has_set_up
