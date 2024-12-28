"""Other useful utilities."""

from __future__ import annotations

import logging
from logging import Logger
from typing import TYPE_CHECKING, Final, TypeVar

from sghi.etl.core import WorkflowDefinition
from sghi.utils import ensure_predicate, type_fqn

if TYPE_CHECKING:
    from collections.abc import Callable


# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Type variable representing the data type after processing."""

_RDT = TypeVar("_RDT")
"""Type variable representing the raw data type."""


# =============================================================================
# CONSTANTS
# =============================================================================


_LOGGER: Final[Logger] = logging.getLogger(name=__name__)


# =============================================================================
# UTILITIES
# =============================================================================


def run_workflow(
    wf: Callable[[], WorkflowDefinition[_RDT, _PDT]]
    | WorkflowDefinition[_RDT, _PDT],
) -> None:
    """Execute an ETL :class:`Workflow<WorkflowDefinition>`.

    .. tip::

        In the context of this function, **"ETL Workflow"** or the shorter
        version **"Workflow”**, refers to an instance of the
        :class:`WorkflowDefinition` class that is being executed or about to
        be executed.

    This function accepts an ETL ``WorkflowDefinition`` instance or factory
    function that supplies a ``WorkflowDefinition`` instance. If a factory
    function is provided, it is first invoked to get the
    ``WorkflowDefinition``/workflow before execution of the workflow starts.
    The execution of the workflow proceeds as follows:

        1. The callable returned by the
           :attr:`~sghi.etl.core.WorkflowDefinition.prologue` property is
           invoked first. If an error occurs while executing the callable, all
           the rest of the steps, except the last, are skipped.
        2. The callable returned by the
           :attr:`~sghi.etl.core.WorkflowDefinition.source_factory` property of
           the supplied ``WorkflowDefinition`` is used to get the
           :class:`~sghi.etl.core.Source` associated with the workflow. The
           :meth:`~sghi.etl.core.Source.draw` method of this ``Source`` is then
           invoked to get the raw data to process. If an error occurs while
           drawing data for the ``Source``, execution jumps to step 5.
        3. The callable returned by the
           :attr:`~sghi.etl.core.WorkflowDefinition.processor_factory` property
           of the supplied ``WorkflowDefinition`` is invoked to get the
           :class:`~sghi.etl.core.Processor` associated with the workflow. This
           ``Processor`` is then applied to the raw data retrieved from the
           ``Source`` in the previous step to obtain processed data. If an
           error occurs while processing the raw data, execution jumps to
           step 5.
        4. The callable returned by the
           :attr:`~sghi.etl.core.WorkflowDefinition.sink_factory` property of
           the supplied ``WorkflowDefinition`` is invoked to get the
           :class:`~sghi.etl.core.Sink` associated with the workflow. The
           processed data from the previous step is drained into this ``Sink``.
        5. The ``Source``, ``Processor`` and ``Sink`` created in the previous
           steps are disposed of. Note that this disposal also happens if an
           error occurs while executing any of the previous steps.
        6. The callable returned by the
           :attr:`~sghi.etl.core.WorkflowDefinition.epilogue` property is
           invoked last. This is always invoked regardless of whether all the
           steps in the workflow completed successfully or not.

    .. note::

        The above is a general description of how the workflow execution
        occurs. The actual implementation may vary slightly from this
        description.

    If an exception is raised during the workflow execution, all the workflow's
    components (source, processor, sink) are disposed of, the epilogue callable
    is invoked, and the error is propagated to the caller.

    :param wf: A ``WorkflowDefinition`` instance or a factory function that
        supplies the ``WorkflowDefinition`` instance to be executed. If a
        factory function is given, it is only invoked once. The given
        value *MUST EITHER* be a ``WorkflowDefinition`` instance or valid
        callable object.

    :return: None.

    :raise ValueError: If ``wf`` is NEITHER a ``WorkflowDefinition`` instance
        NOR a callable object.
    """
    ensure_predicate(
        test=callable(wf) or isinstance(wf, WorkflowDefinition),
        exc_factory=ValueError,
        message=(
            "'wf' MUST be a valid callable object or an "
            f"'{type_fqn(WorkflowDefinition)}' instance."
        ),
    )

    wd: WorkflowDefinition = wf() if callable(wf) else wf
    try:
        _LOGGER.info("[%s:%s] Setting up workflow ...", wd.id, wd.name)
        wd.prologue()
        _LOGGER.info("[%s:%s] Starting workflow execution ...", wd.id, wd.name)
        with (
            wd.source_factory() as source,
            wd.processor_factory() as processor,
            wd.sink_factory() as sink,
        ):
            sink.drain(processor.apply(source.draw()))
        _LOGGER.info(
            "[%s:%s] Workflow execution complete. Cleaning up ...",
            wd.id,
            wd.name,
        )
    finally:
        wd.epilogue()
        _LOGGER.info("[%s:%s] Done :)", wd.id, wd.name)
