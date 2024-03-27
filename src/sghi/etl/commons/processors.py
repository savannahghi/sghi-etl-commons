"""Common :class:`~sghi.etl.core.Processor` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import ExitStack
from logging import Logger
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from attrs import define, field, validators
from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Processor
from sghi.task import ConcurrentExecutor, Task, pipe, task
from sghi.utils import ensure_not_none, ensure_predicate, type_fqn

from .utils import fail_fast

if TYPE_CHECKING:
    from typing import Self

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_T = TypeVar("_T")

_ProcessorCallable = Callable[[_RDT], _PDT]

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]

# =============================================================================
# DECORATORS
# =============================================================================


def processor(f: Callable[[_RDT], _PDT]) -> Processor[_RDT, _PDT]:
    """Mark a ``Callable`` as a :class:`Processor`.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*
        one required argument (the raw data to be processed).

    :return: A ``Processor`` instance.

    :raise ValueError: If the given value is ``None`` or not a ``Callable``.
    """
    ensure_not_none(f, message="The given callable MUST not be None.")
    ensure_predicate(callable(f), message="A callable object is required.")

    return _ProcessorOfCallable(callable=f)


# =============================================================================
# PROCESSOR IMPLEMENTATIONS
# =============================================================================


@define
class NoOpProcessor(Processor[_RDT, _RDT], Generic[_RDT]):
    """:class:`Processor` that returns received data as is."""

    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def process(self, raw_data: _RDT) -> _RDT:
        self._logger.debug("Skipping data processing. Return raw data as is.")
        return raw_data

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")


@define
class ProcessorPipe(Processor[_RDT, _PDT], Generic[_RDT, _PDT]):
    """:class:`Processor` that pipes multiple ``Processors`` together."""

    _processors: Sequence[Processor[Any, Any]] = field(
        alias="processors",
        converter=tuple,
        repr=False,
        validator=[
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(Processor),
            ),
            validators.min_len(1),
        ],
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)
    _prepped_processors: Sequence[Task[Any, Any]] = field(
        init=False,
        repr=False,
    )

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

        # Prepare nested processors for execution by ensuring that they are
        # all disposed of properly once this object is disposed.
        self._prepped_processors = [
            self._processor_to_task(self._exit_stack.push(_processor))
            for _processor in self._processors
        ]

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def process(self, raw_data: _RDT) -> _PDT:
        self._logger.debug("Piping received data through all processors.")

        return pipe(*self._prepped_processors)(raw_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _processor_to_task(p: Processor[_RDT, _PDT]) -> Task[_RDT, _PDT]:
        @task
        def do_process(raw_data: _RDT) -> _PDT:
            with p as _p:
                return _p.process(raw_data)

        return do_process


@define
class ProcessorSet(
    Processor[Sequence[_RDT], Sequence[_PDT]],
    Generic[_RDT, _PDT],
):
    """:class:`Processor` composed of other processors."""

    _processors: Sequence[Processor[_RDT, _PDT]] = field(
        alias="processor",
        converter=tuple,
        repr=False,
        validator=[
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(Processor),
            ),
            validators.min_len(1),
        ],
    )
    _executor_factor: Callable[[], Executor] = field(
        alias="executor_factory",
        default=ThreadPoolExecutor,
        repr=False,
        validator=validators.is_callable(),
    )
    _result_gatherer: _ResultGatherer[_PDT] = field(
        alias="result_gatherer",
        default=fail_fast,
        init=False,
        repr=False,
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[Sequence[_RDT], _PDT] = field(
        init=False,
        repr=False,
    )
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

        # Prepare nested processors for execution by ensuring that they are
        # all disposed of properly once this object is disposed.
        prepped_processors = (
            self._processor_to_task(self._exit_stack.push(_processor), _i)
            for _i, _processor in enumerate(self._processors)
        )
        # Schedule the processors for concurrent execution later.
        self._executor = ConcurrentExecutor(
            *prepped_processors,
            executor=self._executor_factor(),
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @override
    def process(self, raw_data: Sequence[_RDT]) -> Sequence[_PDT]:
        self._logger.debug(
            "Distributing processing to all available processors.",
        )

        assert len(raw_data) == len(self._processors)
        with self._executor as executor:
            futures = executor.execute(raw_data)

        return tuple(self._result_gatherer(futures))

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _processor_to_task(
        p: Processor[_RDT, _PDT],
        i: int,
    ) -> Task[Sequence[_RDT], _PDT]:
        @task
        def do_process(raw_data: Sequence[_RDT]) -> _PDT:
            with p as _p:
                return _p.process(raw_data[i])

        return do_process


@define
class ScatterProcessor(Processor[_RDT, Sequence[_PDT]], Generic[_RDT, _PDT]):
    """:class:`Processor` that 'fans-out' its input to other processors."""

    _processors: Sequence[Processor[_RDT, _PDT]] = field(
        alias="processors",
        converter=tuple,
        repr=False,
        validator=[
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(Processor),
            ),
            validators.min_len(1),
        ],
    )
    _executor_factor: Callable[[], Executor] = field(
        alias="executor_factory",
        default=ThreadPoolExecutor,
        repr=False,
        validator=validators.is_callable(),
    )
    _result_gatherer: _ResultGatherer[_PDT] = field(
        alias="result_gatherer",
        default=fail_fast,
        init=False,
        repr=False,
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[_RDT, _PDT] = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

        # Prepare nested processors for execution by ensuring that they are
        # all disposed of properly once this object is disposed.
        prepped_processors = (
            self._processor_to_task(self._exit_stack.push(_processor))
            for _processor in self._processors
        )
        # Schedule the processors for concurrent execution later.
        self._executor = ConcurrentExecutor(
            *prepped_processors,
            executor=self._executor_factor(),
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def process(self, raw_data: _RDT) -> Sequence[_PDT]:
        self._logger.debug(
            "Scattering received data to all available processors."
        )

        with self._executor as executor:
            futures = executor.execute(raw_data)

        return tuple(self._result_gatherer(futures))

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _processor_to_task(p: Processor[_RDT, _PDT]) -> Task[_RDT, _PDT]:
        @task
        def do_process(raw_data: _RDT) -> _PDT:
            with p as _p:
                return _p.process(raw_data)

        return do_process


@define
class _ProcessorOfCallable(Processor[_RDT, _PDT], Generic[_RDT, _PDT]):
    _callable: _ProcessorCallable[_RDT, _PDT] = field(
        alias="callable",
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(
            name=f"{type_fqn(self.__class__)}({type_fqn(self._callable)})",
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Processor, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def process(self, raw_data: _RDT) -> _PDT:
        self._logger.debug("Delegating to '%s'.", type_fqn(self._callable))
        return self._callable(raw_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")
