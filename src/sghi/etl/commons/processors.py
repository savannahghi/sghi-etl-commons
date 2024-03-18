"""Common :class:`~sghi.etl.core.Processor` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from logging import Logger
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from attrs import define, field, validators
from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Processor
from sghi.task import Task, pipe
from sghi.utils import ensure_not_none, type_fqn

if TYPE_CHECKING:
    from typing import Self

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_ProcessorCallable = Callable[[_RDT], _PDT]


# =============================================================================
# DECORATORS
# =============================================================================


def processor(f: Callable[[_RDT], _PDT]) -> Processor[_RDT, _PDT]:
    """Mark a ``Callable`` as a :class:`Processor`.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*
        one required argument (the raw data to be processed).

    :return: A ``Processor`` instance.

    :raise ValueError: If ``f`` is ``None``.
    """
    ensure_not_none(f, "'f' MUST not be None.")

    return _ProcessorOfCallable(callable=f)


# =============================================================================
# PROCESSOR IMPLEMENTATIONS
# =============================================================================


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

        # Prepare processors for execution by ensuring that they are all
        # disposed properly once this object is disposed.
        self._prepped_processors = [
            self._processor_to_task(self._exit_stack.push(_processor))
            for _processor in self._processors
        ]

    @not_disposed
    @override
    def __enter__(self) -> Self:
        super(Processor, self).__enter__()
        self._exit_stack.__enter__()
        return self

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def process(self, raw_data: _RDT) -> _PDT:
        self._logger.info("Processing %s.", str(raw_data))

        return pipe(*self._prepped_processors)(raw_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _processor_to_task(p: Processor[_RDT, _PDT]) -> Task[_RDT, _PDT]:
        def do_process(raw_data: _RDT) -> _PDT:
            with p as _p:
                return _p.process(raw_data)

        return Task.of_callable(do_process)


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
class _ProcessorOfCallable(Processor[_RDT, _PDT], Generic[_RDT, _PDT]):
    _callable: _ProcessorCallable[_RDT, _PDT] = field(
        alias="callable",
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
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
    def process(self, raw_data: _RDT) -> _PDT:
        self._logger.debug(
            "Invoking '%s' to process %s.",
            type_fqn(self._callable),
            str(raw_data),
        )
        return self._callable(raw_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")
