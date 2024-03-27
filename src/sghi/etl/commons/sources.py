"""Common :class:`~sghi.etl.core.Source` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import ExitStack
from logging import Logger
from typing import TYPE_CHECKING, Generic, TypeVar

from attrs import define, field, validators
from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Source
from sghi.task import ConcurrentExecutor, Task, task
from sghi.utils import ensure_not_none, ensure_predicate, type_fqn

from .utils import fail_fast

if TYPE_CHECKING:
    from typing import Self

# =============================================================================
# TYPES
# =============================================================================


_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_T = TypeVar("_T")

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]

_SourceCallable = Callable[[], _RDT]


# =============================================================================
# DECORATORS
# =============================================================================


def source(f: Callable[[], _RDT]) -> Source[_RDT]:
    """Mark/Decorate a ``Callable`` as a :class:`Source`.

    :param f: The callable to be decorated. The callable *MUST* not have any
        required arguments but *MUST* return a value (the drawn data).

    :return: A ``Source`` instance.

    :raise ValueError: If the given value is ``None`` or not a ``Callable``.
    """
    ensure_not_none(f, message="The given callable MUST not be None.")
    ensure_predicate(callable(f), message="A callable object is required.")

    return _SourceOfCallable(callable=f)


# =============================================================================
# SOURCE IMPLEMENTATIONS
# =============================================================================


@define
class SourceSet(Source[Sequence[_RDT]], Generic[_RDT]):
    """:class:`Source` composed of other sources."""

    _sources: Sequence[Source[_RDT]] = field(
        alias="sources",
        converter=tuple,
        repr=False,
        validator=[
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(Source),
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
    _result_gatherer: _ResultGatherer[_RDT] = field(
        alias="result_gatherer",
        default=fail_fast,
        init=False,
        repr=False,
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[None, _RDT] = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

        # Prepare sources for execution by ensuring that they are all
        # disposed properly once this object is disposed.
        prepped_sources = (
            self._source_to_task(self._exit_stack.push(_source))
            for _source in self._sources
        )
        # Schedule the sources for concurrent execution later.
        self._executor = ConcurrentExecutor(
            *prepped_sources,
            executor=self._executor_factor(),
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Source, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> Sequence[_RDT]:
        self._logger.debug("Aggregating data from all available sources.")

        with self._executor as executor:
            futures = executor.execute(None)

        return tuple(self._result_gatherer(futures))

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _source_to_task(s: Source[_RDT]) -> Task[None, _RDT]:
        @task
        def do_draw(_: None) -> _RDT:
            with s as _s:
                return _s.draw()

        return do_draw


@define
class _SourceOfCallable(Source[_RDT], Generic[_RDT]):
    _callable: _SourceCallable[_RDT] = field(
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
        return super(Source, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> _RDT:
        self._logger.debug("Delegating to '%s'.", type_fqn(self._callable))
        return self._callable()

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")
