"""Common :class:`~sghi.etl.core.Sink` implementations."""

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
from sghi.etl.core import Sink
from sghi.task import ConcurrentExecutor, Task, task
from sghi.utils import ensure_not_none, ensure_predicate, type_fqn

from .utils import fail_fast

if TYPE_CHECKING:
    from typing import Self

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_T = TypeVar("_T")

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]

_SinkCallable = Callable[[_PDT], None]


# =============================================================================
# DECORATORS
# =============================================================================


def sink(f: Callable[[_PDT], None]) -> Sink[_PDT]:
    """Mark/Decorate a ``Callable`` as a :class:`Sink`.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*
        one required argument (the processed data to be consumed).

    :return: A ``Sink`` instance.

    :raise ValueError: If the given value is ``None`` or not a ``Callable``.
    """
    ensure_not_none(f, message="The given callable MUST not be None.")
    ensure_predicate(callable(f), message="A callable object is required.")

    return _SinkOfCallable(callable=f)


# =============================================================================
# SINK IMPLEMENTATIONS
# =============================================================================


@define
class NullSink(Sink[_PDT], Generic[_PDT]):
    """:class:`Sink` that discards all the data it receives."""

    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def drain(self, processed_data: _PDT) -> None:
        self._logger.debug("Discarding all received data.")
        # Do nothing, discard all received data.
        ...

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")


@define
class ScatterSink(Sink[_PDT], Generic[_PDT]):
    """:class:`Sink` that 'fans-out' its input to other sinks."""

    _sinks: Sequence[Sink[_PDT]] = field(
        alias="sinks",
        converter=tuple,
        repr=False,
        validator=[
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(Sink),
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
    _result_gatherer: _ResultGatherer[None] = field(
        alias="result_gatherer",
        default=fail_fast,
        init=False,
        repr=False,
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[_PDT, None] = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

        # Prepare nested sinks for execution by ensuring that they are all
        # disposed of properly once this object is disposed.
        prepped_sinks = (
            self._sink_to_task(self._exit_stack.push(_sink))
            for _sink in self._sinks
        )
        # Schedule the sinks for concurrent execution later.
        self._executor = ConcurrentExecutor(
            *prepped_sinks,
            executor=self._executor_factor(),
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @override
    def drain(self, processed_data: _PDT) -> None:
        self._logger.debug("Scattering received data to all available sinks.")

        with self._executor as executor:
            futures = executor.execute(processed_data)

        self._result_gatherer(futures)

    @override
    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _sink_to_task(s: Sink[_PDT]) -> Task[_PDT, None]:
        @task
        def do_drain(processed_data: _PDT) -> None:
            with s as _s:
                return _s.drain(processed_data)

        return do_drain


@define
class SinkSet(Sink[Sequence[_PDT]], Generic[_PDT]):
    """:class:`Sink` composed of other sinks."""

    _sinks: Sequence[Sink[_PDT]] = field(
        alias="sinks",
        converter=tuple,
        repr=False,
        validator=[
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(Sink),
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
    _result_gatherer: _ResultGatherer[None] = field(
        alias="result_gatherer",
        default=fail_fast,
        init=False,
        repr=False,
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[Sequence[_PDT], None] = field(
        init=False,
        repr=False,
    )
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

        # Prepare nested sinks for execution by ensuring that they are all
        # disposed of properly once this object is disposed.
        prepped_sinks = (
            self._sink_to_task(self._exit_stack.push(_sink), _i)
            for _i, _sink in enumerate(self._sinks)
        )
        # Schedule the sinks for concurrent execution later.
        self._executor = ConcurrentExecutor(
            *prepped_sinks,
            executor=self._executor_factor(),
        )

    @not_disposed
    @override
    def __enter__(self) -> Self:
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @override
    def drain(self, processed_data: Sequence[_PDT]) -> None:
        self._logger.debug("Distributing consumption to all available sinks.")

        assert len(processed_data) == len(self._sinks)

        with self._executor as executor:
            futures = executor.execute(processed_data)

        self._result_gatherer(futures)

    @override
    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    @staticmethod
    def _sink_to_task(s: Sink[_PDT], i: int) -> Task[Sequence[_PDT], None]:
        @task
        def do_drain(processed_data: Sequence[_PDT]) -> None:
            with s as _s:
                return _s.drain(processed_data[i])

        return do_drain


@define
class _SinkOfCallable(Sink[_PDT], Generic[_PDT]):
    _callable: _SinkCallable[_PDT] = field(
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
        return super(Sink, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def drain(self, processed_data: _PDT) -> None:
        self._logger.debug("Delegating to '%s'.", type_fqn(self._callable))
        self._callable(processed_data)

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")
