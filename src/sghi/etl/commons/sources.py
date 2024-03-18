"""Common :class:`~sghi.etl.core.Source` implementations."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from logging import Logger
from typing import TYPE_CHECKING, Generic, TypeVar

from attrs import define, field, validators
from typing_extensions import override

from sghi.disposable import not_disposed
from sghi.etl.core import Source
from sghi.utils import ensure_not_none, type_fqn

if TYPE_CHECKING:
    from typing import Self

# =============================================================================
# TYPES
# =============================================================================


_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_SourceCallable = Callable[[], _RDT]


# =============================================================================
# DECORATORS
# =============================================================================


def data_source(f: Callable[[], _RDT]) -> Source[_RDT]:
    """Mark a ``Callable`` as a :class:`Source`.

    :param f: The callable to be decorated. The callable *MUST* not have any
        required arguments but *MUST* return a value (the drawn data).
    :return: A ``DataSource`` instance.

    :raise ValueError: If ``f`` is ``None``.
    """
    ensure_not_none(f, "'f' MUST not be None.")

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
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:  # noqa: D105
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    @override
    def __enter__(self) -> Self:
        super(Source, self).__enter__()
        self._exit_stack.__enter__()
        return self

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> Sequence[_RDT]: ...

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._logger.debug("Disposal complete.")


@define
class _SourceOfCallable(Source[_RDT], Generic[_RDT]):
    _callable: _SourceCallable[_RDT] = field(
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
        return super(Source, self).__enter__()

    @property
    @override
    def is_disposed(self) -> bool:
        return self._is_disposed

    @not_disposed
    @override
    def draw(self) -> _RDT:
        self._logger.debug("Drawing from '%s'.", type_fqn(self._callable))
        return self._callable()

    @override
    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")
