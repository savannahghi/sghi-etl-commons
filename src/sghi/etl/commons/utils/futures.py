"""Utilities for working with ``concurrent.futures.Future``."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import Future
from typing import TypeVar

from sghi.utils import ensure_not_none, future_succeeded

# =============================================================================
# TYPES
# =============================================================================


_T = TypeVar("_T")

_ResultGatherer = Callable[[Iterable[Future[_T]]], Iterable[_T]]


# =============================================================================
# UTILITIES
# =============================================================================


def fail_fast(
    futures: Iterable[Future[_T]],
    exc_wrapper_factory: Callable[[str | None], BaseException] | None = None,
    exc_wrapper_message: str | None = None,
) -> Iterable[_T]:
    """Return the results of the futures or fail if any of the futures failed.

    :param futures:
    :param exc_wrapper_factory:
    :param exc_wrapper_message:

    :return:
    """
    ensure_not_none(futures, "'futures' MUST not be None.")

    for future in futures:
        try:
            yield future.result()
        except BaseException as exp:  # noqa: BLE001
            if exc_wrapper_factory:
                raise exc_wrapper_factory(exc_wrapper_message) from exp
            else:
                raise


def fail_fast_factory(
    exc_wrapper_factory: Callable[[str | None], BaseException] | None = None,
    exc_wrapper_message: str | None = None,
) -> _ResultGatherer[_T]:
    """Return a :func:`fail_fast` function that raises the specified exception.

    :param exc_wrapper_factory:
    :param exc_wrapper_message:

    :return:
    """
    def _do_fail_fast(futures: Iterable[Future[_T]]) -> Iterable[_T]:
        return fail_fast(
            futures,
            exc_wrapper_factory=exc_wrapper_factory,
            exc_wrapper_message=exc_wrapper_message,
        )

    return _do_fail_fast


def ignored_failed(futures: Iterable[Future[_T]]) -> Iterable[_T]:
    """Return all the results of the futures that succeeded.

    :param futures:

    :return:
    """
    ensure_not_none(futures, "'futures' MUST not be None.")

    for future in futures:
        if future_succeeded(future):
            yield future.result()
