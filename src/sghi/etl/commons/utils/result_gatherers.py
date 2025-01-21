"""Utilities for working with ``concurrent.futures.Future`` objects.

This module provides helper functions for collecting results and/or errors from
concurrent ``Future`` objects.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import Future
from typing import TypeVar

from sghi.utils import (
    ensure_callable,
    ensure_instance_of,
    ensure_predicate,
    future_succeeded,
)

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
    """Return the results from futures or error if any of the futures failed.

    This function returns an ``Iterable`` of the results or errors(if any of
    the futures failed) gathered from the provided
    :class:`~concurrent.futures.Future` objects. Note that, this method by
    itself doesn't raise any exceptions for valid input values, but rather,
    consuming the returned ``Iterable`` is what raises the exception. When
    iterating through the returned ``Iterable``, the exception will be raised
    on the first encounter of a non-successful result. Optionally, a custom
    exception type factory and message can be provided to be used when raising
    the exception.

    Essentially, this function maps each ``Future`` object to its result if it
    completed successfully, or to a ``raise exp`` statement if the callable
    wrapped by the ``Future`` raised an exception during execution. ``exp``
    will either be the exception returned by ``exc_wrapper_factory`` function
    when provided, or the original exception raised by the wrapped callable.

    :param futures: An ``Iterable`` of ``Future`` objects to gather results
        from.
    :param exc_wrapper_factory: An optional ``Exception`` factory function. If
        provided, the function will be used to create the actual exception
        instance that will be raised, with the original exception attached as
        the cause. That is, the original exception will be attached to the new
        exception as the ``__cause__`` attribute.
        When not provided, the original exception is raised as it is.
    :param exc_wrapper_message: An optional message to use with the raised
        exception. Note that this is only used if the ``exc_wrapper_factory``
        parameter is also provided and ignored otherwise.
        Defaults to ``None`` when not provided.

    :return: An ``Iterable`` of the results gathered from the provided futures.

    :raise TypeError: If ``futures`` is NOT an ``Iterable``.
    :raise ValueError: If ``exc_wrapper_factory`` is provided but is NOT a
        callable object.
    """
    ensure_instance_of(futures, Iterable, "'futures' MUST be an Iterable.")
    ensure_predicate(
        test=callable(exc_wrapper_factory) if exc_wrapper_factory else True,
        message="When not None, 'exc_wrapper_factory' MUST be a callable.",
    )

    # Wrap the actual gathering operation in a nested function. This way, this
    # entire function doesn't become a generator, and it can fail quickly when
    # supplied with invalid arguments. That is, the "ensure_*" checks raise
    # exceptions immediately on the invocation of this function if the checks
    # fail instead of waiting until ``next`` is called on the returned
    # generator.
    def _do_gather() -> Iterable[_T]:
        for future in futures:
            try:
                yield future.result()
            except BaseException as exp:
                if exc_wrapper_factory:
                    raise exc_wrapper_factory(exc_wrapper_message) from exp
                else:
                    raise

    return _do_gather()


def fail_fast_factory(
    exc_wrapper_factory: Callable[[str | None], BaseException],
    exc_wrapper_message: str | None = None,
) -> _ResultGatherer[_T]:
    """Return a :func:`fail_fast` function that raises the specified exception.

    Create and return a pre-configured :func:`fail_fast` function that raises
    the specified exception and message.

    :param exc_wrapper_factory: An ``Exception`` factory function that takes
        an optional error message as input and returns an exception object to
        be raised. The original exception will be attached to the created
        exception as the ``__cause__`` attribute. This MUST be a valid callable
        object.
    :param exc_wrapper_message: An optional error message to use with the
        raised exception. Defaults to ``None`` when not provided.

    :return: A function that behaves as ``fail_fast`` but uses the specified
        configuration.

    :raise ValueError: If ``exc_wrapper_factory`` is NOT a callable object.
    """
    ensure_callable(
        value=exc_wrapper_factory,
        message="'exc_wrapper_factory' MUST be a callable.",
    )

    def _do_fail_fast(futures: Iterable[Future[_T]]) -> Iterable[_T]:
        return fail_fast(
            futures,
            exc_wrapper_factory=exc_wrapper_factory,
            exc_wrapper_message=exc_wrapper_message,
        )

    return _do_fail_fast


def ignored_failed(futures: Iterable[Future[_T]]) -> Iterable[_T]:
    """Return results only from successful futures.

    Gather and return results from the successful futures of the provided set.
    In this context, a ``Future`` is considered to have completed successfully
    if it wasn't canceled and no uncaught exceptions were raised by its callee.

    :param futures: An ``Iterable`` of ``Future`` objects to gather results
        from.

    :return: An ``Iterable`` of the results gathered from only the successful
        futures of the provided set.

    :raise TypeError: If ``futures`` is NOT an ``Iterable``.
    """
    ensure_instance_of(futures, Iterable, "'futures' MUST be an Iterable.")

    # Wrap the actual gathering operation in a nested function. This way, this
    # entire function doesn't become a generator, and it can fail quickly when
    # supplied with invalid arguments. That is, the "ensure_*" checks raise
    # exceptions immediately on the invocation of this function if the checks
    # fail instead of waiting until ``next`` is called on the returned
    # generator.
    def _do_gather() -> Iterable[_T]:
        yield from (ftr.result() for ftr in filter(future_succeeded, futures))

    return _do_gather()
