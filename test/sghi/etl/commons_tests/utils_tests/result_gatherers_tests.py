"""Tests for the ``sghi.etl.commons.utils.result_gatherers`` module."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import Future

import pytest

from sghi.etl.commons.utils import fail_fast, fail_fast_factory, ignored_failed
from sghi.exceptions import SGHIError

# =============================================================================
# HELPERS
# =============================================================================


def _create_successful_features(count: int = 5) -> Iterable[Future[int]]:
    def _num_to_future(num: int) -> Future[int]:
        f = Future()
        f.set_result(num)
        return f

    yield from map(_num_to_future, range(count))


def _create_futures_with_one_failed(
    count: int = 5,
    failed_index: int = 3,
    exc_factory: Callable[[], BaseException] = SGHIError,
) -> Iterable[Future[int]]:
    def _num_to_future(num: int) -> Future[int]:
        f = Future()
        if num == failed_index:
            f.set_exception(exc_factory())
        else:
            f.set_result(num)
        return f

    yield from map(_num_to_future, range(count))


# =============================================================================
# TEST CASES
# =============================================================================


def test_fail_fast_fails_when_given_invalid_args() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast` should fail when given invalid
    args.
    """  # noqa: D205
    with pytest.raises(TypeError, match="MUST be an Iterable") as exp_info1:
        fail_fast(None)  # type: ignore

    with pytest.raises(ValueError, match="MUST be a callable") as exp_info2:
        fail_fast(_create_successful_features(), exc_wrapper_factory="oops")  # type: ignore

    assert exp_info1.value.args[0] == "'futures' MUST be an Iterable."
    assert (
        exp_info2.value.args[0]
        == "When not None, 'exc_wrapper_factory' MUST be a callable."
    )


def test_fail_fast_returns_iterable_even_with_failed_futures_present() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast` should always return an
    ``Iterable`` and not raise any exception for valid input args.

    This should hold even if the any of the provided futures failed.
    """  # noqa: D205
    # noinspection PyBroadException
    try:
        results = fail_fast(
            futures=_create_futures_with_one_failed(5, failed_index=2),
        )
    except BaseException:  # noqa: BLE001
        fail_reason: str = (
            "'fail_fast' should not raise any exception when given valid "
            "inputs."
        )
        pytest.fail(fail_reason)

    assert isinstance(results, Iterable)


def test_fail_fast_return_value_when_futures_are_successful() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast` should return an ``Iterable``
    of the gathered results if no future failed..
    """  # noqa: D205
    results = fail_fast(_create_successful_features(count=5))

    assert isinstance(results, Iterable)
    assert tuple(results) == (0, 1, 2, 3, 4)


def test_fail_fast_return_value_with_failed_futures_no_exc_wrapper() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast` should return an ``Iterable``
    of the gathered results and errors if any future failed.

    Consuming the returned ``Iterable`` should raise an error at the first
    encounter of the non-successful result.
    """  # noqa: D205
    results = fail_fast(_create_futures_with_one_failed(5, failed_index=2))

    assert isinstance(results, Iterable)

    results_iterator = iter(results)
    assert next(results_iterator) == 0
    assert next(results_iterator) == 1

    with pytest.raises(SGHIError):
        next(results_iterator)  # The third future(at index 2) failed.


def test_fail_fast_return_value_with_failed_futures_and_exc_wrapper() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast` should return an ``Iterable``
    of the gathered results and errors if any future failed.

    Consuming the returned ``Iterable`` should raise an error at the first
    encounter of the non-successful result. When ``exc_wrapper_factory`` is
    provided, the raised error should be that returned by the factory with its
    ``__cause__`` attribute set to the original error.
    """  # noqa: D205
    results = fail_fast(
        futures=_create_futures_with_one_failed(5, failed_index=3),
        exc_wrapper_factory=RuntimeError,
    )

    assert isinstance(results, Iterable)

    results_iterator = iter(results)
    assert next(results_iterator) == 0
    assert next(results_iterator) == 1
    assert next(results_iterator) == 2

    with pytest.raises(RuntimeError) as exp_info:
        next(results_iterator)

    assert isinstance(exp_info.value.__cause__, SGHIError)


def test_fail_fast_factory_fails_when_given_invalid_args() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast_factory` should raise a
    :exc:`ValueError` when ``exc_wrapper_factory`` is not a callable object.
    """  # noqa: D205
    with pytest.raises(ValueError, match="MUST be a callable") as exp_info:
        fail_fast_factory(exc_wrapper_factory="not a callable")  # type: ignore

    assert (
        exp_info.value.args[0] == "'exc_wrapper_factory' MUST be a callable."
    )


def test_fail_fast_factory_returns_expected_value() -> None:
    """:func:`sghi.etl.commons.utils.fail_fast_factory` should return a
    gatherer function with the same semantics as
    :func:`sghi.etl.commons.utils.fail_fast`.

    Consuming the returned ``Iterable`` should raise an error at the first
    encounter of the non-successful result. The raised error should be that
    returned by the provided ``exc_wrapper_factory`` factory with its
    ``__cause__`` attribute set to the original error.
    """  # noqa: D205
    results1 = fail_fast_factory(
        exc_wrapper_factory=RuntimeError,
    )(_create_successful_features(5))

    results2 = fail_fast_factory(
        exc_wrapper_factory=RuntimeError, exc_wrapper_message="oops"
    )(_create_futures_with_one_failed(5, failed_index=2))

    assert isinstance(results1, Iterable)
    assert isinstance(results2, Iterable)

    assert tuple(results1) == (0, 1, 2, 3, 4)

    results_iterator = iter(results2)
    assert next(results_iterator) == 0
    assert next(results_iterator) == 1

    with pytest.raises(RuntimeError, match="oops") as exp_info:
        next(results_iterator)  # The third future(at index 2) failed.

    assert isinstance(exp_info.value.__cause__, SGHIError)


def test_ignore_failed_fails_when_given_invalid_args() -> None:
    """:func:`sghi.etl.commons.utils.ignore_failed` should raise a
    :exc:`TypeError` when ``futures`` is not an ``Iterable`` object.
    """  # noqa: D205
    with pytest.raises(TypeError, match="MUST be an Iterable") as exp_info:
        ignored_failed(None)  # type: ignore

    assert exp_info.value.args[0] == "'futures' MUST be an Iterable."


def test_ignore_failed_returns_expected_value() -> None:
    """:func:`sghi.etl.commons.utils.ignore_failed` should return an
    ``Iterable`` of the gathered results from successful futures of the
    provided set.
    """  # noqa: D205
    results1 = ignored_failed(_create_successful_features(5))
    results2 = ignored_failed(
        _create_futures_with_one_failed(5, failed_index=2)
    )

    assert isinstance(results1, Iterable)
    assert isinstance(results2, Iterable)

    assert tuple(results1) == (0, 1, 2, 3, 4)
    assert tuple(results2) == (0, 1, 3, 4)
