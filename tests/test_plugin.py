import json
from pathlib import Path
from typing import Final, Tuple

import pytest

from perfsephone.plugin import TRACE_LEVEL_ARG_NAME


def test_given_perfetto_arg_trace_files_are_written(
    pytester: pytest.Pytester, temp_perfetto_file_path: Path
) -> None:
    pytester.makepyfile("""
        def test_hello(): ...
    """)
    result = pytester.runpytest_inprocess(f"--perfetto={temp_perfetto_file_path}")
    result.assert_outcomes(passed=1)
    assert temp_perfetto_file_path.exists()


def test_given_perfetto_arg_with_no_value__then_trace_files_are_written_in_current_dir(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile("""
        def test_hello(): ...
    """)
    pytester.runpytest_inprocess("--perfetto").assert_outcomes(passed=1)
    assert len(list(Path().glob("perfsephone-*.json"))) == 1


@pytest.mark.parametrize("args", [("--perfetto",), ("--perfetto", "--outline"), ("--outline",)])
def test_given_perfsephone_args__then_no_error(
    pytester: pytest.Pytester, args: Tuple[str, ...]
) -> None:
    pytester.makepyfile("""
        def test_hello(): ...
    """)
    pytester.runpytest_inprocess(*args).assert_outcomes(passed=1)


def test_when_perfsephone_level_not_provided__then_default_value_is_full(
    pytester: pytest.Pytester,
) -> None:
    config = pytester.parseconfig("--perfetto")
    expected_value = False
    actual_value = config.getoption(TRACE_LEVEL_ARG_NAME)
    assert expected_value == actual_value


def test_given_non_serializable_params__when_dump_trace__then_file_is_written(
    pytester: pytest.Pytester, temp_perfetto_file_path: Path
) -> None:
    pytester.makepyfile("""
        import pytest
        from uuid import uuid4

        class NotJsonSerializable:
            def __init__(self) -> None:
                self.hello = "world"

        @pytest.fixture(params=[NotJsonSerializable(), uuid4()])
        def some_fixture(request) -> None:
            ...

        @pytest.mark.parametrize("some_fixture", [uuid4()], indirect=True)
        def test_hello(some_fixture) -> None:
            ...
    """)
    result = pytester.runpytest_inprocess(f"--perfetto={temp_perfetto_file_path}")
    result.assert_outcomes(passed=1)
    assert temp_perfetto_file_path.exists()


def test_given_multiple_threads__then_multiple_distinct_tids_are_reported(
    pytester: pytest.Pytester, temp_perfetto_file_path: Path
) -> None:
    pytester.makepyfile("""
        import threading
        import time

        SLEEP_TIME_S = 0.002

        def test_hello() -> None:
            def foo() -> None:
                def bar() -> None:
                    time.sleep(SLEEP_TIME_S)
                thread = threading.Thread(target=bar)
                thread.start()
                thread.join()

            thread = threading.Thread(target=foo)
            thread.start()
            thread.join()
    """)
    pytester.runpytest_inprocess(f"--perfetto={temp_perfetto_file_path}").assert_outcomes(passed=1)
    trace_file = json.load(temp_perfetto_file_path.open("r"))
    EXPECTED_DISTINCT_TID_COUNT: Final[int] = 3

    assert (
        len(
            {
                event["tid"]
                for event in trace_file
                if event.get("name", "") in ["foo", "bar", "test_hello"]
            }
        )
        == EXPECTED_DISTINCT_TID_COUNT
    )


def test_given_thread_created_in_setup__then_thread_is_recorded_and_not_ignored(
    pytester: pytest.Pytester, temp_perfetto_file_path: Path
) -> None:
    pytester.makepyfile("""
        from pytest import fixture

        from typing import Generator
        from concurrent.futures import ThreadPoolExecutor
        from time import sleep

        def foo() -> None:
            sleep(0.005)


        def bar() -> None:
            sleep(0.005)


        def quix() -> None:
            sleep(0.005)


        @fixture(scope="session")
        def pool() -> Generator[ThreadPoolExecutor, None, None]:
            with ThreadPoolExecutor() as pool:
                yield pool


        def test_hello(pool: ThreadPoolExecutor) -> None:
            pool.submit(foo).result()


        def test_hello2(pool: ThreadPoolExecutor) -> None:
            pool.submit(bar).result()


        def test_hello3(pool: ThreadPoolExecutor) -> None:
            pool.submit(quix).result()
    """)
    pytester.runpytest_inprocess(f"--perfetto={temp_perfetto_file_path}").assert_outcomes(passed=3)
    trace_file = json.load(temp_perfetto_file_path.open("r"))

    EXPECTED_EVENT_COUNT: Final[int] = 3
    EXPECTED_THREAD_POOL_TID: Final[int] = 2

    events = list(filter(lambda event: event.get("name") in ["foo", "bar", "quix"], trace_file))

    assert len({event["name"] for event in events}) == EXPECTED_EVENT_COUNT, (
        "because three functions were executed in the thread pool thread"
    )

    assert all(event["tid"] == EXPECTED_THREAD_POOL_TID for event in events), (
        "because the thread pool was expected to use the same thread for all submissions"
    )


# TODO: Test that the outline actually includes the setup, call, and teardown events.
def test_given_outline_mode__then_test_is_not_profiled(
    pytester: pytest.Pytester, temp_perfetto_file_path: Path
) -> None:
    pytester.makepyfile("""
    from time import sleep

    def test_hello():
        def foobar():
            sleep(0.003)
        foobar()
    """)
    pytester.runpytest_inprocess(
        f"--perfetto={temp_perfetto_file_path}",
        "--outline",
    ).assert_outcomes(passed=1)

    actual_events = json.load(temp_perfetto_file_path.open("r"))
    assert len([event for event in actual_events if event.get("name", "") == "foobar"]) == 0
