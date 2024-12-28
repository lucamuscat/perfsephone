"""
The perfsephone plugin aims to help developers profile their tests by ultimately producing a
'perfetto' trace file, which may be natively visualized using most Chromium-based browsers.
"""

import inspect
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import (
    Any,
    Dict,
    Final,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pytest
from _pytest.config import Notset

from perfsephone import (
    BeginDurationEvent,
    Category,
    EndDurationEvent,
    InstantEvent,
    SerializableEvent,
    Timestamp,
)
from perfsephone.profiler import Profiler

PERFETTO_ARG_NAME: Final[str] = "perfetto_path"
logger = logging.getLogger(__name__)


class PytestPerfettoPlugin:
    def __init__(self) -> None:
        self.events: List[SerializableEvent] = []
        self.profiler: Profiler = Profiler()

    @pytest.hookimpl(hookwrapper=True)
    def pytest_sessionstart(self) -> Generator[None, None, None]:
        # Called after the `Session` object has been created and before performing collection and
        # entering the run test loop.
        self.events.append(
            BeginDurationEvent(
                name="pytest session",
                cat=Category("pytest"),
            )
        )
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_sessionfinish(self, session: pytest.Session) -> Generator[None, None, None]:
        # Called after whole test run finished, right before returning the exit status to the system
        # https://docs.pytest.org/en/7.1.x/reference/reference.html#pytest.hookspec.pytest_sessionfinish
        self.events.append(EndDurationEvent())
        perfetto_path: Union[Path, Notset] = session.config.getoption("perfetto_path")
        if isinstance(perfetto_path, Path):
            with perfetto_path.open("w") as file:
                result = [asdict(event) for event in self.events]
                for event in result:
                    # Python's time.time() produces timestamps using a seconds as its granularity,
                    # whilst perfetto uses a miceosecond granularity.
                    event["ts"] /= 1e-6

                json.dump(result, file)
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_collection(self) -> Generator[None, None, None]:
        self.events.append(
            BeginDurationEvent(
                name="Start Collection",
                cat=Category("pytest"),
            )
        )
        yield

    def pytest_itemcollected(self, item: pytest.Item) -> None:
        self.events.append(InstantEvent(name=f"[Item Collected] {item.nodeid}"))

    @pytest.hookimpl(hookwrapper=True)
    def pytest_collection_finish(self) -> Generator[None, None, None]:
        self.events.append(EndDurationEvent())
        yield

    # ===== Test running (runtest) hooks =====
    # https://docs.pytest.org/en/7.1.x/reference/reference.html#test-running-runtest-hooks
    @staticmethod
    def create_args_from_location(
        location: Tuple[str, Optional[int], str],
    ) -> Dict[str, Union[str, Sequence[str]]]:
        (file_name, line_number, test_name) = location

        args: Dict[str, Union[str, Sequence[str]]] = {
            "file_name": file_name,
            "test_name": test_name,
        }

        if line_number is not None:
            args["line_number"] = str(line_number)

        return args

    def pytest_runtest_logstart(
        self, nodeid: str, location: Tuple[str, Optional[int], str]
    ) -> None:
        self.events.append(
            BeginDurationEvent(
                name=nodeid,
                args=PytestPerfettoPlugin.create_args_from_location(location),
                cat=Category("test"),
            )
        )

    def pytest_runtest_logfinish(self) -> None:
        self.events.append(EndDurationEvent())

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when is None or report.when == "call":
            return

        self.events.append(
            BeginDurationEvent(name=report.when, cat=Category("test"), ts=Timestamp(report.start))
        )
        self.events.append(EndDurationEvent(ts=Timestamp(report.stop)))

    @pytest.hookimpl(hookwrapper=True)
    def pytest_pyfunc_call(self, pyfuncitem: pytest.Function) -> Generator[None, None, None]:
        is_async = inspect.iscoroutinefunction(pyfuncitem.function)

        with self.profiler(root_frame_name="call", is_async=is_async) as events:
            yield

        self.events += events

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_makereport(self) -> Generator[None, None, None]:
        self.events.append(BeginDurationEvent(name="pytest make report", cat=Category("pytest")))
        yield
        self.events.append(EndDurationEvent())

    # ===== Reporting hooks =====
    @pytest.hookimpl(hookwrapper=True)
    def pytest_fixture_setup(
        self, fixturedef: pytest.FixtureDef[Any]
    ) -> Generator[None, None, None]:
        args = {
            "argnames": fixturedef.argnames,
            "baseid": fixturedef.baseid,
            # `fixturedef.params` are not guaranteed to serializable via json.dump(s), as a param
            # can be a sequence of objects of any type.
            "params": list(map(str, fixturedef.params)) if fixturedef.params else "",
            "scope": fixturedef.scope,
        }

        with self.profiler(root_frame_name=fixturedef.argname, args=args, is_async=True) as events:
            yield

        self.events += events


# ===== Initialization hooks =====
def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--perfetto",
        dest=PERFETTO_ARG_NAME,
        metavar=PERFETTO_ARG_NAME,
        action="store",
        type=Path,
        help="The file path for the trace file generated by the `pytest-perfetto` plugin.",
    )


def pytest_configure(config: pytest.Config) -> None:
    option: Union[Path, Notset] = config.getoption(PERFETTO_ARG_NAME)
    if isinstance(option, Path) and option.is_dir():
        raise ValueError("The provided path must not be a directory")
    config.pluginmanager.register(PytestPerfettoPlugin())
