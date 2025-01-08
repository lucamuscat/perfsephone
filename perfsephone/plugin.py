"""
The perfsephone plugin aims to help developers profile their tests by ultimately producing a
'perfetto' trace file, which may be natively visualized using most Chromium-based browsers.
"""

import inspect
import logging
import time
from pathlib import Path
from typing import (
    Any,
    Dict,
    Final,
    Generator,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pytest
from _pytest.config import Notset

from perfsephone import (
    Category,
    InstantScope,
    Timestamp,
)
from perfsephone.profiler import Profiler
from perfsephone.trace_store import ChromeTraceEventFormatJSONStore, TraceStore

PERFETTO_ARG_NAME: Final[str] = "perfetto_path"
logger = logging.getLogger(__name__)


class PytestPerfettoPlugin:
    def __init__(self, output_path: Path) -> None:
        self.events: TraceStore = ChromeTraceEventFormatJSONStore()
        self.profiler: Profiler = Profiler()
        self.output_path: Path = output_path

    @pytest.hookimpl(hookwrapper=True)
    def pytest_sessionstart(self) -> Generator[None, None, None]:
        # Called after the `Session` object has been created and before performing collection and
        # entering the run test loop.
        self.profiler.register_thread_profiler()
        self.events.add_begin_event(
            name="pytest session",
            category=Category("pytest"),
        )
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_sessionfinish(self, session: pytest.Session) -> Generator[None, None, None]:
        # Called after whole test run finished, right before returning the exit status to the system
        # https://docs.pytest.org/en/7.1.x/reference/reference.html#pytest.hookspec.pytest_sessionfinish
        self.profiler.unregister_thread_profiler(self.events)

        self.events.add_end_event()
        perfetto_path: Union[Path, Notset] = session.config.getoption("perfetto_path")
        if isinstance(perfetto_path, Path):
            self.events.dump(path=perfetto_path)
        yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_collection(self) -> Generator[None, None, None]:
        self.events.add_begin_event(
            name="Start Collection",
            category=Category("pytest"),
        )
        yield

    def pytest_itemcollected(self, item: pytest.Item) -> None:
        self.events.add_instant_event(name=f"[Item Collected] {item.nodeid}", scope=InstantScope.t)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_collection_finish(self) -> Generator[None, None, None]:
        self.events.add_end_event()
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
        self.events.add_begin_event(
            name=nodeid,
            args=PytestPerfettoPlugin.create_args_from_location(location),
            category=Category("test"),
        )

    def pytest_runtest_logfinish(self) -> None:
        self.events.add_end_event()

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when is None or report.when == "call":
            return

        self.events.add_begin_event(
            name=report.when,
            category=Category("test"),
            timestamp=Timestamp(report.start),
        )

        self.events.add_end_event(timestamp=Timestamp(report.stop))

    @pytest.hookimpl(hookwrapper=True)
    def pytest_pyfunc_call(self, pyfuncitem: pytest.Function) -> Generator[None, None, None]:
        is_async = inspect.iscoroutinefunction(pyfuncitem.function)

        with self.profiler(root_frame_name="call", is_async=is_async) as events:
            yield

        self.events.merge(events)

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_makereport(self) -> Generator[None, None, None]:
        self.events.add_begin_event(
            name="pytest make report",
            category=Category("pytest"),
        )
        yield
        self.events.add_end_event()

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

        self.events.merge(events)


# ===== Initialization hooks =====
def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--perfetto",
        dest=PERFETTO_ARG_NAME,
        metavar=PERFETTO_ARG_NAME,
        action="store",
        type=Path,
        const=Path(f"perfsephone-{int(time.time())}.json"),
        nargs="?",
        help="The file path for the trace file generated by the `pytest-perfetto` plugin.",
    )


def pytest_configure(config: pytest.Config) -> None:
    option: Union[Path, Notset] = config.getoption(PERFETTO_ARG_NAME)

    if isinstance(option, Notset):
        return

    if isinstance(option, Path) and option.is_dir():
        raise ValueError("The provided path must not be a directory")

    config.pluginmanager.register(PytestPerfettoPlugin(option))
