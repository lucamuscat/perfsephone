import abc
import threading
from contextlib import contextmanager
from itertools import chain
from typing import Any, Callable, Dict, Generator, List, Literal, Optional, Sequence, Union

import pyinstrument
import pyinstrument.session

from perfsephone import Category
from perfsephone.perfetto_renderer import render
from perfsephone.trace_store import ChromeTraceEventFormatJSONStore, TraceStore


class Profiler(abc.ABC):
    @contextmanager
    @abc.abstractmethod
    def __call__(
        self,
        *,
        root_frame_name: str,
        is_async: bool,
        args: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
    ) -> Generator[TraceStore, None, None]:
        """
        Args:
            * root_frame_name: str - The name of the first frame to be emitted into the TraceStore
            * is_async: bool - Whether the target being profiled is an async function.
            * args: Optional[Dict[str, Union[str, Sequence[str]]]] = None - Miscellaneous arguments
            that become embedded with the root frame (see the `args` parameter in
            `BeginDurationEvent`)
        """
        yield ChromeTraceEventFormatJSONStore()
        raise NotImplementedError

    def register_thread_profiler(self) -> None:  # noqa: B027
        pass

    def unregister_thread_profiler(self, trace_store: TraceStore) -> None:  # noqa: B027
        pass


class OutlineProfiler(Profiler):
    @contextmanager
    def __call__(
        self,
        *,
        root_frame_name: str,
        is_async: bool,  # noqa: ARG002
        args: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
    ) -> Generator[TraceStore, None, None]:
        # TODO: Create a TraceStore factory that gets passed around in the init variables of each of
        # these profiler classes
        result: TraceStore = ChromeTraceEventFormatJSONStore()
        result.add_begin_event(
            name=root_frame_name, category=Category("test"), tid=1, pid=1, args=args
        )
        yield result
        result.add_end_event()


class _ThreadProfiler:
    def __init__(self) -> None:
        self.thread_local = threading.local()
        self.profilers: List[pyinstrument.session.Session] = []
        self.original_thread_class = threading.Thread

    def drain(self) -> Generator[pyinstrument.session.Session, None, None]:
        while self.profilers:
            yield self.profilers.pop(0)

    def register(self) -> None:
        profiler = self

        class ProfiledThread(threading.Thread):
            def run(self) -> None:
                profiler(super().run)

        threading.Thread = ProfiledThread  # type: ignore

    def unregister(self) -> None:
        threading.Thread = self.original_thread_class  # type: ignore

    def __call__(self, runnable: Callable[[], None]) -> Any:
        self.thread_local.profiler = pyinstrument.Profiler()
        self.thread_local.profiler.start()

        runnable()

        self.profilers.append(self.thread_local.profiler.stop())


class PyinstrumentProfiler(Profiler):
    def __init__(self) -> None:
        self.thread_profiler = _ThreadProfiler()
        self.max_tid: int = 0

    def register_thread_profiler(self) -> None:
        self.thread_profiler.register()

    def unregister_thread_profiler(self, trace_store: TraceStore) -> None:
        """Stop profiling non-main threads. The profile of each thread which has not yet been
        rendered will be merged in the provided trace store"""
        self.thread_profiler.unregister()

        for index, profiler_session in enumerate(
            self.thread_profiler.drain(), start=self.max_tid + 1
        ):
            trace_store.merge(
                render(
                    session=profiler_session,
                    tid=index,
                )
            )

    @contextmanager
    def __call__(
        self,
        *,
        root_frame_name: str,
        is_async: bool,
        args: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
    ) -> Generator[TraceStore, None, None]:
        if args is None:
            args = {}

        profiler_async_mode: Literal["enabled", "disabled"] = "enabled" if is_async else "disabled"
        with OutlineProfiler()(
            root_frame_name=root_frame_name, is_async=is_async, args=args
        ) as result, pyinstrument.Profiler(async_mode=profiler_async_mode) as profile:
            yield result

        result.add_begin_event(
            name="[pytest-perfetto] Dumping frames",
            category=Category("pytest"),
        )

        profiles_to_render = (
            profile
            for profile in chain([profile.last_session], self.thread_profiler.drain())
            if profile is not None
        )

        for index, session in enumerate(profiles_to_render, start=1):
            self.max_tid = max(index, self.max_tid)

            result.merge(
                render(
                    session=session,
                    tid=index,
                )
            )

        result.add_end_event()
