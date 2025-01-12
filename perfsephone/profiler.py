import abc
import logging
import threading
from contextlib import contextmanager
from itertools import chain
from types import FrameType
from typing import Any, Dict, Generator, List, Literal, Optional, Sequence, Union

import pyinstrument
import pyinstrument.session

from perfsephone import Category
from perfsephone.perfetto_renderer import render
from perfsephone.trace_store import ChromeTraceEventFormatJSONStore, TraceStore

logger = logging.getLogger(__name__)


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

    def drain(self) -> Generator[pyinstrument.session.Session, None, None]:
        while self.profilers:
            yield self.profilers.pop(0)

    def register(self) -> None:
        # We use `threading.settrace`, as opposed to `threading.setprofile`, as
        # `pyinstrument.Profiler().start()` calls `threading.setprofile` under the hood, overriding
        # our profiling function.
        #
        # `threading.settrace` & `threading.setprofile` provides a rather convoluted mechanism of
        # starting a pyinstrument profiler as soon as a thread starts executing its `run()` method,
        # & stopping said profiler once the `run()` method finishes.
        threading.settrace(self.__call__)  # type: ignore

    def unregister(self) -> None:
        threading.settrace(None)  # type: ignore

    def __call__(
        self,
        frame: FrameType,
        event: Literal["call", "line", "return", "exception", "opcode"],
        _args: Any,
    ) -> Any:
        """This method should only be used with `threading.settrace()`."""
        frame.f_trace_lines = False

        is_frame_from_thread_run: bool = (
            frame.f_code.co_name == "run" and frame.f_code.co_filename == threading.__file__
        )

        # Detect when `Thread.run()` is called
        if event == "call" and is_frame_from_thread_run:
            # If this is the first time `Thread.run()` is being called on this thread, start the
            # profiler.
            if getattr(self.thread_local, "run_stack_depth", 0) == 0:
                profiler = pyinstrument.Profiler(async_mode="disabled")
                self.thread_local.profiler = profiler
                self.thread_local.run_stack_depth = 0
                profiler.start()
            # Keep track of the number of active calls of `Thread.run()`.
            self.thread_local.run_stack_depth += 1
            return self.__call__

        # Detect when `Threading.run()` returns.
        if event == "return" and is_frame_from_thread_run:
            self.thread_local.run_stack_depth -= 1
            # When there are no more active invocations of `Thread.run()`, this implies that the
            # target of the thread being profiled has finished executing.
            if self.thread_local.run_stack_depth == 0:
                assert hasattr(self.thread_local, "profiler"), (
                    "because a profiler must have been started"
                )
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

        profiler_async_mode = "enabled" if is_async else "disabled"
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
