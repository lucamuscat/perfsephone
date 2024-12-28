import logging
import threading
from contextlib import contextmanager
from itertools import chain
from types import FrameType
from typing import Any, Dict, Generator, List, Literal, Optional, Sequence, Union

import pyinstrument

from perfsephone import BeginDurationEvent, Category, EndDurationEvent, SerializableEvent
from perfsephone.perfetto_renderer import render

logger = logging.getLogger(__name__)


class _ThreadProfiler:
    def __init__(self) -> None:
        self.thread_local = threading.local()
        self.profilers: Dict[int, pyinstrument.Profiler] = {}

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
                assert hasattr(
                    self.thread_local, "profiler"
                ), "because a profiler must have been started"
                self.thread_local.profiler.stop()
                self.profilers[threading.get_ident()] = self.thread_local.profiler


class Profiler:
    @contextmanager
    def __call__(
        self,
        *,
        root_frame_name: str,
        is_async: bool,
        args: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
    ) -> Generator[List[SerializableEvent], None, None]:
        if args is None:
            args = {}

        result: List[SerializableEvent] = []
        start_event = BeginDurationEvent(name=root_frame_name, cat=Category("test"), args=args)

        thread_profiler = _ThreadProfiler()

        # We use `threading.settrace`, as opposed to `threading.setprofile`, as
        # `pyinstrument.Profiler().start()` calls `threading.setprofile` under the hood, overriding
        # our profiling function.
        #
        # `threading.settrace` & `threading.setprofile` provides a rather convoluted mechanism of
        # starting a pyinstrument profiler as soon as a thread starts executing its `run()` method,
        # & stopping said profiler once the `run()` method finishes.
        threading.settrace(thread_profiler)  # type: ignore

        result.append(start_event)
        profiler_async_mode = "enabled" if is_async else "disabled"
        with pyinstrument.Profiler(async_mode=profiler_async_mode) as profile:
            yield result
        end_event = EndDurationEvent()
        start_rendering_event = BeginDurationEvent(
            name="[pytest-perfetto] Dumping frames", cat=Category("pytest")
        )

        threading.settrace(None)  # type: ignore

        profiles_to_render = (
            profile
            for profile in chain([profile], thread_profiler.profilers.values())
            if profile.last_session
        )

        for index, profiler in enumerate(profiles_to_render, start=1):
            if profiler.is_running:
                logger.warning(
                    "There exists a run-away thread which has not been joined after the end of the"
                    " test.The thread's profiler will be discarded."
                )
            elif profiler.last_session:
                result += render(
                    session=profiler.last_session,
                    start_time=profiler.last_session.start_time,
                    tid=index,
                )

        end_rendering_event = EndDurationEvent()
        result += [end_event, start_rendering_event, end_rendering_event]
