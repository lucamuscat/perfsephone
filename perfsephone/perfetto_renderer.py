from pathlib import Path
from typing import Dict, List, Optional, Sequence

import _pytest
import pluggy
import pytest
from pyinstrument.frame import SYNTHETIC_LEAF_IDENTIFIERS, Frame
from pyinstrument.renderers.speedscope import (
    SpeedscopeEvent,
    SpeedscopeEventType,
    SpeedscopeFrame,
    SpeedscopeRenderer,
)
from pyinstrument.session import Session

from perfsephone import (
    Category,
    Timestamp,
)
from perfsephone.trace_store import ChromeTraceEventFormatJSONStore, TraceStore

BLACKLISTED_MODULES = [pytest, pluggy, _pytest]

BLACKLISTED_PYTEST_LOCATIONS: List[str] = []

for blacklisted_module in BLACKLISTED_MODULES:
    if not blacklisted_module.__file__:
        raise ValueError(
            f"The file path for the module {blacklisted_module.__name__} was not found"
        )
    BLACKLISTED_PYTEST_LOCATIONS.append(str(Path(blacklisted_module.__file__).parent))


def is_pytest_related_frame(frame: Frame) -> bool:
    for blacklisted_location in BLACKLISTED_PYTEST_LOCATIONS:
        if frame.file_path and blacklisted_location in frame.file_path:
            return True
    return False


class RootFrameCannotBeHoistedException(Exception):
    """The root frame cannot be hoisted"""


def hoist(frame: Frame) -> None:
    """Removes the frame `frame`, placing the removed frame's children in its place."""
    if frame.parent is None:
        raise RootFrameCannotBeHoistedException()
    frame.parent.add_children(frame.children, after=frame)
    frame.remove_from_parent()  # type: ignore


def remove_pytest_related_frames(root_frame: Frame) -> Sequence[Frame]:
    """Removes pytest related frames. May return multiple root frames in the scenario where the
    provided root frame is a pytest related frame."""
    if root_frame.children:
        for child_frame in root_frame.children:
            remove_pytest_related_frames(child_frame)

    if is_pytest_related_frame(root_frame) and root_frame.parent is None:
        return root_frame.children

    if is_pytest_related_frame(root_frame):
        hoist(root_frame)
        return []
    return [root_frame]


def render(session: Session, start_time: Optional[float] = None, tid: int = 1) -> TraceStore:
    if start_time is None:
        start_time = session.start_time

    renderer = SpeedscopeRenderer()
    root_frame = session.root_frame()
    if root_frame is None:
        return ChromeTraceEventFormatJSONStore()

    perfetto_events: TraceStore = ChromeTraceEventFormatJSONStore()
    new_roots: List[Frame] = list(remove_pytest_related_frames(root_frame))

    def render_root_frame(root_frame: Frame) -> TraceStore:
        result: TraceStore = ChromeTraceEventFormatJSONStore()
        events: List[SpeedscopeEvent] = renderer.render_frame(root_frame)

        inverted_speedscope_index: Dict[int, SpeedscopeFrame] = {
            v: k for (k, v) in renderer._frame_to_index.items()
        }

        for speedscope_event in events:
            speedscope_frame: Optional[SpeedscopeFrame] = inverted_speedscope_index.get(
                speedscope_event.frame
            )
            file: Optional[str] = speedscope_frame.file if speedscope_frame else None
            line: Optional[int] = speedscope_frame.line if speedscope_frame else None
            name: Optional[str] = speedscope_frame.name if speedscope_frame else None
            timestamp: Timestamp = Timestamp(speedscope_event.at + start_time)
            if (
                speedscope_event.type == SpeedscopeEventType.OPEN
                and name not in SYNTHETIC_LEAF_IDENTIFIERS
            ):
                result.add_begin_event(
                    name=name or "nothing",
                    category=Category("runtime"),
                    timestamp=timestamp,
                    args={"file": file or "", "line": str(line or 0), "name": name or ""},
                    tid=tid,
                )

            elif (
                speedscope_event.type == SpeedscopeEventType.CLOSE
                and name not in SYNTHETIC_LEAF_IDENTIFIERS
            ):
                result.add_end_event(timestamp=timestamp, tid=tid)
        return result

    for root in new_roots:
        perfetto_events.merge(render_root_frame(root))

    return perfetto_events
