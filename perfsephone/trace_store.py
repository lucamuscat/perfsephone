"""An abstraction over storing and keeping track of tracing data."""

import json
import time
from abc import ABC, abstractmethod
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

from perfsephone import (
    BeginDurationEvent,
    Category,
    EndDurationEvent,
    InstantEvent,
    InstantScope,
    SerializableEvent,
    Timestamp,
)


class TraceStore(ABC):
    @abstractmethod
    def add_begin_event(  # noqa: PLR0913
        self,
        *,
        name: str,
        category: Category,
        timestamp: Optional[Timestamp] = None,
        pid: int = 1,
        tid: int = 1,
        args: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_end_event(
        self, *, pid: int = 1, tid: int = 1, timestamp: Optional[Timestamp] = None
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_instant_event(
        self,
        *,
        name: str,
        pid: int = 1,
        tid: int = 1,
        timestamp: Optional[Timestamp] = None,
        scope: InstantScope,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def dump(self, *, path: Path) -> None:
        raise NotImplementedError

    @abstractmethod
    def merge(self, other: "TraceStore") -> None:
        raise NotImplementedError


class ChromeTraceEventFormatJSONStore(TraceStore):
    def __init__(self) -> None:
        self.events: List[SerializableEvent] = []

    def add_begin_event(  # noqa: PLR0913
        self,
        *,
        name: str,
        category: Category,
        timestamp: Optional[Timestamp] = None,
        pid: int = 1,
        tid: int = 1,
        args: Optional[Dict[str, Union[str, Sequence[str]]]] = None,
    ) -> None:
        if timestamp is None:
            timestamp = Timestamp(time.time())

        if args is None:
            args = {}

        self.events.append(
            BeginDurationEvent(name=name, cat=category, ts=timestamp, pid=pid, tid=tid, args=args)
        )

    def add_end_event(
        self, *, pid: int = 1, tid: int = 1, timestamp: Optional[Timestamp] = None
    ) -> None:
        if timestamp is None:
            timestamp = Timestamp(time.time())

        self.events.append(EndDurationEvent(pid=pid, tid=tid, ts=timestamp))

    def add_instant_event(
        self,
        *,
        name: str,
        pid: int = 1,
        tid: int = 1,
        timestamp: Optional[Timestamp] = None,
        scope: InstantScope = InstantScope.t,
    ) -> None:
        if timestamp is None:
            timestamp = Timestamp(time.time())

        self.events.append(InstantEvent(name=name, pid=pid, tid=tid, ts=timestamp, s=scope))

    def dump(self, *, path: Path) -> None:
        with path.open("w") as file:
            result = [asdict(event) for event in self.events]
            for event in result:
                # Python's time.time() produces timestamps using a seconds as its granularity,
                # whilst perfetto uses a miceosecond granularity.
                event["ts"] /= 1e-6

            json.dump(result, file)

    def merge(self, other: TraceStore) -> None:
        assert isinstance(other, ChromeTraceEventFormatJSONStore)
        self.events += other.events
