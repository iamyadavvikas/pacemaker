"""Run report: timeline of governor decisions plus a human-readable summary."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass, field


@dataclass
class Sample:
    t: float
    active_backends: int
    blocked_backends: int
    level: str
    limit: int
    in_flight: int


@dataclass
class Event:
    t: float
    kind: str
    detail: str


@dataclass
class Report:
    label: str
    started_at: float = field(default_factory=time.time)
    samples: list[Sample] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_sample(self, s: Sample) -> None:
        with self._lock:
            self.samples.append(s)

    def add_event(self, kind: str, detail: str) -> None:
        with self._lock:
            self.events.append(Event(t=time.time() - self.started_at, kind=kind, detail=detail))

    # --- derived metrics ---
    def max_active(self) -> int:
        return max((s.active_backends for s in self.samples), default=0)

    def total_blocked_samples(self) -> int:
        return sum(1 for s in self.samples if s.blocked_backends > 0)

    def pause_events(self) -> int:
        return sum(1 for e in self.events if e.kind == "circuit_break")

    def backoff_events(self) -> int:
        return sum(1 for e in self.events if e.kind == "backoff")

    def snapshot(self, window: int = 240, event_window: int = 40) -> dict:
        """A thread-safe, JSON-ready view of recent timeline state.

        Returns the most recent ``window`` samples and ``event_window`` events
        (oldest-to-newest) plus the run-wide derived counts. Taken under the
        write lock so a concurrent sampler thread can't tear the lists mid-read.
        """
        with self._lock:
            recent_samples = self.samples[-window:] if window else list(self.samples)
            recent_events = self.events[-event_window:] if event_window else list(self.events)
            return {
                "label": self.label,
                "started_at": self.started_at,
                "sample_count": len(self.samples),
                "event_count": len(self.events),
                "max_active_backends": self.max_active(),
                "blocked_sample_count": self.total_blocked_samples(),
                "backoff_events": self.backoff_events(),
                "circuit_break_events": self.pause_events(),
                "samples": [asdict(s) for s in recent_samples],
                "events": [asdict(e) for e in recent_events],
            }

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "max_active_backends": self.max_active(),
            "blocked_sample_count": self.total_blocked_samples(),
            "backoff_events": self.backoff_events(),
            "circuit_break_events": self.pause_events(),
            "samples": [asdict(s) for s in self.samples],
            "events": [asdict(e) for e in self.events],
        }

    def write_json(self, path: str) -> None:
        with open(path, "w") as fh:
            json.dump(self.to_dict(), fh, indent=2)
