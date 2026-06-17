"""Sensor interface: turns live DB signals into a normalized headroom reading."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import Thresholds


class Level(enum.IntEnum):
    GREEN = 0
    YELLOW = 1
    RED = 2
    CRITICAL = 3


@dataclass
class CohortLoad:
    """Active / lock-waiting backend counts for one attributed workload cohort.

    ``victims`` lists ``(pid, signal)`` for the *active* backends in this cohort,
    where ``signal`` is the attribution signal that matched (``usename`` /
    ``query_tag`` / ``application_name``). It is populated only for the migration
    cohort and lets an enforcer pace the job by cancelling specific backends —
    preferring strong signals and never touching prod.
    """

    active: int = 0
    blocked: int = 0
    victims: list[tuple[int, str]] = field(default_factory=list)


@dataclass
class Headroom:
    level: Level
    active_backends: int
    blocked_backends: int
    raw: dict = field(default_factory=dict)
    # Optional per-cohort breakdown (e.g. {"migration": CohortLoad, "prod": CohortLoad}).
    # Empty unless the sensor was given a cohort classifier.
    cohorts: dict[str, CohortLoad] = field(default_factory=dict)
    # Optional secondary signals (``None`` when the sensor can't or doesn't read
    # them). Surfaced for the dashboard/report and used to escalate ``level``.
    replication_lag_s: float | None = None
    conn_pool_used: int | None = None
    conn_pool_max: int | None = None
    # p99 of the sensor's own probe round-trip (ms) — a DB-layer latency canary.
    query_latency_ms: float | None = None

    @property
    def conn_pool_frac(self) -> float | None:
        """Used/max connection fraction in [0, 1], or ``None`` if unknown."""
        if self.conn_pool_used is None or not self.conn_pool_max:
            return None
        return self.conn_pool_used / self.conn_pool_max


class LatencyTracker:
    """Rolling p99 of the sensor's own probe round-trip latency.

    Each ``sample()`` times the round-trip of the health query it already runs and
    feeds it here; ``p99()`` returns the 99th percentile over the last ``maxlen``
    probes. This is a *real* measured latency percentile observed at the DB layer
    with zero extra queries and no engine extension (no ``pg_stat_statements`` /
    ``pg_stat_monitor`` grant required) — the probe is the canary that slows down
    exactly when a migration saturates the engine.
    """

    def __init__(self, maxlen: int = 240) -> None:
        self._samples: deque[float] = deque(maxlen=maxlen)

    def record(self, ms: float) -> None:
        self._samples.append(ms)

    def p99(self) -> float | None:
        if not self._samples:
            return None
        ordered = sorted(self._samples)
        idx = min(len(ordered) - 1, int(round(0.99 * (len(ordered) - 1))))
        return round(ordered[idx], 2)


def level_from_signals(
    thresholds: "Thresholds",
    active: int,
    blocked: int,
    replication_lag_s: float | None = None,
    conn_pool_frac: float | None = None,
    query_latency_ms: float | None = None,
) -> Level:
    """Map raw + optional secondary DB signals to a headroom Level.

    Shared by every sensor so Postgres and Mongo escalate identically. The
    primary signal is ``active`` (concurrently-executing work). Secondary signals
    (replication lag, connection-pool saturation, query latency) can only *raise*
    the level, never lower it — adding them is always at least as conservative.
    """
    t = thresholds
    if active >= t.critical_active:
        level = Level.CRITICAL
    elif active <= t.green_max_active:
        level = Level.GREEN
    elif active <= t.yellow_max_active:
        level = Level.YELLOW
    else:
        level = Level.RED

    if blocked > 0 and t.blocked_forces_red and level < Level.RED:
        level = Level.RED

    level = max(level, _escalate(replication_lag_s, t.critical_replication_lag_s,
                                 t.red_replication_lag_s, t.yellow_replication_lag_s))
    level = max(level, _escalate(conn_pool_frac, t.critical_conn_pool_frac,
                                 t.red_conn_pool_frac, t.yellow_conn_pool_frac))
    level = max(level, _escalate(query_latency_ms, t.critical_query_latency_ms,
                                 t.red_query_latency_ms, t.yellow_query_latency_ms))
    return level


def _escalate(
    value: float | None,
    critical: float | None,
    red: float | None,
    yellow: float | None,
) -> Level:
    """Level implied by one secondary signal crossing its bounds (worst first)."""
    if value is None:
        return Level.GREEN
    if critical is not None and value >= critical:
        return Level.CRITICAL
    if red is not None and value >= red:
        return Level.RED
    if yellow is not None and value >= yellow:
        return Level.YELLOW
    return Level.GREEN


class Sensor(ABC):
    """Read-only probe of database health.

    Implementations MUST be side-effect free and use a read-only connection in
    production. They must never write to or lock the target database.
    """

    @abstractmethod
    def sample(self) -> Headroom:
        ...

    def close(self) -> None:  # pragma: no cover - optional override
        pass
