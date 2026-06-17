"""Sensor interface: turns live DB signals into a normalized headroom reading."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


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
