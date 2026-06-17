"""Shared test fixtures and fakes."""

from __future__ import annotations

import itertools

from governor.sensors.base import CohortLoad, Headroom, Level, Sensor


class FakeSensor(Sensor):
    """Returns scripted headroom levels; optionally raises to simulate an unreachable DB."""

    def __init__(self, levels: list[Level] | None = None, raise_after: int | None = None) -> None:
        self._levels = itertools.cycle(levels or [Level.GREEN])
        self._raise_after = raise_after
        self._calls = 0
        self.closed = False

    def sample(self) -> Headroom:
        self._calls += 1
        if self._raise_after is not None and self._calls > self._raise_after:
            raise ConnectionError("simulated DB unreachable")
        level = next(self._levels)
        active = {Level.GREEN: 1, Level.YELLOW: 3, Level.RED: 5, Level.CRITICAL: 8}[level]
        return Headroom(level=level, active_backends=active, blocked_backends=0)

    def close(self) -> None:
        self.closed = True


class FakeCohortSensor(Sensor):
    """Returns scripted (level, migration_active) readings with a cohort breakdown.

    Drives the ObserverAgent without a database: each entry is a tuple of a
    ``Level`` and the number of active migration-cohort backends, so a test can
    script exactly when the shadow policy should decide to throttle.
    """

    def __init__(self, script: list[tuple[Level, int]] | None = None) -> None:
        self._script = itertools.cycle(script or [(Level.GREEN, 0)])
        self.closed = False

    def sample(self) -> Headroom:
        level, mig_active = next(self._script)
        total_active = {Level.GREEN: 1, Level.YELLOW: 3, Level.RED: 5, Level.CRITICAL: 8}[level]
        total_active = max(total_active, mig_active)
        return Headroom(
            level=level,
            active_backends=total_active,
            blocked_backends=0,
            cohorts={
                "migration": CohortLoad(active=mig_active, blocked=0),
                "prod": CohortLoad(active=max(total_active - mig_active, 0), blocked=0),
            },
        )

    def close(self) -> None:
        self.closed = True
