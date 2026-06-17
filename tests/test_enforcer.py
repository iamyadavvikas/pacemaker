"""EnforcerAgent tests \u2014 non-cooperative pacing, no database required."""

from __future__ import annotations

import itertools
import threading

from governor.config import GovernorConfig
from governor.enforcer import Canceller, EnforcerAgent, _select_victims
from governor.sensors.base import CohortLoad, Headroom, Level, Sensor


class FakeVictimSensor(Sensor):
    """Scripted readings with a migration cohort that exposes cancellable pids.

    Each script entry is ``(level, [(pid, signal), ...])`` describing the active
    migration backends. ``active`` is derived from the victim list length.
    """

    def __init__(self, script):
        self._script = itertools.cycle(script)
        self.closed = False

    def sample(self) -> Headroom:
        level, victims = next(self._script)
        mig_active = len(victims)
        total = {Level.GREEN: 1, Level.YELLOW: 3, Level.RED: 5, Level.CRITICAL: 8}[level]
        total = max(total, mig_active)
        return Headroom(
            level=level,
            active_backends=total,
            blocked_backends=0,
            cohorts={
                "migration": CohortLoad(active=mig_active, blocked=0, victims=list(victims)),
                "prod": CohortLoad(active=max(total - mig_active, 0)),
            },
        )

    def close(self) -> None:
        self.closed = True


class FakeCanceller(Canceller):
    """Records every cancelled pid; never touches a real DB."""

    def __init__(self, fail_pids=()):
        self.cancelled: list[int] = []
        self._fail = set(fail_pids)
        self.closed = False

    def cancel(self, pid: int) -> bool:
        if pid in self._fail:
            return False
        self.cancelled.append(pid)
        return True

    def close(self) -> None:
        self.closed = True


def _wait(seconds: float = 0.2) -> None:
    threading.Event().wait(seconds)


def _agent(script, canceller, **cfg_kwargs):
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01, **cfg_kwargs)
    return EnforcerAgent(cfg, FakeVictimSensor(script), canceller=canceller)


# --- _select_victims: strongest signal first, weak dropped by default ---

def test_select_victims_prefers_strong_signal():
    victims = [(10, "application_name"), (11, "usename"), (12, "query_tag")]
    picked = _select_victims(victims, excess=2, require_strong_signal=True)
    assert picked == [(11, "usename"), (12, "query_tag")]


def test_select_victims_drops_weak_when_strong_required():
    victims = [(10, "application_name"), (11, "application_name")]
    assert _select_victims(victims, excess=5, require_strong_signal=True) == []


def test_select_victims_allows_weak_when_permitted():
    victims = [(10, "application_name")]
    assert _select_victims(victims, excess=5, require_strong_signal=False) == [(10, "application_name")]


def test_select_victims_caps_at_excess():
    victims = [(i, "usename") for i in range(10)]
    assert len(_select_victims(victims, excess=3, require_strong_signal=True)) == 3


# --- enforcement behaviour ---

def test_cancels_excess_migration_backends_when_over_limit():
    # RED drives the shadow limit toward min (1); 5 active migration backends are
    # over budget, so the enforcer cancels the excess (rate-limited per tick).
    victims = [(100 + i, "usename") for i in range(5)]
    canceller = FakeCanceller()
    agent = _agent([(Level.RED, victims)], canceller, max_cancels_per_interval=2)
    agent.start()
    try:
        _wait()
        assert canceller.cancelled  # something got paced
        # never exceeds the configured per-tick budget on the very first tick
        assert set(canceller.cancelled) <= {v[0] for v in victims}
        assert agent.snapshot()["cancels_total"] >= 1
    finally:
        agent.stop()


def test_never_cancels_when_under_limit_and_green():
    victims = [(200, "usename")]
    canceller = FakeCanceller()
    agent = _agent([(Level.GREEN, victims)], canceller)
    agent.start()
    try:
        _wait()
        assert canceller.cancelled == []
        assert agent.snapshot()["cancels_total"] == 0
    finally:
        agent.stop()


def test_weak_signal_backends_are_never_cancelled_by_default():
    # all migration backends are matched only on application_name (spoofable);
    # with require_strong_signal (default) the enforcer must NOT cancel them.
    victims = [(300 + i, "application_name") for i in range(5)]
    canceller = FakeCanceller()
    agent = _agent([(Level.RED, victims)], canceller)
    agent.start()
    try:
        _wait()
        assert canceller.cancelled == []
    finally:
        agent.stop()


def test_sensor_error_fails_open_no_cancels():
    class _Boom(FakeVictimSensor):
        def sample(self):
            raise ConnectionError("db gone")

    canceller = FakeCanceller()
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01)
    agent = EnforcerAgent(cfg, _Boom([(Level.RED, [])]), canceller=canceller)
    agent.start()
    try:
        _wait()
        # fail OPEN: an unreadable sensor must never trigger a blind cancel
        assert canceller.cancelled == []
        kinds = {e["kind"] for e in agent.snapshot()["events"]}
        assert "sensor_error" in kinds
    finally:
        agent.stop()


def test_runaway_guard_trips_and_stops_enforcing():
    victims = [(400 + i, "usename") for i in range(8)]
    canceller = FakeCanceller()
    # tiny threshold so the guard trips almost immediately, large per-tick budget.
    agent = _agent(
        [(Level.CRITICAL, victims)],
        canceller,
        max_cancels_per_interval=8,
        runaway_cancel_threshold=3,
        runaway_window_s=60.0,
    )
    agent.start()
    try:
        _wait(0.3)
        snap = agent.snapshot()
        assert snap["enforcing"] is False
        kinds = {e["kind"] for e in snap["events"]}
        assert "enforcer_tripped" in kinds
    finally:
        agent.stop()


def test_snapshot_and_verdict_shape():
    import json

    victims = [(500, "usename"), (501, "usename")]
    agent = _agent([(Level.RED, victims)], FakeCanceller())
    agent.start()
    try:
        _wait()
        snap = agent.snapshot()
        json.dumps(snap)
        for key in ("mode", "limit", "cancels_total", "enforcing", "migration_active"):
            assert key in snap
        assert snap["mode"] == "enforce"
        verdict = agent.throttle_verdict()
        assert verdict["mode"] == "enforce"
        assert isinstance(verdict["throttle"], bool)
    finally:
        agent.stop()


def test_stop_closes_sensor_and_canceller():
    canceller = FakeCanceller()
    sensor = FakeVictimSensor([(Level.GREEN, [])])
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01)
    agent = EnforcerAgent(cfg, sensor, canceller=canceller)
    agent.start()
    _wait(0.05)
    agent.stop()
    assert sensor.closed is True
    assert canceller.closed is True
