"""ObserverAgent tests — shadow pacing, no database required."""

from __future__ import annotations

import threading

from conftest import FakeCohortSensor

from governor.config import GovernorConfig
from governor.observer import ObserverAgent
from governor.sensors.base import Level


def _agent(script, **cfg_kwargs) -> ObserverAgent:
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01, **cfg_kwargs)
    return ObserverAgent(cfg, FakeCohortSensor(script))


def _wait(seconds: float = 0.2) -> None:
    threading.Event().wait(seconds)


def test_throttles_in_shadow_when_migration_exceeds_limit():
    # RED headroom drives the shadow limit down toward min (1), so a migration
    # cohort running 5 active is well over the allowed pace -> would-throttle.
    agent = _agent([(Level.RED, 5)])
    agent.start()
    try:
        _wait()
        verdict = agent.throttle_verdict()
        assert verdict["throttle"] is True
        assert verdict["mode"] == "observe"
        assert verdict["migration_active"] == 5
        snap = agent.snapshot()
        assert snap["would_throttle_total"] >= 1
        assert snap["migration_active"] == 5
    finally:
        agent.stop()


def test_no_throttle_when_migration_idle_and_green():
    agent = _agent([(Level.GREEN, 0)])
    agent.start()
    try:
        _wait()
        assert agent.throttle_verdict()["throttle"] is False
    finally:
        agent.stop()


def test_critical_forces_throttle_verdict():
    agent = _agent([(Level.CRITICAL, 2)])
    agent.start()
    try:
        _wait()
        verdict = agent.throttle_verdict()
        assert verdict["throttle"] is True
        assert verdict["level"] == "CRITICAL"
        # shadow limit pauses at 0 on critical
        assert verdict["limit"] == 0
    finally:
        agent.stop()


def test_sensor_error_fails_safe_to_throttle():
    class _Boom(FakeCohortSensor):
        def sample(self):
            raise ConnectionError("db gone")

    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01)
    agent = ObserverAgent(cfg, _Boom())
    agent.start()
    try:
        _wait()
        # unreadable DB -> CRITICAL -> would-throttle, but never blocks anything
        assert agent.throttle_verdict()["throttle"] is True
        kinds = {e["kind"] for e in agent.snapshot()["events"]}
        assert "sensor_error" in kinds
    finally:
        agent.stop()


def test_never_enforces_observer_has_no_set_mode():
    agent = _agent([(Level.GREEN, 0)])
    # observe-only: it must not expose a runtime enforce switch
    assert not hasattr(agent, "set_mode")


def test_snapshot_is_json_ready_and_complete():
    import json

    agent = _agent([(Level.GREEN, 1), (Level.YELLOW, 3), (Level.RED, 5)])
    agent.start()
    try:
        _wait()
        snap = agent.snapshot()
        json.dumps(snap)  # must be serializable
        for key in (
            "mode",
            "limit",
            "in_flight",
            "last_level",
            "running",
            "migration_active",
            "would_throttle_now",
            "would_throttle_total",
            "samples",
            "events",
        ):
            assert key in snap
        assert snap["mode"] == "observe"
    finally:
        agent.stop()
