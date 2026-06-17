"""Unit tests for the Governor control loop (no database required)."""

from __future__ import annotations

import time

from governor import Governor, GovernorConfig, Mode
from governor.sensors.base import Level

from conftest import FakeSensor


def _settle(seconds: float = 0.6) -> None:
    time.sleep(seconds)


def _cfg() -> GovernorConfig:
    return GovernorConfig(dsn="x", poll_interval_s=0.02, start_limit=3, max_limit=6, min_limit=1)


def test_green_ramps_up_to_max():
    gov = Governor(_cfg(), FakeSensor([Level.GREEN]), mode=Mode.ENFORCE)
    gov.start()
    _settle()
    gov.stop()
    assert gov.current_limit == 6


def test_red_backs_off():
    gov = Governor(_cfg(), FakeSensor([Level.RED]), mode=Mode.ENFORCE)
    gov.start()
    _settle()
    gov.stop()
    assert gov.current_limit == 1
    assert gov.report.backoff_events() >= 1


def test_critical_circuit_breaks():
    gov = Governor(_cfg(), FakeSensor([Level.CRITICAL]), mode=Mode.ENFORCE)
    gov.start()
    _settle()
    gov.stop()
    assert gov.current_limit == 0
    assert gov.report.pause_events() >= 1


def test_sensor_failure_fails_safe_to_pause():
    # First sample ok, then the "DB" goes unreachable -> must clamp to 0, not coast.
    gov = Governor(_cfg(), FakeSensor([Level.GREEN], raise_after=1), mode=Mode.ENFORCE)
    gov.start()
    _settle()
    gov.stop()
    assert gov.current_limit == 0
    assert any(e.kind == "sensor_error" for e in gov.report.events)


def test_observe_mode_never_blocks():
    # limit will be 0 (critical) but OBSERVE must still let batches through.
    gov = Governor(_cfg(), FakeSensor([Level.CRITICAL]), mode=Mode.OBSERVE)
    gov.start()
    _settle(0.2)
    ran = []
    with gov.batch():
        ran.append(True)
    gov.stop()
    assert ran == [True]
    assert any(e.kind == "would_throttle" for e in gov.report.events)


def test_stop_closes_sensor():
    sensor = FakeSensor([Level.GREEN])
    gov = Governor(_cfg(), sensor, mode=Mode.ENFORCE)
    gov.start()
    _settle(0.1)
    gov.stop()
    assert sensor.closed is True
