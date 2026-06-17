"""Unit tests for the AIMD pacing policy."""

from __future__ import annotations

from governor.config import GovernorConfig
from governor.policy.aimd import next_limit
from governor.sensors.base import Level

CFG = GovernorConfig(
    dsn="x",
    min_limit=1,
    max_limit=6,
    additive_increase=1,
    decrease_factor=0.5,
)


def test_green_additively_increases():
    assert next_limit(2, Level.GREEN, CFG) == 3


def test_green_respects_max_limit():
    assert next_limit(6, Level.GREEN, CFG) == 6


def test_yellow_holds():
    assert next_limit(4, Level.YELLOW, CFG) == 4


def test_red_multiplicatively_decreases():
    assert next_limit(6, Level.RED, CFG) == 3


def test_red_respects_min_limit():
    assert next_limit(1, Level.RED, CFG) == 1


def test_critical_circuit_breaks_to_zero():
    assert next_limit(6, Level.CRITICAL, CFG) == 0


def test_critical_throttle_only_decreases_not_pauses():
    # pause_on_critical=False -> never drops to 0; decreases toward min_limit.
    cfg = GovernorConfig(
        dsn="x", min_limit=1, max_limit=6, decrease_factor=0.5, pause_on_critical=False
    )
    assert next_limit(6, Level.CRITICAL, cfg) == 3
    assert next_limit(2, Level.CRITICAL, cfg) == 1
    # never below min_limit
    assert next_limit(1, Level.CRITICAL, cfg) == 1


def test_critical_throttle_only_respects_higher_min_limit():
    cfg = GovernorConfig(
        dsn="x", min_limit=2, max_limit=8, decrease_factor=0.5, pause_on_critical=False
    )
    assert next_limit(3, Level.CRITICAL, cfg) == 2  # floor(3*0.5)=1 -> clamped to min 2

