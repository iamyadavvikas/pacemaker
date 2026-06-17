"""Tests for self-service live tuning (governor.tuning + agent set_tuning)."""

from __future__ import annotations

import pytest
from conftest import FakeSensor

from governor.config import GovernorConfig
from governor.governor import Governor, Mode
from governor.sensors.base import Level
from governor.tuning import apply_tuning, tuning_view


def _cfg(**kw) -> GovernorConfig:
    base = dict(dsn="fake", min_limit=1, max_limit=8, additive_increase=1, decrease_factor=0.5)
    base.update(kw)
    return GovernorConfig(**base)


def test_tuning_view_exposes_only_knobs():
    view = tuning_view(_cfg())
    assert set(view) == {
        "min_limit",
        "max_limit",
        "additive_increase",
        "decrease_factor",
        "poll_interval_s",
        "pause_on_critical",
    }
    # never leaks the DSN or enforcement creds
    assert "dsn" not in view
    assert "control_dsn" not in view


def test_apply_tuning_returns_new_config_unchanged_original():
    cfg = _cfg()
    new = apply_tuning(cfg, {"max_limit": 20})
    assert new.max_limit == 20
    assert cfg.max_limit == 8  # original frozen config untouched


def test_apply_tuning_rejects_unknown_knob():
    with pytest.raises(ValueError, match="unknown or non-tunable"):
        apply_tuning(_cfg(), {"dsn": "evil"})


def test_apply_tuning_rejects_out_of_range():
    with pytest.raises(ValueError, match="decrease_factor"):
        apply_tuning(_cfg(), {"decrease_factor": 1.5})
    with pytest.raises(ValueError, match="min_limit"):
        apply_tuning(_cfg(), {"min_limit": 0})


def test_apply_tuning_cross_field_min_le_max():
    with pytest.raises(ValueError, match="must be <= max_limit"):
        apply_tuning(_cfg(), {"min_limit": 10, "max_limit": 5})


def test_apply_tuning_empty_updates_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        apply_tuning(_cfg(), {})


def test_apply_tuning_bool_coercion_from_string():
    new = apply_tuning(_cfg(pause_on_critical=True), {"pause_on_critical": "off"})
    assert new.pause_on_critical is False
    new = apply_tuning(_cfg(pause_on_critical=False), {"pause_on_critical": "true"})
    assert new.pause_on_critical is True


def test_governor_set_tuning_applies_and_snapshot_reflects():
    sensor = FakeSensor([Level.GREEN])
    gov = Governor(_cfg(max_limit=8), sensor, mode=Mode.OBSERVE)
    result = gov.set_tuning({"max_limit": 16, "additive_increase": 2})
    assert result["max_limit"] == 16
    snap = gov.snapshot()
    assert snap["tuning"]["max_limit"] == 16
    assert snap["tuning"]["additive_increase"] == 2


def test_governor_set_tuning_clamps_limit_to_new_max():
    sensor = FakeSensor([Level.GREEN])
    gov = Governor(_cfg(min_limit=1, max_limit=50), sensor, mode=Mode.OBSERVE)
    # push limit up first by reporting headroom, then lower max below it
    gov.set_tuning({"max_limit": 2})
    snap = gov.snapshot()
    assert snap["limit"] <= 2
    assert snap["tuning"]["max_limit"] == 2


def test_governor_set_tuning_rejects_bad_and_keeps_config():
    sensor = FakeSensor([Level.GREEN])
    gov = Governor(_cfg(max_limit=8), sensor, mode=Mode.OBSERVE)
    with pytest.raises(ValueError):
        gov.set_tuning({"max_limit": -1})
    assert gov.snapshot()["tuning"]["max_limit"] == 8  # unchanged
