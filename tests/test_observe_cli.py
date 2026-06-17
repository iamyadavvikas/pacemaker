"""Unit tests for the unified (Postgres + Mongo) ``dbguard observe`` CLI wiring.

All pure-function / arg-guard coverage — no real database required.
"""

from __future__ import annotations

from harness.observe import _calibrate_thresholds, _is_mongo_dsn, main


def test_is_mongo_dsn_detects_scheme():
    assert _is_mongo_dsn("mongodb://localhost:27017")
    assert _is_mongo_dsn("mongodb+srv://user:pw@cluster.example.net/?retryWrites=true")
    assert not _is_mongo_dsn("postgresql://gov_sensor:pw@host:5432/app")
    assert not _is_mongo_dsn("postgres://localhost/db")


def test_mongo_dsn_rejects_demo():
    # --demo seeds synthetic data, so it must be refused for a Mongo target.
    assert main(["--dsn", "mongodb://localhost:27017", "--demo"]) == 2


def test_mongo_dsn_rejects_sql_probe_query():
    # --probe-query is SQL; meaningless against MongoDB.
    rc = main(["--dsn", "mongodb://localhost:27017", "--probe-query", "SELECT 1"])
    assert rc == 2


class _FakeSample:
    def __init__(self, active: int) -> None:
        self.active_backends = active


class _ScriptedSensor:
    """Returns a scripted sequence of active-backend readings, then repeats last."""

    def __init__(self, actives: list[int]) -> None:
        self._actives = actives
        self._i = 0

    def sample(self) -> _FakeSample:
        a = self._actives[min(self._i, len(self._actives) - 1)]
        self._i += 1
        return _FakeSample(a)


def test_calibrate_derives_thresholds_from_baseline():
    # A baseline that hovers around 8-12 active backends should yield thresholds
    # well above the 1-CPU demo defaults (green<=2 / critical>=6).
    sensor = _ScriptedSensor([8, 10, 9, 11, 12, 10, 9, 10])
    th = _calibrate_thresholds(sensor, seconds=0.05, poll_s=0.0)
    assert th is not None
    assert th.green_max_active >= 8
    assert th.yellow_max_active > th.green_max_active
    assert th.critical_active >= th.yellow_max_active + 2


def test_calibrate_returns_none_without_samples():
    class _Dead:
        def sample(self):  # noqa: ANN001
            raise RuntimeError("unreachable")

    assert _calibrate_thresholds(_Dead(), seconds=0.02, poll_s=0.0) is None


def test_preflight_flag_parses_and_defaults_off():
    from harness.observe import _build_parser

    args = _build_parser().parse_args(["--dsn", "mongodb://localhost:27017"])
    assert args.preflight is False
    args = _build_parser().parse_args(["--dsn", "mongodb://localhost:27017", "--preflight"])
    assert args.preflight is True

