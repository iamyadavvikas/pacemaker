"""Tests for the shared level_from_signals escalation logic."""

from __future__ import annotations

from governor.config import Thresholds
from governor.sensors.base import Level, LatencyTracker, level_from_signals


def test_primary_active_count_levels():
    t = Thresholds()  # green<=2, yellow<=3, critical>=6
    assert level_from_signals(t, 1, 0) is Level.GREEN
    assert level_from_signals(t, 3, 0) is Level.YELLOW
    assert level_from_signals(t, 5, 0) is Level.RED
    assert level_from_signals(t, 6, 0) is Level.CRITICAL


def test_blocked_forces_at_least_red():
    t = Thresholds()
    assert level_from_signals(t, 1, 1) is Level.RED


def test_replication_lag_can_only_raise_level():
    t = Thresholds(yellow_replication_lag_s=5, red_replication_lag_s=10, critical_replication_lag_s=30)
    # green active count, but lag escalates
    assert level_from_signals(t, 1, 0, replication_lag_s=6) is Level.YELLOW
    assert level_from_signals(t, 1, 0, replication_lag_s=12) is Level.RED
    assert level_from_signals(t, 1, 0, replication_lag_s=40) is Level.CRITICAL
    # below the lowest bound -> no escalation
    assert level_from_signals(t, 1, 0, replication_lag_s=1) is Level.GREEN


def test_conn_pool_escalation_never_lowers():
    t = Thresholds(critical_conn_pool_frac=0.9)
    # active count alone is CRITICAL; a low pool frac must not lower it
    assert level_from_signals(t, 6, 0, conn_pool_frac=0.1) is Level.CRITICAL
    # low active but saturated pool -> CRITICAL
    assert level_from_signals(t, 1, 0, conn_pool_frac=0.95) is Level.CRITICAL


def test_none_secondary_signals_are_ignored():
    t = Thresholds(red_replication_lag_s=10, red_conn_pool_frac=0.8)
    assert level_from_signals(t, 1, 0, replication_lag_s=None, conn_pool_frac=None) is Level.GREEN


def test_query_latency_can_only_raise_level():
    t = Thresholds(
        yellow_query_latency_ms=15, red_query_latency_ms=40, critical_query_latency_ms=120
    )
    assert level_from_signals(t, 1, 0, query_latency_ms=20) is Level.YELLOW
    assert level_from_signals(t, 1, 0, query_latency_ms=50) is Level.RED
    assert level_from_signals(t, 1, 0, query_latency_ms=200) is Level.CRITICAL
    # under the lowest bound -> no escalation
    assert level_from_signals(t, 1, 0, query_latency_ms=5) is Level.GREEN
    # None ignored
    assert level_from_signals(t, 1, 0, query_latency_ms=None) is Level.GREEN


def test_query_latency_escalation_never_lowers():
    t = Thresholds(critical_query_latency_ms=120)
    # primary already CRITICAL; a tiny latency must not lower it
    assert level_from_signals(t, 6, 0, query_latency_ms=1) is Level.CRITICAL


def test_latency_tracker_empty_is_none():
    assert LatencyTracker().p99() is None


def test_latency_tracker_p99_picks_tail():
    lt = LatencyTracker()
    for v in range(1, 101):  # 1..100 ms
        lt.record(float(v))
    # 99th percentile of 1..100 is ~100 (index round(0.99*99)=98 -> value 99)
    assert lt.p99() == 99.0


def test_latency_tracker_window_evicts_old():
    lt = LatencyTracker(maxlen=3)
    lt.record(1000.0)
    lt.record(1.0)
    lt.record(2.0)
    lt.record(3.0)  # evicts the 1000.0 outlier
    assert lt.p99() == 3.0
