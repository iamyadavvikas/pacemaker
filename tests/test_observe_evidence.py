"""Evidence/counterfactual helpers for ``dbguard observe`` — no database required."""

from __future__ import annotations

import json
import threading

from conftest import FakeCohortSensor

from governor.attribution import CohortClassifier
from governor.config import GovernorConfig
from governor.observer import ObserverAgent
from governor.sensors.base import Level

from harness.checkout_probe import CheckoutProbe
from harness.observe import _evidence_report, _throttle_episodes


def _wait(seconds: float = 0.2) -> None:
    threading.Event().wait(seconds)


# --- _throttle_episodes: collapse consecutive throttling samples into episodes ---

def test_episodes_empty_is_zero():
    assert _throttle_episodes([]) == 0


def test_episodes_none_when_always_under_limit():
    samples = [{"limit": 4, "in_flight": 1} for _ in range(5)]
    assert _throttle_episodes(samples) == 0


def test_episodes_consecutive_run_counts_once():
    # one uninterrupted run of throttling samples = a single episode,
    # not one-per-sample (the bug we are fixing).
    samples = [{"limit": 2, "in_flight": 5} for _ in range(10)]
    assert _throttle_episodes(samples) == 1


def test_episodes_separate_runs_count_separately():
    over = {"limit": 2, "in_flight": 5}
    under = {"limit": 4, "in_flight": 0}
    samples = [over, over, under, under, over, under, over]
    assert _throttle_episodes(samples) == 3


def test_episodes_paused_limit_zero_counts_as_throttling():
    samples = [{"limit": 0, "in_flight": 0}, {"limit": 0, "in_flight": 0}]
    assert _throttle_episodes(samples) == 1


# --- CheckoutProbe.latency_payload: bucketed p95 series, no DB connection ---

def test_latency_payload_empty_series():
    probe = CheckoutProbe("postgresql://unused")  # not started -> never connects
    assert probe.latency_payload() == {"latency_series": []}


def test_latency_payload_buckets_recent_samples():
    probe = CheckoutProbe("postgresql://unused")
    # inject a synthetic series of (t_seconds, latency_ms) directly.
    probe._series = [(float(i) * 0.1, 10.0 + i) for i in range(50)]
    payload = probe.latency_payload(window_s=40.0, buckets=10)
    series = payload["latency_series"]
    assert isinstance(series, list) and series
    for point in series:
        assert len(point) == 2  # [t, p95_ms]
    # times must be non-decreasing across buckets
    times = [p[0] for p in series]
    assert times == sorted(times)


# --- _evidence_report: episodes + honest counterfactual labelling ---

def _agent(script):
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01)
    agent = ObserverAgent(cfg, FakeCohortSensor(script))
    agent.start()
    _wait()
    agent.stop()
    return agent


def test_evidence_report_correlation_only_without_counterfactual():
    agent = _agent([(Level.RED, 5)])
    classifier = CohortClassifier.from_lists(app_names=["mig"])
    report = _evidence_report(agent, classifier)
    json.dumps(report)  # must stay serializable
    assert report["mode"] == "observe"
    assert "would_throttle_episodes" in report
    assert "would_throttle_samples" in report
    assert report["counterfactual"]["kind"] == "correlation_only"


def test_evidence_report_includes_measured_ab_and_checkout():
    agent = _agent([(Level.RED, 5)])
    classifier = CohortClassifier.from_lists(app_names=["mig"])
    counterfactual = {
        "safe_pace": 1,
        "uncapped_p99_ms": 412.0,
        "capped_p99_ms": 22.0,
    }
    probe_summary = {"p50_ms": 5.0, "p95_ms": 18.0, "p99_ms": 22.0, "max_ms": 30.0}
    report = _evidence_report(
        agent, classifier, counterfactual=counterfactual, probe_summary=probe_summary
    )
    json.dumps(report)
    assert report["counterfactual"]["kind"] == "measured_ab"
    assert report["counterfactual"]["uncapped_p99_ms"] == 412.0
    assert report["counterfactual"]["capped_p99_ms"] == 22.0
    assert report["measured_checkout"]["p99_ms"] == 22.0
