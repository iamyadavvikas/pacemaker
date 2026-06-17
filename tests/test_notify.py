"""Notifier tests — webhook payloads, fan-out, fail-safety."""

from __future__ import annotations

from governor.notify import (
    CallbackNotifier,
    MultiNotifier,
    NullNotifier,
    SignalFxNotifier,
    SlackNotifier,
)


def test_null_notifier_is_noop():
    NullNotifier().notify("anything", "msg", {"a": 1})  # must not raise


def test_callback_notifier_invokes_fn():
    seen = []
    n = CallbackNotifier(lambda kind, msg, ctx: seen.append((kind, msg, ctx)))
    n.notify("throttle_started", "begin", {"limit": 2})
    assert seen == [("throttle_started", "begin", {"limit": 2})]


def test_multi_notifier_fans_out_and_isolates_failures():
    seen = []
    good = CallbackNotifier(lambda k, m, c: seen.append(k))

    def _boom(k, m, c):
        raise RuntimeError("sink down")

    bad = CallbackNotifier(_boom)
    MultiNotifier(bad, good).notify("sensor_error", "db gone", None)
    # the good sink still fires even though the bad one raised
    assert seen == ["sensor_error"]


def test_slack_payload_shape(monkeypatch):
    captured = {}

    def _fake_post(url, payload, headers, timeout):
        captured["url"] = url
        captured["payload"] = payload
        captured["headers"] = headers

    monkeypatch.setattr("governor.notify._post_json", _fake_post)
    n = SlackNotifier("https://hooks.slack.test/abc")
    n.notify("would_circuit_break", "at CRITICAL", {"migration_active": 9})
    _join_notify_threads()

    assert captured["url"] == "https://hooks.slack.test/abc"
    assert "would_circuit_break" in captured["payload"]["text"]
    assert "migration_active=9" in captured["payload"]["text"]


def test_signalfx_payload_and_token(monkeypatch):
    captured = {}

    def _fake_post(url, payload, headers, timeout):
        captured["url"] = url
        captured["payload"] = payload
        captured["headers"] = headers

    monkeypatch.setattr("governor.notify._post_json", _fake_post)
    n = SignalFxNotifier(token="tok123", realm="us1")
    n.notify("enforcer_tripped", "runaway", {"cancels_total": 51})
    _join_notify_threads()

    assert captured["url"] == "https://ingest.us1.signalfx.com/v2/event"
    assert captured["headers"]["X-SF-Token"] == "tok123"
    event = captured["payload"][0]
    assert event["eventType"] == "dbguard.enforcer_tripped"
    assert event["properties"]["cancels_total"] == "51"


def _join_notify_threads() -> None:
    """Wait for fire-and-forget notify daemon threads to run."""
    import threading

    for t in threading.enumerate():
        if t.name == "dbguard-notify":
            t.join(timeout=2)
