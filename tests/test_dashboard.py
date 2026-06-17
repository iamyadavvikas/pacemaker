"""Dashboard server tests — no database required (driven by FakeSensor)."""

from __future__ import annotations

import base64
import json
import threading
import urllib.error
import urllib.request

from conftest import FakeCohortSensor, FakeSensor

from governor.config import GovernorConfig
from governor.dashboard import DashboardServer
from governor.governor import Governor, Mode
from governor.observer import ObserverAgent
from governor.sensors.base import Level


def _make_governor(mode: Mode = Mode.ENFORCE) -> Governor:
    sensor = FakeSensor([Level.GREEN, Level.YELLOW, Level.RED, Level.CRITICAL])
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01)
    return Governor(cfg, sensor, mode=mode)


def _get(url: str) -> tuple[int, bytes]:
    with urllib.request.urlopen(url, timeout=3) as resp:
        return resp.status, resp.read()


def _post(url: str, payload) -> tuple[int, bytes]:
    data = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=3) as resp:
        return resp.status, resp.read()


def _basic(token: str, user: str = "x") -> str:
    return "Basic " + base64.b64encode(f"{user}:{token}".encode()).decode()


def test_serves_html_and_state():
    gov = _make_governor()
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        # let the sampler produce a few samples
        threading.Event().wait(0.2)

        code, html = _get(dash.url + "/")
        assert code == 200
        assert b"pacing-governor" in html

        code, body = _get(dash.url + "/api/state")
        assert code == 200
        state = json.loads(body)
        for key in ("mode", "limit", "in_flight", "last_level", "running", "samples", "events"):
            assert key in state
        assert state["mode"] == "enforce"
        assert state["running"] is True
        assert isinstance(state["samples"], list)
        assert state["last_level"] in ("GREEN", "YELLOW", "RED", "CRITICAL")

        code, body = _get(dash.url + "/api/health")
        assert code == 200
        assert json.loads(body) == {"ok": True}
    finally:
        dash.stop()
        gov.stop()


def test_unknown_path_404():
    gov = _make_governor()
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        try:
            _get(dash.url + "/does-not-exist")
            raise AssertionError("expected HTTP 404")
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
    finally:
        dash.stop()
        gov.stop()


def test_extra_metrics_merged_and_isolated():
    gov = _make_governor()
    gov.start()
    dash = DashboardServer(
        gov, host="127.0.0.1", port=0, extra_metrics=lambda: {"latency_series": [[0.0, 5.0]]}
    )
    dash.start()
    try:
        _, body = _get(dash.url + "/api/state")
        state = json.loads(body)
        assert state["extra"]["latency_series"] == [[0.0, 5.0]]
    finally:
        dash.stop()
        gov.stop()


def test_extra_metrics_failure_does_not_500():
    def boom() -> dict:
        raise RuntimeError("metrics hook failed")

    gov = _make_governor()
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0, extra_metrics=boom)
    dash.start()
    try:
        code, body = _get(dash.url + "/api/state")
        assert code == 200
        state = json.loads(body)
        assert "extra_metrics_error" in state["extra"]
    finally:
        dash.stop()
        gov.stop()


def test_set_mode_flips_and_snapshot_reflects_it():
    gov = _make_governor(Mode.ENFORCE)
    assert gov.mode is Mode.ENFORCE
    gov.set_mode("observe")
    assert gov.mode is Mode.OBSERVE
    assert gov.snapshot()["mode"] == "observe"
    gov.set_mode(Mode.ENFORCE)
    assert gov.snapshot()["mode"] == "enforce"


def test_set_mode_rejects_unknown():
    gov = _make_governor()
    try:
        gov.set_mode("turbo")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_post_mode_switches_governor():
    gov = _make_governor(Mode.ENFORCE)
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        code, body = _post(dash.url + "/api/mode", {"mode": "observe"})
        assert code == 200
        assert json.loads(body)["mode"] == "observe"
        assert gov.mode is Mode.OBSERVE

        _, body = _get(dash.url + "/api/state")
        assert json.loads(body)["mode"] == "observe"
    finally:
        dash.stop()
        gov.stop()


def test_post_mode_rejects_bad_body():
    gov = _make_governor()
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        for bad in (b"not json", {"mode": "turbo"}, {}):
            try:
                _post(dash.url + "/api/mode", bad)
                raise AssertionError("expected HTTP 400")
            except urllib.error.HTTPError as exc:
                assert exc.code == 400
        assert gov.mode is Mode.ENFORCE  # unchanged
    finally:
        dash.stop()
        gov.stop()


def test_baseline_surfaced_in_state():
    gov = _make_governor()
    gov.start()
    baseline = {"p50_ms": 5.0, "p99_ms": 90.0, "max_ms": 120.0, "blocked_samples": 31}
    dash = DashboardServer(gov, host="127.0.0.1", port=0, baseline=baseline)
    dash.start()
    try:
        _, body = _get(dash.url + "/api/state")
        assert json.loads(body)["baseline"] == baseline
    finally:
        dash.stop()
        gov.stop()


def test_auth_gate_rejects_and_accepts():
    gov = _make_governor(Mode.ENFORCE)
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0, auth_token="s3cret")
    dash.start()
    try:
        # no credentials -> 401 with a Basic challenge
        try:
            _get(dash.url + "/api/state")
            raise AssertionError("expected HTTP 401")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
            assert "Basic" in exc.headers.get("WWW-Authenticate", "")

        # wrong password -> 401
        try:
            req = urllib.request.Request(
                dash.url + "/api/state", headers={"Authorization": _basic("nope")}
            )
            urllib.request.urlopen(req, timeout=3)
            raise AssertionError("expected HTTP 401")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401

        # correct password -> 200 (any username is accepted)
        req = urllib.request.Request(
            dash.url + "/api/state", headers={"Authorization": _basic("s3cret", user="anyone")}
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            assert resp.status == 200
            assert "mode" in json.loads(resp.read())

        # health stays open for liveness probes even with auth on
        code, body = _get(dash.url + "/api/health")
        assert code == 200
        assert json.loads(body) == {"ok": True}
    finally:
        dash.stop()
        gov.stop()


def test_auth_gate_protects_post_mode():
    gov = _make_governor(Mode.ENFORCE)
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0, auth_token="s3cret")
    dash.start()
    try:
        # unauthenticated POST is rejected and must NOT change the mode
        try:
            _post(dash.url + "/api/mode", {"mode": "observe"})
            raise AssertionError("expected HTTP 401")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
        assert gov.mode is Mode.ENFORCE

        # authenticated POST works
        data = json.dumps({"mode": "observe"}).encode("utf-8")
        req = urllib.request.Request(
            dash.url + "/api/mode",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", "Authorization": _basic("s3cret")},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            assert resp.status == 200
        assert gov.mode is Mode.OBSERVE
    finally:
        dash.stop()
        gov.stop()


def test_post_tuning_applies_and_state_reflects():
    gov = _make_governor(Mode.OBSERVE)
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        code, body = _post(dash.url + "/api/tuning", {"max_limit": 16, "decrease_factor": 0.25})
        assert code == 200
        view = json.loads(body)["tuning"]
        assert view["max_limit"] == 16
        assert view["decrease_factor"] == 0.25

        _, body = _get(dash.url + "/api/state")
        assert json.loads(body)["tuning"]["max_limit"] == 16
    finally:
        dash.stop()
        gov.stop()


def test_post_tuning_rejects_bad_value_400():
    gov = _make_governor(Mode.OBSERVE)
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        for bad in (b"not json", {"max_limit": -1}, {"bogus": 1}, [1, 2]):
            try:
                _post(dash.url + "/api/tuning", bad)
                raise AssertionError("expected HTTP 400")
            except urllib.error.HTTPError as exc:
                assert exc.code == 400
    finally:
        dash.stop()
        gov.stop()


def test_post_tuning_405_when_unsupported():
    class _NoTuning:
        """A governor-like object exposing snapshot() but no set_tuning."""

        def snapshot(self, **_kw):
            return {
                "mode": "observe",
                "limit": 1,
                "in_flight": 0,
                "last_level": "GREEN",
                "running": True,
                "samples": [],
                "events": [],
            }

    dash = DashboardServer(_NoTuning(), host="127.0.0.1", port=0)
    dash.start()
    try:
        try:
            _post(dash.url + "/api/tuning", {"max_limit": 5})
            raise AssertionError("expected HTTP 405")
        except urllib.error.HTTPError as exc:
            assert exc.code == 405
    finally:
        dash.stop()


def test_snapshot_stable_under_concurrent_sampling():
    """snapshot() must never tear while the sampler thread mutates state."""
    gov = _make_governor()
    gov.start()
    errors: list[str] = []

    def reader() -> None:
        for _ in range(300):
            try:
                snap = gov.snapshot()
                json.dumps(snap)  # must stay serializable
                assert isinstance(snap["limit"], int)
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))
                return

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()
    for t in readers:
        t.join()
    gov.stop()
    assert errors == []


def _make_observer(script) -> ObserverAgent:
    cfg = GovernorConfig(dsn="fake", poll_interval_s=0.01)
    return ObserverAgent(cfg, FakeCohortSensor(script))


def test_throttle_endpoint_200_when_proceed():
    agent = _make_observer([(Level.GREEN, 0)])
    agent.start()
    dash = DashboardServer(agent, host="127.0.0.1", port=0)
    dash.start()
    try:
        threading.Event().wait(0.2)
        code, body = _get(dash.url + "/throttle")
        assert code == 200
        verdict = json.loads(body)
        assert verdict["throttle"] is False
        assert verdict["mode"] == "observe"
    finally:
        dash.stop()
        agent.stop()


def test_throttle_endpoint_429_when_throttle():
    # RED headroom drives the shadow limit down; migration cohort over it -> 429
    agent = _make_observer([(Level.RED, 5)])
    agent.start()
    dash = DashboardServer(agent, host="127.0.0.1", port=0)
    dash.start()
    try:
        threading.Event().wait(0.2)
        try:
            _get(dash.url + "/throttle")
            raise AssertionError("expected HTTP 429")
        except urllib.error.HTTPError as exc:
            assert exc.code == 429
            assert json.loads(exc.read())["throttle"] is True
    finally:
        dash.stop()
        agent.stop()


def test_throttle_endpoint_stays_open_under_auth():
    agent = _make_observer([(Level.GREEN, 0)])
    agent.start()
    dash = DashboardServer(agent, host="127.0.0.1", port=0, auth_token="s3cret")
    dash.start()
    try:
        threading.Event().wait(0.2)
        # /throttle must be reachable WITHOUT credentials (external tools poll it)
        code, _ = _get(dash.url + "/throttle")
        assert code == 200
        # but /api/state is still gated
        try:
            _get(dash.url + "/api/state")
            raise AssertionError("expected HTTP 401")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
    finally:
        dash.stop()
        agent.stop()


def test_throttle_endpoint_head_mirrors_status():
    agent = _make_observer([(Level.RED, 5)])
    agent.start()
    dash = DashboardServer(agent, host="127.0.0.1", port=0)
    dash.start()
    try:
        threading.Event().wait(0.2)
        req = urllib.request.Request(dash.url + "/throttle", method="HEAD")
        try:
            urllib.request.urlopen(req, timeout=3)
            raise AssertionError("expected HTTP 429 on HEAD")
        except urllib.error.HTTPError as exc:
            assert exc.code == 429
    finally:
        dash.stop()
        agent.stop()


def test_observer_dashboard_state_and_post_mode_405():
    agent = _make_observer([(Level.GREEN, 1)])
    agent.start()
    dash = DashboardServer(agent, host="127.0.0.1", port=0)
    dash.start()
    try:
        threading.Event().wait(0.2)
        _, body = _get(dash.url + "/api/state")
        state = json.loads(body)
        assert state["mode"] == "observe"
        assert "would_throttle_total" in state

        # an observer is OBSERVE-only: POST /api/mode is not allowed
        try:
            _post(dash.url + "/api/mode", {"mode": "enforce"})
            raise AssertionError("expected HTTP 405")
        except urllib.error.HTTPError as exc:
            assert exc.code == 405
    finally:
        dash.stop()
        agent.stop()


def test_throttle_endpoint_governor_fallback():
    # a plain Governor has no throttle_verdict(); the endpoint falls back to
    # snapshot() (limit<=0 == throttle). Green keeps limit>0 -> 200.
    gov = _make_governor(Mode.ENFORCE)
    gov.start()
    dash = DashboardServer(gov, host="127.0.0.1", port=0)
    dash.start()
    try:
        threading.Event().wait(0.2)
        code, body = _get(dash.url + "/throttle")
        assert code == 200
        assert "throttle" in json.loads(body)
    finally:
        dash.stop()
        gov.stop()
