"""End-to-end demo: reproduce 'backfill saturates the DB', then fix it.

Runs the same backfill twice against a synthetic Postgres while a checkout probe
measures customer-facing latency:

  1. UNGOVERNED  -> backfill runs flat-out, checkout p99 spikes.
  2. GOVERNED    -> backfill paced by the runtime governor, checkout p99 stays flat.

Prints a side-by-side scorecard and writes JSON reports to ./reports/.
"""

from __future__ import annotations

import os
import threading
import time

import psycopg

from governor import GovernorConfig, Governor, Mode, PostgresSensor
from governor.config import Thresholds

from . import DSN
from .backfill import run_backfill
from .checkout_probe import CheckoutProbe
from .plot import render_latency_plot
from .seed import seed

REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")


def _wait_for_db(dsn: str, timeout_s: float = 60.0) -> None:
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, connect_timeout=3):
                return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.0)
    raise RuntimeError(f"database not reachable at {dsn}: {last_err}")


class LoadMonitor:
    """Independently samples DB load so both scenarios are measured identically."""

    def __init__(self, dsn: str, interval_s: float = 0.25) -> None:
        self._sensor = PostgresSensor(dsn)
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.max_active = 0
        self.blocked_samples = 0

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._sensor.close()

    def _run(self) -> None:
        while not self._stop.is_set():
            hr = self._sensor.sample()
            self.max_active = max(self.max_active, hr.active_backends)
            if hr.blocked_backends > 0:
                self.blocked_samples += 1
            self._stop.wait(self._interval)


def _run_scenario(label: str, governor: Governor | None) -> dict:
    monitor = LoadMonitor(DSN)
    probe = CheckoutProbe(DSN)
    monitor.start()
    probe.start()
    if governor is not None:
        governor.start()

    t0 = time.perf_counter()
    run_backfill(DSN, governor=governor)
    duration = time.perf_counter() - t0

    if governor is not None:
        governor.stop()
    probe.stop()
    monitor.stop()

    result = probe.summary()
    result.update(
        label=label,
        backfill_seconds=round(duration, 1),
        max_active_backends=monitor.max_active,
        blocked_samples=monitor.blocked_samples,
        series=probe.series,
    )
    if governor is not None:
        result["backoff_events"] = governor.report.backoff_events()
        result["circuit_break_events"] = governor.report.pause_events()
        result["would_throttle_events"] = sum(
            1 for e in governor.report.events if e.kind == "would_throttle"
        )
    return result


def _vacuum() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("VACUUM (ANALYZE) projects;")


def main() -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    print("waiting for database ...", flush=True)
    _wait_for_db(DSN)
    seed(DSN)

    print("\n=== Scenario A: UNGOVERNED backfill ===", flush=True)
    ungoverned = _run_scenario("ungoverned", governor=None)
    print(_brief(ungoverned), flush=True)

    _vacuum()

    print("\n=== Scenario B: OBSERVE mode (no throttling, just flags risk) ===", flush=True)
    obs_cfg = GovernorConfig(dsn=DSN, thresholds=Thresholds())
    obs_gov = Governor(obs_cfg, PostgresSensor(DSN), mode=Mode.OBSERVE)
    observe = _run_scenario("observe", governor=obs_gov)
    obs_gov.report.write_json(os.path.join(REPORTS_DIR, "observe_timeline.json"))
    print(_brief(observe), flush=True)
    print(
        f"  -> OBSERVE would have throttled {observe.get('would_throttle_events', 0)} batches "
        f"(zero risk: it changed nothing this run).",
        flush=True,
    )

    _vacuum()

    print("\n=== Scenario C: GOVERNED backfill (ENFORCE) ===", flush=True)
    cfg = GovernorConfig(dsn=DSN, thresholds=Thresholds())
    gov = Governor(cfg, PostgresSensor(DSN), mode=Mode.ENFORCE)
    governed = _run_scenario("governed", governor=gov)
    gov.report.write_json(os.path.join(REPORTS_DIR, "governed_timeline.json"))
    print(_brief(governed), flush=True)

    _print_scorecard(ungoverned, governed, observe)
    _maybe_plot(ungoverned, governed, observe)


def _brief(d: dict) -> dict:
    """Result dict without the bulky series, for printing."""
    return {k: v for k, v in d.items() if k != "series"}


def _maybe_plot(*results: dict) -> None:
    series_by_label = {r["label"]: r.get("series", []) for r in results}
    out = os.path.join(REPORTS_DIR, "checkout_latency.png")
    if render_latency_plot(series_by_label, out):
        print(f"\npitch graph written to {out}", flush=True)
    else:
        print(
            "\n(install matplotlib for the pitch graph: pip install -e '.[plot]')",
            flush=True,
        )


def _print_scorecard(a: dict, b: dict, observe: dict | None = None) -> None:
    rows = [
        ("checkout p50 (ms)", a["p50_ms"], b["p50_ms"]),
        ("checkout p95 (ms)", a["p95_ms"], b["p95_ms"]),
        ("checkout p99 (ms)", a["p99_ms"], b["p99_ms"]),
        ("checkout max (ms)", a["max_ms"], b["max_ms"]),
        ("max DB active backends", a["max_active_backends"], b["max_active_backends"]),
        ("blocked samples", a["blocked_samples"], b["blocked_samples"]),
        ("backfill seconds", a["backfill_seconds"], b["backfill_seconds"]),
    ]
    width = 28
    print("\n" + "=" * 64)
    print("SCORECARD".center(64))
    print("=" * 64)
    print(f"{'metric':<{width}}{'UNGOVERNED':>16}{'GOVERNED':>16}")
    print("-" * 64)
    for name, av, bv in rows:
        print(f"{name:<{width}}{av:>16}{bv:>16}")
    print("-" * 64)
    if a["p99_ms"] > 0:
        factor = a["p99_ms"] / b["p99_ms"] if b["p99_ms"] else float("inf")
        print(f"checkout p99 improvement: {factor:.1f}x lower under the governor")
    print(
        f"governor backed off {b.get('backoff_events', 0)} times, "
        f"circuit-broke {b.get('circuit_break_events', 0)} times"
    )
    if observe is not None:
        print(
            f"OBSERVE mode flagged {observe.get('would_throttle_events', 0)} risky batches "
            f"with zero behavior change"
        )
    print("=" * 64)


if __name__ == "__main__":
    main()
