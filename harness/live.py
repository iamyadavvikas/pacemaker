"""Watchable live demo: a real governed backfill you can WATCH in the browser.

Unlike ``harness.demo`` (which runs three short scenarios and exits), this keeps
a governed backfill running in a loop against synthetic Postgres while serving
the live dashboard, so you can open it and watch the governor react in real time.

    docker compose up -d db
    python -m harness.live                 # ENFORCE (default)
    python -m harness.live --mode observe   # OBSERVE: logs would-throttle, never blocks

Then open the printed URL (default http://127.0.0.1:8765).

Everything here uses synthetic, generated data only.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
import webbrowser

import psycopg

from governor import DashboardServer, Governor, GovernorConfig, Mode, PostgresSensor

from . import DSN
from .backfill import N_WORKERS, run_backfill
from .checkout_probe import CheckoutProbe
from .seed import N_PROJECTS, seed


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


def _needs_seed(dsn: str) -> bool:
    """True unless ``projects`` already exists with the expected row count."""
    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM projects;")
            (n,) = cur.fetchone()
            return n < N_PROJECTS
    except Exception:  # noqa: BLE001 - table missing / not seeded yet
        return True


def _latency_payload(probe: CheckoutProbe, window_s: float = 40.0, buckets: int = 120) -> dict:
    """Bucketed rolling p95 of recent checkout latency, for the live chart."""
    return probe.latency_payload(window_s=window_s, buckets=buckets)


def _backfill_loop(stop: threading.Event, governor: Governor | None, n_workers: int) -> None:
    """Keep the backfill running (one pass at a time) until asked to stop."""
    while not stop.is_set():
        run_backfill(DSN, governor=governor, n_workers=n_workers, passes=1)


def _calibrate(n_workers: int, window_s: float = 8.0) -> dict:
    """Run a short UNGOVERNED warmup to capture an honest 'no governor' baseline.

    Drives the backfill flat-out (no governor) for ``window_s`` seconds while a
    checkout probe + load monitor measure the damage. The resulting latency band
    and blocked-session count become the ghost baseline the live governed run is
    compared against — same machine, same data, same run, so it can't be argued
    away as cherry-picked.
    """
    from .demo import LoadMonitor  # local import keeps module load light

    probe = CheckoutProbe(DSN)
    monitor = LoadMonitor(DSN)
    stop = threading.Event()
    probe.start()
    monitor.start()
    worker = threading.Thread(
        target=_backfill_loop, args=(stop, None, n_workers), name="calib-backfill", daemon=True
    )
    worker.start()
    time.sleep(window_s)
    stop.set()
    worker.join(timeout=10)
    summary = probe.summary()
    probe.stop()
    monitor.stop()
    return {
        "p50_ms": summary["p50_ms"],
        "p95_ms": summary["p95_ms"],
        "p99_ms": summary["p99_ms"],
        "max_ms": summary["max_ms"],
        "max_active_backends": monitor.max_active,
        "blocked_samples": monitor.blocked_samples,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Live governor dashboard + looping backfill.")
    parser.add_argument(
        "--mode",
        choices=["enforce", "observe"],
        default=os.environ.get("GOV_MODE", "enforce"),
        help="enforce = actually throttle; observe = log would-throttle only (default: enforce)",
    )
    parser.add_argument("--host", default=os.environ.get("GOV_DASH_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("GOV_DASH_PORT", "8765")))
    parser.add_argument("--workers", type=int, default=N_WORKERS)
    parser.add_argument("--no-open", action="store_true", help="don't auto-open the browser")
    parser.add_argument(
        "--no-calibrate",
        action="store_true",
        help="skip the ~8s ungoverned warmup that captures the ghost baseline",
    )
    args = parser.parse_args(argv)

    mode = Mode.OBSERVE if args.mode == "observe" else Mode.ENFORCE

    print(f"waiting for database at {DSN} ...", flush=True)
    _wait_for_db(DSN)
    if _needs_seed(DSN):
        print("seeding synthetic data (first run) ...", flush=True)
        seed(DSN)
    else:
        print("data already seeded, skipping.", flush=True)

    baseline = None
    if not args.no_calibrate:
        print("calibrating ungoverned baseline (~8s, expect a latency spike) ...", flush=True)
        baseline = _calibrate(args.workers)
        print(
            f"  ungoverned baseline: checkout p99 {baseline['p99_ms']}ms, "
            f"{baseline['blocked_samples']} blocked samples, "
            f"max {baseline['max_active_backends']} active backends.",
            flush=True,
        )

    governor = Governor(GovernorConfig(dsn=DSN), PostgresSensor(DSN), mode=mode)
    probe = CheckoutProbe(DSN)
    dashboard = DashboardServer(
        governor,
        host=args.host,
        port=args.port,
        extra_metrics=lambda: _latency_payload(probe),
        baseline=baseline,
    )

    stop = threading.Event()
    governor.start()
    probe.start()
    dashboard.start()
    backfill = threading.Thread(
        target=_backfill_loop, args=(stop, governor, args.workers), name="backfill-loop", daemon=True
    )
    backfill.start()

    url = dashboard.url
    print(f"\n  pacing-governor live dashboard  [{mode.value.upper()}]")
    print(f"  -> {url}\n")
    if os.environ.get("GOV_DASH_TOKEN"):
        print("  auth: HTTP Basic enabled (password = GOV_DASH_TOKEN; any username).")
    elif args.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"  WARNING: bound to {args.host} with NO auth token. The mode toggle is an\n"
            "           open control endpoint — set GOV_DASH_TOKEN before exposing this.",
            flush=True,
        )
    print("  Ctrl-C to stop.\n", flush=True)
    if not args.no_open:
        try:
            webbrowser.open(url)
        except Exception:  # noqa: BLE001 - headless / no browser is fine
            pass

    def _handle_sigterm(_signum, _frame):
        stop.set()

    # signal handlers only register from the main thread; ignore otherwise
    # (e.g. when main() is embedded/driven from a worker thread in tests).
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
    except ValueError:
        pass

    try:
        while not stop.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nstopping ...", flush=True)
    finally:
        stop.set()
        backfill.join(timeout=10)
        dashboard.stop()
        probe.stop()
        governor.stop()
        print("stopped.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
