"""``dbguard enforce`` \u2014 out-of-band, non-cooperative pacing you can WATCH.

This is the Stage 2 wedge: it governs a migration/backfill job that will **not**
cooperatively call ``/throttle``. It attaches to a live Postgres, attributes the
migration cohort, and when that cohort runs over the AIMD-allowed pace it cancels
the excess migration backends' in-flight statements (``pg_cancel_backend``) so a
batch loop retries more slowly \u2014 a capped concurrency the job never agreed to.

    # against your own DB (read-only sensor role + a privileged control role):
    dbguard enforce --dsn postgresql://gov_sensor:pw@host/app \
        --control-dsn postgresql://gov_control:pw@host/app \
        --migration-user backfill_job --report reports/enforce.json

    # or watch the bundled synthetic demo cancel a non-cooperative migration:
    docker compose up -d db
    python -m harness.enforce --demo

Enforcement is out-of-band: prod queries never pass through dbguard. Only
migration-cohort backends matched on a strong signal (role / query tag) are ever
cancelled; prod is never touched; an unreadable sensor fails OPEN (no cancels).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import webbrowser

import psycopg

from governor import (
    CohortClassifier,
    DashboardServer,
    EnforcerAgent,
    GovernorConfig,
    PostgresSensor,
)
from governor.attribution import DEFAULT_QUERY_TAG

from . import DSN
from .checkout_probe import CheckoutProbe

# Read-only probe used in --demo to measure checkout latency without writing.
_DEMO_PROBE_SQL = "SELECT status FROM orders WHERE id = (floor(random() * 5000) + 1)::int;"


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


def _uncapped_baseline(dsn: str, window_s: float = 8.0) -> dict:
    """Measure the 'no enforcement' damage: run the demo migration flat-out.

    Drives the synthetic migration uncapped for ``window_s`` seconds while a
    checkout probe + load monitor measure the resulting latency band and blocked
    sessions. This becomes the ghost baseline the live *enforced* run is drawn
    against \u2014 same DB, same data, same run, so the comparison is honest.
    """
    from .demo import LoadMonitor
    from .multicohort import start_demo_load

    probe = CheckoutProbe(dsn, probe_sql=_DEMO_PROBE_SQL)
    monitor = LoadMonitor(dsn)
    load = start_demo_load(dsn)
    probe.start()
    monitor.start()
    time.sleep(window_s)
    load.stop()
    monitor.stop()
    summary = probe.summary()
    probe.stop()
    return {
        "p50_ms": summary["p50_ms"],
        "p95_ms": summary["p95_ms"],
        "p99_ms": summary["p99_ms"],
        "max_ms": summary["max_ms"],
        "max_active_backends": monitor.max_active,
        "blocked_samples": monitor.blocked_samples,
    }


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dbguard enforce",
        description="Out-of-band, non-cooperative pacing of the migration cohort.",
    )
    p.add_argument(
        "--dsn",
        default=os.environ.get("GOV_DEMO_DSN", DSN),
        help="Postgres DSN for the read-only health sensor. Default: GOV_DEMO_DSN.",
    )
    p.add_argument(
        "--control-dsn",
        default=os.environ.get("GOV_CONTROL_DSN"),
        help="privileged DSN used ONLY to cancel migration backends "
        "(needs pg_signal_backend or the migration role). Defaults to --dsn.",
    )
    p.add_argument(
        "--migration-user",
        action="append",
        default=[],
        metavar="ROLE",
        help="DB role/usename that identifies the migration cohort (repeatable).",
    )
    p.add_argument(
        "--migration-app",
        action="append",
        default=[],
        metavar="APP_NAME",
        help="application_name that identifies the migration cohort (repeatable).",
    )
    p.add_argument(
        "--migration-tag",
        default=DEFAULT_QUERY_TAG,
        help=f"SQL comment tag marking migration queries (default: {DEFAULT_QUERY_TAG!r}).",
    )
    p.add_argument(
        "--max-cancels",
        type=int,
        default=2,
        metavar="N",
        help="max backends cancelled per poll (gradual pacing, default: 2).",
    )
    p.add_argument(
        "--allow-weak-signal",
        action="store_true",
        help="also cancel backends matched only on application_name (UNSAFE: "
        "spoofable). Off by default \u2014 only strong signals (role/tag) are actioned.",
    )
    p.add_argument("--host", default=os.environ.get("GOV_DASH_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("GOV_DASH_PORT", "8765")))
    p.add_argument(
        "--probe-query",
        default=None,
        metavar="SQL",
        help="read-only SQL run repeatedly to measure prod latency for the chart.",
    )
    p.add_argument(
        "--report",
        default=None,
        metavar="PATH",
        help="write a JSON evidence report here on exit.",
    )
    p.add_argument("--no-open", action="store_true", help="don't auto-open the browser")
    p.add_argument(
        "--demo",
        action="store_true",
        help="run the bundled synthetic non-cooperative migration and pace it.",
    )
    return p


def _evidence_report(agent: EnforcerAgent, classifier: CohortClassifier, baseline: dict | None) -> dict:
    snap = agent.snapshot(window=0, event_window=0)
    samples = snap.get("samples", [])
    mig_active = [s["in_flight"] for s in samples] or [0]
    cancel_events = sum(1 for e in snap.get("events", []) if e["kind"] == "cancel")
    report = {
        "mode": "enforce",
        "samples": snap.get("sample_count", 0),
        "peak_migration_active": max(mig_active),
        "cancels_total": snap.get("cancels_total", 0),
        "cancel_events_in_window": cancel_events,
        "enforcing_at_exit": snap.get("enforcing", True),
        "max_active_backends": snap.get("max_active_backends", 0),
        "blocked_sample_count": snap.get("blocked_sample_count", 0),
    }
    if baseline is not None:
        report["unenforced_baseline_p99_ms"] = baseline.get("p99_ms")
    report["attribution"] = {
        "migration_users": sorted(classifier.usenames),
        "migration_app_names": sorted(classifier.app_names),
        "migration_query_tag": classifier.query_tag,
    }
    return report


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    dsn = args.dsn

    demo_threads = None
    if args.demo:
        from .multicohort import DEMO_MIGRATION_APP

        if not args.migration_user and not args.migration_app:
            args.migration_app = [DEMO_MIGRATION_APP]

    classifier = CohortClassifier.from_lists(
        usenames=args.migration_user,
        app_names=args.migration_app,
        query_tag=args.migration_tag,
    )
    if classifier.is_empty:
        print(
            "error: no migration attribution configured. Pass at least one of\n"
            "  --migration-user ROLE / --migration-app APP_NAME / --migration-tag TAG",
            file=sys.stderr,
        )
        return 2

    print(f"waiting for database at {dsn} ...", flush=True)
    _wait_for_db(dsn)

    cfg = GovernorConfig(
        dsn=dsn,
        control_dsn=args.control_dsn,
        enforce_mechanism="cancel",
        max_cancels_per_interval=args.max_cancels,
        require_strong_signal=not args.allow_weak_signal,
    )
    probe_sql = args.probe_query
    baseline = None

    if args.demo:
        from .multicohort import seed_demo, start_demo_load

        if not probe_sql:
            probe_sql = _DEMO_PROBE_SQL
        print("seeding synthetic multi-squad data ...", flush=True)
        seed_demo(dsn)
        print("measuring unenforced baseline (migration flat-out, ~8s) ...", flush=True)
        baseline = _uncapped_baseline(dsn)
        print(
            f"  unenforced checkout p99 {baseline['p99_ms']}ms, "
            f"{baseline['blocked_samples']} blocked samples, "
            f"max {baseline['max_active_backends']} active backends.",
            flush=True,
        )
        demo_threads = start_demo_load(dsn)

    probe = CheckoutProbe(dsn, probe_sql=probe_sql) if probe_sql else None

    sensor = PostgresSensor(dsn, classifier=classifier)
    agent = EnforcerAgent(cfg, sensor)
    dashboard = DashboardServer(
        agent,
        host=args.host,
        port=args.port,
        extra_metrics=(lambda: probe.latency_payload()) if probe else None,
        baseline=baseline,
    )

    agent.start()
    if probe is not None:
        probe.start()
    dashboard.start()

    url = dashboard.url
    print("\n  dbguard enforce  [ENFORCE \u2014 out-of-band, paces non-cooperative jobs]")
    print(f"  -> dashboard: {url}")
    print(f"  -> throttle signal (gh-ost --throttle-http): {url}/throttle")
    mig = ", ".join(sorted(classifier.usenames) + sorted(classifier.app_names)) or "(tag only)"
    print(f"  migration cohort: {mig}  tag={classifier.query_tag!r}")
    print(
        f"  pacing via pg_cancel_backend (max {args.max_cancels}/poll, "
        f"{'strong signals only' if not args.allow_weak_signal else 'WEAK SIGNALS ALLOWED'}).\n"
    )
    if not os.environ.get("GOV_DASH_TOKEN") and args.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"  WARNING: bound to {args.host} with NO auth token. Set GOV_DASH_TOKEN before\n"
            "           exposing the dashboard beyond localhost (/throttle stays open by design).",
            flush=True,
        )
    print("  Ctrl-C to stop.\n", flush=True)
    if not args.no_open:
        try:
            webbrowser.open(url)
        except Exception:  # noqa: BLE001 - headless is fine
            pass

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nstopping ...", flush=True)
    finally:
        if demo_threads is not None:
            demo_threads.stop()
        if probe is not None:
            probe.stop()
        dashboard.stop()
        agent.stop()
        if args.report:
            import json

            report = _evidence_report(agent, classifier, baseline)
            with open(args.report, "w") as f:
                json.dump(report, f, indent=2)
            print("\n  evidence report:", flush=True)
            for k, v in report.items():
                if not isinstance(v, dict):
                    print(f"    {k}: {v}", flush=True)
            print(f"  full timeline -> {args.report}", flush=True)
        print("stopped.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
