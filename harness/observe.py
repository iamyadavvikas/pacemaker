"""``dbguard observe`` — standalone, read-only DB guardrail you can WATCH.

This is the product wedge as a sidecar. It attaches to a live Postgres with a
read-only role, attributes backends into a *migration* cohort vs *prod* traffic,
and runs the AIMD pacing policy **in shadow** — recording exactly when it *would*
have throttled the migration, while never blocking, cancelling, or touching the
database. It serves the live dashboard plus a gh-ost/pt-osc-compatible
``/throttle`` endpoint, and writes an evidence report on exit.

    # against your own DB, read-only role, observe-only:
    dbguard observe --dsn postgresql://gov_sensor:pw@host:5432/app \
        --migration-user backfill_job \
        --report reports/observe.json

    # or drive the bundled synthetic multi-squad demo:
    docker compose up -d db
    python -m harness.observe --demo

Everything here is read-only and side-effect free against the target DB.
The migration tool (or app job) consults ``GET /throttle`` (HTTP 200 = proceed,
429 = back off) — in OBSERVE the verdict is advisory; the dashboard shows what it
*would* have signalled.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
import webbrowser

import psycopg

from governor import (
    CohortClassifier,
    DashboardServer,
    GovernorConfig,
    ObserverAgent,
    PostgresSensor,
)
from governor.attribution import DEFAULT_QUERY_TAG

from . import DSN
from .checkout_probe import CheckoutProbe


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


# Read-only probe used in --demo to measure checkout latency without writing to
# the target DB (the observer must stay side-effect free).
_DEMO_PROBE_SQL = "SELECT status FROM orders WHERE id = (floor(random() * 5000) + 1)::int;"


def _throttle_episodes(samples: list[dict]) -> int:
    """Count distinct throttle *episodes* (rising edges), not per-sample hits.

    A sample is "throttling" when the migration cohort is at/over the shadow
    limit (or the limit has paused at 0). Consecutive throttling samples form one
    continuous episode, so we count only the transitions into throttling. This is
    the honest "we would have stepped in N times" number, vs the raw ~4/s sample
    count that ``would_throttle_total`` accumulates.
    """
    episodes = 0
    prev = False
    for s in samples:
        limit = s.get("limit", 0)
        throttling = limit == 0 or s.get("in_flight", 0) >= limit
        if throttling and not prev:
            episodes += 1
        prev = throttling
    return episodes


def _demo_counterfactual(
    dsn: str,
    cfg: GovernorConfig,
    classifier: CohortClassifier,
    window_s: float = 8.0,
) -> dict:
    """Measured A/B for --demo: uncapped migration vs migration capped to pace.

    Phase A runs the synthetic migration flat-out while a shadow observer + load
    monitor + read-only checkout probe measure the damage AND capture the safe
    pace the policy converges to. Phase B re-runs the migration capped to that
    pace and measures checkout latency again. Both p99s are *measured* on the
    same DB in the same run, so the counterfactual ("had we paced it, checkout
    p99 would have been X not Y") is honest, not modelled. This is legitimate
    only because we own the synthetic migration workers; against a real target DB
    we never cap the customer's migration and so report correlation only.
    """
    from .demo import LoadMonitor
    from .multicohort import start_demo_load

    # Phase A: uncapped migration + shadow observer (capture safe pace + damage).
    probe_a = CheckoutProbe(dsn, probe_sql=_DEMO_PROBE_SQL)
    monitor = LoadMonitor(dsn)
    agent = ObserverAgent(cfg, PostgresSensor(dsn, classifier=classifier))
    load_a = start_demo_load(dsn)
    probe_a.start()
    monitor.start()
    agent.start()
    time.sleep(window_s)
    load_a.stop()
    agent.stop()
    monitor.stop()
    uncapped = probe_a.summary()
    probe_a.stop()

    limits = [s["limit"] for s in agent.snapshot(window=0)["samples"] if s["limit"] > 0]
    safe_pace = min(limits) if limits else cfg.min_limit

    # Phase B: re-run the migration capped to the recommended pace.
    probe_b = CheckoutProbe(dsn, probe_sql=_DEMO_PROBE_SQL)
    load_b = start_demo_load(dsn, n_migration_workers=safe_pace)
    probe_b.start()
    time.sleep(window_s)
    load_b.stop()
    capped = probe_b.summary()
    probe_b.stop()

    return {
        "safe_pace": safe_pace,
        "uncapped_p99_ms": uncapped["p99_ms"],
        "capped_p99_ms": capped["p99_ms"],
        "uncapped_p95_ms": uncapped["p95_ms"],
        "capped_p95_ms": capped["p95_ms"],
        # ghost-band baseline for the dashboard (uncapped = the "no pacing" profile):
        "baseline": {
            "p50_ms": uncapped["p50_ms"],
            "p95_ms": uncapped["p95_ms"],
            "p99_ms": uncapped["p99_ms"],
            "max_ms": uncapped["max_ms"],
            "max_active_backends": monitor.max_active,
            "blocked_samples": monitor.blocked_samples,
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dbguard observe",
        description="Read-only, observe-only DB guardrail: shadow-pace the migration cohort.",
    )
    p.add_argument(
        "--dsn",
        default=os.environ.get("GOV_DEMO_DSN", DSN),
        help="Postgres DSN (use a read-only role in production). Default: GOV_DEMO_DSN.",
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
    p.add_argument("--host", default=os.environ.get("GOV_DASH_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("GOV_DASH_PORT", "8765")))
    p.add_argument(
        "--probe-query",
        default=None,
        metavar="SQL",
        help="read-only SQL run repeatedly to measure prod latency (e.g. a cheap "
        "SELECT on a hot table). Renders the latency chart against a real target; "
        "--demo supplies a default read-only probe.",
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
        help="run the bundled synthetic multi-squad load (migration vs checkout) to watch.",
    )
    return p


def _evidence_report(
    agent: ObserverAgent,
    classifier: CohortClassifier,
    counterfactual: dict | None = None,
    probe_summary: dict | None = None,
) -> dict:
    """Compact, human-readable summary of what the observer would have done.

    The leave-behind for a 'would this have helped?' conversation: peak migration
    concurrency, how many distinct throttle *episodes* we'd have triggered (not
    the raw per-sample count), the worst headroom seen, the safe pace the policy
    converged on, the measured prod-checkout latency, and — in --demo — a
    measured A/B counterfactual. Against a real target we never cap the
    migration, so that section is labelled correlation-only.
    """
    snap = agent.snapshot(window=0, event_window=0)
    samples = snap.get("samples", [])
    mig_active = [s["in_flight"] for s in samples] or [0]
    limits = [s["limit"] for s in samples] or [0]
    report = {
        "mode": "observe",
        "samples": snap.get("sample_count", 0),
        "peak_migration_active": max(mig_active),
        "would_throttle_samples": snap.get("would_throttle_total", 0),
        "would_throttle_episodes": _throttle_episodes(samples),
        "max_active_backends": snap.get("max_active_backends", 0),
        "blocked_sample_count": snap.get("blocked_sample_count", 0),
        "projected_safe_pace": min(limits) if any(limits) else 0,
    }
    if probe_summary is not None:
        report["measured_checkout"] = {
            "p50_ms": probe_summary["p50_ms"],
            "p95_ms": probe_summary["p95_ms"],
            "p99_ms": probe_summary["p99_ms"],
        }
    if counterfactual is not None:
        report["counterfactual"] = {
            "kind": "measured_ab",
            "safe_pace": counterfactual["safe_pace"],
            "uncapped_p99_ms": counterfactual["uncapped_p99_ms"],
            "capped_p99_ms": counterfactual["capped_p99_ms"],
        }
    else:
        report["counterfactual"] = {
            "kind": "correlation_only",
            "note": "real target: measured prod latency + would-throttle markers only; "
            "the migration was never capped, so there is no measured A/B counterfactual.",
        }
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
        # synthetic load is generated with a known application_name + query tag,
        # so the default classifier attributes it without any extra flags even
        # though it shares the demo DB role with the prod-traffic cohort.
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

    cfg = GovernorConfig(dsn=dsn)
    probe_sql = args.probe_query
    counterfactual = None
    baseline = None

    if args.demo:
        from .multicohort import seed_demo, start_demo_load

        if not probe_sql:
            probe_sql = _DEMO_PROBE_SQL
        print("seeding synthetic multi-squad data ...", flush=True)
        seed_demo(dsn)
        print("measuring counterfactual (uncapped vs paced migration, ~16s) ...", flush=True)
        counterfactual = _demo_counterfactual(dsn, cfg, classifier)
        baseline = counterfactual["baseline"]
        print(
            f"  uncapped checkout p99 {counterfactual['uncapped_p99_ms']}ms"
            f" -> paced to {counterfactual['safe_pace']} concurrent:"
            f" {counterfactual['capped_p99_ms']}ms.",
            flush=True,
        )
        demo_threads = start_demo_load(dsn)

    probe = CheckoutProbe(dsn, probe_sql=probe_sql) if probe_sql else None

    sensor = PostgresSensor(dsn, classifier=classifier)
    agent = ObserverAgent(cfg, sensor)
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
    print("\n  dbguard observe  [OBSERVE — read-only, never throttles]")
    print(f"  -> dashboard: {url}")
    print(f"  -> throttle signal (gh-ost --throttle-http): {url}/throttle")
    mig = ", ".join(sorted(classifier.usenames) + sorted(classifier.app_names)) or "(tag only)"
    print(f"  migration cohort: {mig}  tag={classifier.query_tag!r}\n")
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
            report = _evidence_report(
                agent,
                classifier,
                counterfactual=counterfactual,
                probe_summary=probe.summary() if probe else None,
            )
            agent.report.write_json(args.report)
            print("\n  evidence report:", flush=True)
            for k, v in report.items():
                if not isinstance(v, dict):
                    print(f"    {k}: {v}", flush=True)
            cf = report.get("counterfactual", {})
            if cf.get("kind") == "measured_ab":
                print(
                    f"    counterfactual: uncapped p99 {cf['uncapped_p99_ms']}ms"
                    f" -> paced({cf['safe_pace']}) p99 {cf['capped_p99_ms']}ms",
                    flush=True,
                )
            mc = report.get("measured_checkout")
            if mc:
                print(f"    measured checkout p99: {mc['p99_ms']}ms", flush=True)
            print(f"  full timeline -> {args.report}", flush=True)
        print("stopped.", flush=True)
    return 0


def cli() -> int:
    """Console entry point: ``dbguard <subcommand>`` (``observe`` | ``enforce``)."""
    argv = sys.argv[1:]
    if argv and argv[0] == "observe":
        return main(argv[1:])
    if argv and argv[0] == "enforce":
        from .enforce import main as enforce_main

        return enforce_main(argv[1:])
    if argv and argv[0] in ("-h", "--help", "help"):
        print(
            "usage:\n"
            "  dbguard observe [options]   read-only shadow guardrail "
            "(see: dbguard observe --help)\n"
            "  dbguard enforce [options]   out-of-band non-cooperative pacing "
            "(see: dbguard enforce --help)"
        )
        return 0
    # default to observe so `dbguard --dsn ...` works
    return main(argv)


if __name__ == "__main__":
    sys.exit(main())
