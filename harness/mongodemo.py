"""Watchable MongoDB demo: a governed Mongo backfill you can WATCH in the browser.

The Mongo counterpart to ``harness.live`` / ``dbguard observe --demo``. It drives
two synthetic workloads against a live MongoDB:

- a **migration cohort**: several workers rewriting documents in ``projects`` in
  batches, each op carrying a ``comment="dbguard:migration"`` and connecting with
  ``appname='dbguard_demo_migration'`` so the default classifier attributes it;
- a **prod cohort**: light, steady reads/updates on ``orders`` sharing the same
  user, separated only by signal (comment / appName).

A read-only ``MongoSensor`` shadow-paces the migration via the same AIMD policy
and serves the live dashboard + gh-ost/pt-osc ``/throttle`` endpoint.

    docker compose up -d mongo
    python -m harness.mongodemo            # OBSERVE (read-only, never throttles)

Then open the printed URL (default http://127.0.0.1:8765).

Requires ``pymongo`` (``pip install -e '.[mongo]'``). Synthetic data only.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time
import webbrowser

from governor import (
    CohortClassifier,
    DashboardServer,
    EnforcerAgent,
    GovernorConfig,
    MongoKiller,
    MongoSensor,
    ObserverAgent,
    Thresholds,
)

MONGO_URI = os.environ.get("GOV_DEMO_MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("GOV_DEMO_MONGO_DB", "govdemo")

DEMO_MIGRATION_APP = "dbguard_demo_migration"
DEMO_PROD_APP = "dbguard_demo_checkout"
MIGRATION_COMMENT = "dbguard:migration"

_N_PROJECTS = 50_000
_N_ORDERS = 5_000
_N_MIGRATION_WORKERS = 8
_MIGRATION_BATCH = 500


def _wait_for_mongo(uri: str, timeout_s: float = 60.0) -> None:
    from pymongo import MongoClient

    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            client.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.0)
    raise RuntimeError(f"mongo not reachable at {uri}: {last_err}")


def seed(uri: str, db_name: str) -> None:
    """(Re)seed synthetic ``projects`` and ``orders`` collections (idempotent)."""
    from pymongo import MongoClient

    client = MongoClient(uri)
    try:
        db = client[db_name]
        if db.projects.estimated_document_count() < _N_PROJECTS:
            db.projects.drop()
            db.projects.insert_many(
                ({"_id": i, "state": "v2", "data": "x" * 32} for i in range(_N_PROJECTS)),
                ordered=False,
            )
        if db.orders.estimated_document_count() < _N_ORDERS:
            db.orders.drop()
            db.orders.insert_many(
                ({"_id": i, "status": "open"} for i in range(_N_ORDERS)),
                ordered=False,
            )
    finally:
        client.close()


class DemoLoad:
    """Handle for the running synthetic Mongo workload; call ``stop()`` to end it."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    @property
    def stop_event(self) -> threading.Event:
        return self._stop

    def add(self, thread: threading.Thread) -> None:
        self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=10)


def _migration_worker(uri: str, db_name: str, stop: threading.Event, start_id: int) -> None:
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure, PyMongoError

    client = MongoClient(uri, appname=DEMO_MIGRATION_APP)
    try:
        coll = client[db_name].projects
        lo = start_id
        while not stop.is_set():
            hi = lo + _MIGRATION_BATCH
            try:
                # comment marks this op as the migration cohort for attribution.
                coll.update_many(
                    {"_id": {"$gte": lo, "$lt": hi}},
                    {"$set": {"state": "v3"}},
                    comment=MIGRATION_COMMENT,
                )
            except OperationFailure as exc:
                # In enforce mode the governor kills our op (killOp -> code 11601
                # "Interrupted"). That is the whole point of the demo, so back off
                # briefly and retry rather than crashing the worker thread.
                if exc.code == 11601:
                    stop.wait(0.1)
                    continue
                raise
            except PyMongoError:
                # Transient connection churn during a killOp storm; pause and retry.
                stop.wait(0.1)
                continue
            lo = hi if hi < _N_PROJECTS else 0
    finally:
        client.close()


def _prod_worker(uri: str, db_name: str, stop: threading.Event) -> None:
    from pymongo import MongoClient

    client = MongoClient(uri, appname=DEMO_PROD_APP)
    try:
        coll = client[db_name].orders
        while not stop.is_set():
            oid = random.randint(0, _N_ORDERS - 1)
            coll.find_one({"_id": oid})
            coll.update_one({"_id": oid}, {"$set": {"touched_at": time.time()}})
            stop.wait(0.01)
    finally:
        client.close()


def start_demo_load(uri: str, db_name: str, n_migration_workers: int | None = None) -> DemoLoad:
    workers = _N_MIGRATION_WORKERS if n_migration_workers is None else max(1, n_migration_workers)
    load = DemoLoad()
    stop = load.stop_event
    stride = max(1, _N_PROJECTS // workers)
    for i in range(workers):
        t = threading.Thread(
            target=_migration_worker,
            args=(uri, db_name, stop, i * stride),
            name=f"mongo-migration-{i}",
            daemon=True,
        )
        load.add(t)
        t.start()
    prod = threading.Thread(
        target=_prod_worker, args=(uri, db_name, stop), name="mongo-prod", daemon=True
    )
    load.add(prod)
    prod.start()
    return load


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="harness.mongodemo",
        description="Watchable MongoDB observe demo (read-only, synthetic load).",
    )
    p.add_argument("--uri", default=MONGO_URI, help="MongoDB connection URI.")
    p.add_argument("--db", default=DB_NAME, help="demo database name.")
    p.add_argument("--host", default=os.environ.get("GOV_DASH_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("GOV_DASH_PORT", "8765")))
    p.add_argument("--no-open", action="store_true", help="don't auto-open the browser")
    p.add_argument("--no-load", action="store_true", help="don't generate synthetic load")
    p.add_argument(
        "--mode",
        choices=("observe", "enforce"),
        default="observe",
        help="observe = read-only shadow (never kills); enforce = killOp the migration "
        "cohort to hold headroom (synthetic data only). Default: observe.",
    )
    from .notifiers import add_notifier_args

    add_notifier_args(p)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        import pymongo  # noqa: F401
    except ImportError:
        print("error: pymongo is required. Install with: pip install -e '.[mongo]'", file=sys.stderr)
        return 2

    print(f"waiting for mongo at {args.uri} ...", flush=True)
    _wait_for_mongo(args.uri)
    print("seeding synthetic data ...", flush=True)
    seed(args.uri, args.db)

    # Demo thresholds: a small Mongo saturates quickly, and the probe latency /
    # connection signals matter as much as raw op count.
    thresholds = Thresholds(
        green_max_active=2,
        yellow_max_active=3,
        critical_active=6,
        yellow_query_latency_ms=15,
        red_query_latency_ms=40,
        critical_query_latency_ms=120,
    )
    classifier = CohortClassifier.from_lists(
        app_names=[DEMO_MIGRATION_APP], query_tag=MIGRATION_COMMENT
    )
    cfg = GovernorConfig(dsn=args.uri, pause_on_critical=False)
    sensor = MongoSensor(uri=args.uri, thresholds=thresholds, classifier=classifier)

    from .notifiers import build_notifier

    notifier = build_notifier(args)
    if args.mode == "enforce":
        agent = EnforcerAgent(
            cfg, sensor, canceller=MongoKiller(uri=args.uri), notifier=notifier
        )
        mode_banner = "ENFORCE — killOp the migration cohort to hold headroom"
    else:
        agent = ObserverAgent(cfg, sensor, notifier=notifier)
        mode_banner = "OBSERVE — read-only, never throttles"
    dashboard = DashboardServer(agent, host=args.host, port=args.port)

    load = None if args.no_load else start_demo_load(args.uri, args.db)
    agent.start()
    dashboard.start()

    url = dashboard.url
    print(f"\n  harness.mongodemo  [{mode_banner}]")
    print(f"  -> dashboard: {url}")
    print(f"  -> throttle signal (gh-ost --throttle-http): {url}/throttle")
    print(f"  migration cohort: appName={DEMO_MIGRATION_APP!r} comment={MIGRATION_COMMENT!r}")
    if args.mode == "enforce":
        print("  enforce mode: prod ops are never killed (only the migration cohort). Ctrl-C to stop.\n", flush=True)
    else:
        print("  throttle-only mode (never fully pauses). Ctrl-C to stop.\n", flush=True)
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
        if load is not None:
            load.stop()
        dashboard.stop()
        agent.stop()
        print("stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
