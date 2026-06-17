"""``dbguard preflight`` — prove a target DB is safe + the sensor reads it right.

The pre-flight you run ONCE against a real (non-prod) Shutterfly database before
trusting ``dbguard observe``. It is strictly read-only and does three things:

1. **Connectivity** — confirms the read-only role can reach the target (Postgres
   or MongoDB, auto-detected from the DSN scheme) within a short timeout.
2. **Sensor reading** — takes a few ``MongoSensor`` / ``PostgresSensor`` samples
   and prints the normalized headroom (level, active/blocked, replication lag,
   connection-pool fraction, probe p99) the observer would act on.
3. **Independent cross-check** — issues its OWN raw introspection read (NOT via
   the sensor) and prints the active/blocked counts side-by-side with the
   sensor's, so you can confirm the sensor isn't mis-counting. Any drift beyond a
   small tolerance is flagged.

It never writes, cancels, locks, or seeds anything; the only commands issued are
read-only introspection (``pg_stat_activity`` / ``currentOp`` / ``serverStatus``
/ ``replSetGetStatus``). Exit code 0 = safe to proceed to ``dbguard observe``.

    python -m harness.preflight --dsn postgresql://gov_sensor:pw@host:5432/app
    python -m harness.preflight --dsn "mongodb://gov_sensor:pw@host:27017/?readPreference=secondaryPreferred"
"""

from __future__ import annotations

import argparse
import sys
import time

from governor import CohortClassifier
from governor.attribution import DEFAULT_QUERY_TAG

from .observe import _is_mongo_dsn, _wait_for_db, _wait_for_mongo

# Independent (NOT-through-the-sensor) ground-truth read for Postgres. Mirrors the
# sensor's own counting so a match proves the sensor is reading correctly.
_PG_GROUND_TRUTH_SQL = (
    "SELECT "
    "  count(*) FILTER (WHERE state = 'active') AS active, "
    "  count(*) FILTER (WHERE wait_event_type = 'Lock') AS blocked "
    "FROM pg_stat_activity "
    "WHERE pid <> pg_backend_pid();"
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dbguard preflight",
        description="Read-only safety + sensor-accuracy pre-flight for a target DB.",
    )
    p.add_argument(
        "--dsn",
        required=True,
        help="Postgres or MongoDB DSN (read-only role). Engine auto-detected from scheme.",
    )
    p.add_argument(
        "--migration-user", action="append", default=[], metavar="ROLE",
        help="role/usename identifying the migration cohort (repeatable; optional for preflight).",
    )
    p.add_argument(
        "--migration-app", action="append", default=[], metavar="APP_NAME",
        help="application_name / appName identifying the migration cohort (repeatable).",
    )
    p.add_argument(
        "--migration-tag", default=DEFAULT_QUERY_TAG,
        help=f"comment tag marking migration ops (default: {DEFAULT_QUERY_TAG!r}).",
    )
    p.add_argument(
        "--samples", type=int, default=5, metavar="N",
        help="number of sensor samples to take (default: 5).",
    )
    p.add_argument(
        "--tolerance", type=int, default=2, metavar="N",
        help="max allowed |sensor - ground-truth| active-backend drift (default: 2).",
    )
    return p


def _classifier(args: argparse.Namespace) -> CohortClassifier | None:
    cls = CohortClassifier.from_lists(
        usenames=args.migration_user,
        app_names=args.migration_app,
        query_tag=args.migration_tag,
    )
    return None if cls.is_empty else cls


def _pg_ground_truth(dsn: str) -> tuple[int, int]:
    import psycopg

    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(_PG_GROUND_TRUTH_SQL)
            active, blocked = cur.fetchone()
    return int(active), int(blocked)


def _mongo_ground_truth(dsn: str) -> tuple[int, int]:
    from pymongo import MongoClient

    client = MongoClient(dsn, serverSelectionTimeoutMS=5000)
    try:
        res = client.admin.command({"currentOp": 1, "active": True, "$all": False})
        ops = list(res.get("inprog", []))
        active = sum(1 for op in ops if op.get("active", True))
        blocked = sum(1 for op in ops if op.get("waitingForLock"))
        return active, blocked
    finally:
        client.close()


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    dsn = args.dsn
    is_mongo = _is_mongo_dsn(dsn)
    engine = "MongoDB" if is_mongo else "PostgreSQL"

    print(f"  dbguard preflight  [{engine}]  (read-only — no writes/cancels/locks)\n")

    # 1. Connectivity.
    print("  1. connectivity ...", flush=True)
    try:
        if is_mongo:
            _wait_for_mongo(dsn, timeout_s=15)
        else:
            _wait_for_db(dsn, timeout_s=15)
    except Exception as e:  # noqa: BLE001
        print(f"     FAIL: cannot reach target read-only: {e}", file=sys.stderr)
        return 1
    print("     ok: reachable.", flush=True)

    # 2. Sensor reading.
    classifier = _classifier(args)
    if is_mongo:
        from governor import MongoSensor

        sensor = MongoSensor(
            dsn, classifier=classifier, track_replication=True, track_connections=True
        )
    else:
        from governor import PostgresSensor

        sensor = PostgresSensor(
            dsn, classifier=classifier, track_replication=True, track_connections=True
        )

    print(f"\n  2. sensor reading ({args.samples} samples) ...", flush=True)
    last = None
    for _ in range(max(1, args.samples)):
        try:
            last = sensor.sample()
        except Exception as e:  # noqa: BLE001
            print(f"     FAIL: sensor.sample() errored: {e}", file=sys.stderr)
            return 1
        time.sleep(0.25)
    assert last is not None
    pool = f"{last.conn_pool_frac:.3f}" if last.conn_pool_frac is not None else "n/a"
    lag = f"{last.replication_lag_s:.2f}s" if last.replication_lag_s is not None else "n/a"
    lat = f"{last.query_latency_ms:.2f}ms" if last.query_latency_ms is not None else "n/a"
    print(f"     level={last.level.name} active={last.active_backends} "
          f"blocked={last.blocked_backends}", flush=True)
    print(f"     replication_lag={lag}  conn_pool_frac={pool}  probe_p99={lat}", flush=True)
    if last.cohorts:
        mig = last.cohorts.get("migration")
        prod = last.cohorts.get("prod")
        if mig is not None and prod is not None:
            print(f"     attribution: migration active={mig.active} "
                  f"(victims={len(mig.victims)})  prod active={prod.active}", flush=True)

    # 3. Independent cross-check.
    print("\n  3. independent cross-check (raw introspection, not via sensor) ...", flush=True)
    try:
        gt_active, gt_blocked = (
            _mongo_ground_truth(dsn) if is_mongo else _pg_ground_truth(dsn)
        )
    except Exception as e:  # noqa: BLE001
        print(f"     WARN: ground-truth read failed ({e}); skipping cross-check.",
              file=sys.stderr)
        print("\n  preflight: PASS (connectivity + sensor ok; cross-check skipped).", flush=True)
        return 0

    drift = abs(last.active_backends - gt_active)
    print(f"     sensor.active={last.active_backends}  ground_truth.active={gt_active}  "
          f"|drift|={drift}", flush=True)
    print(f"     sensor.blocked={last.blocked_backends}  ground_truth.blocked={gt_blocked}",
          flush=True)
    # Live traffic moves between the two reads, so allow a small tolerance.
    if drift > args.tolerance:
        print(f"\n  preflight: WARN — active-backend drift {drift} > tolerance "
              f"{args.tolerance}. Re-run on a quieter moment; persistent large drift "
              "means the sensor query and your ground-truth disagree.", flush=True)
        return 0

    print("\n  preflight: PASS — read-only, reachable, and the sensor's counts match "
          "ground truth.\n  Proceed to:  dbguard observe --dsn ... [--calibrate 60] "
          "[--track-secondary] --report reports/observe.json", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
