"""Synthetic multi-squad load for ``dbguard observe --demo``.

Generates two concurrent workloads against the demo Postgres so the observer's
attribution and shadow-throttling visibly fire:

- a **migration cohort**: several workers rewriting ``projects`` in batches, each
  query carrying the ``/* dbguard:migration */`` tag and connecting with
  ``application_name='dbguard_demo_migration'`` — so the default classifier
  attributes it without guesswork;
- a **prod cohort**: a light, steady checkout-style workload on ``orders`` that
  shares the same DB role, to prove attribution separates them by signal (tag /
  application_name), not just by login.

Synthetic, generated data only; the load writes only to the demo tables.
"""

from __future__ import annotations

import random
import threading
import time

import psycopg

from .seed import seed

# Identifiers the default classifier keys on (application_name + query tag).
DEMO_MIGRATION_APP = "dbguard_demo_migration"
DEMO_MIGRATION_USER = DEMO_MIGRATION_APP  # alias used by the CLI's --migration-app default
DEMO_PROD_APP = "dbguard_demo_checkout"

_MIGRATION_BATCH = 5_000
_PROJECT_MAX_ID = 800_000
_N_MIGRATION_WORKERS = 8

# The migration UPDATE carries an inline tag so attribution can flag it even when
# it shares a DB role with prod traffic.
_MIGRATION_SQL = """
/* dbguard:migration */
UPDATE projects
SET state = 'v3',
    data = md5(md5(data || clock_timestamp()::text)),
    touched_at = now()
WHERE id >= %s AND id < %s;
"""


class DemoLoad:
    """Handle for the running synthetic workload; call ``stop()`` to end it."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def add(self, thread: threading.Thread) -> None:
        self._threads.append(thread)

    @property
    def stop_event(self) -> threading.Event:
        return self._stop

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=10)


def seed_demo(dsn: str) -> None:
    """(Re)seed the synthetic schema. Idempotent enough for a demo."""
    seed(dsn)


def _migration_worker(dsn: str, stop: threading.Event, start_id: int = 1) -> None:
    with psycopg.connect(dsn, autocommit=True, application_name=DEMO_MIGRATION_APP) as conn:
        lo = start_id
        while not stop.is_set():
            hi = min(lo + _MIGRATION_BATCH, _PROJECT_MAX_ID + 1)
            try:
                with conn.cursor() as cur:
                    cur.execute(_MIGRATION_SQL, (lo, hi))
            except psycopg.errors.OperationalError:
                # Deadlock/serialization failure under contention: autocommit has
                # already rolled the batch back. Skip it and keep the synthetic
                # migration running rather than crashing the worker thread.
                pass
            lo = hi if hi <= _PROJECT_MAX_ID else 1


def _prod_worker(dsn: str, stop: threading.Event) -> None:
    # Light, steady checkout-style traffic: same role, different signal (app_name).
    with psycopg.connect(dsn, autocommit=True, application_name=DEMO_PROD_APP) as conn:
        while not stop.is_set():
            oid = random.randint(1, 5_000)
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM orders WHERE id = %s;", (oid,))
                cur.fetchone()
                cur.execute("UPDATE orders SET updated_at = now() WHERE id = %s;", (oid,))
            stop.wait(0.01)


def start_demo_load(dsn: str, n_migration_workers: int | None = None) -> DemoLoad:
    """Start the migration + prod workloads on daemon threads.

    ``n_migration_workers`` caps the number of concurrent migration workers
    (default: ``_N_MIGRATION_WORKERS``). Lowering it simulates running the
    migration at a reduced pace — used by the demo counterfactual to compare an
    uncapped migration against one capped to the pace dbguard would recommend.
    """
    workers = _N_MIGRATION_WORKERS if n_migration_workers is None else max(1, n_migration_workers)
    load = DemoLoad()
    stop = load.stop_event
    # Stagger each worker's start id across the key space so they sweep different
    # regions concurrently instead of all hammering the same rows (which caused
    # frequent deadlocks). This is both more realistic and far more stable.
    stride = max(1, _PROJECT_MAX_ID // workers)
    for i in range(workers):
        start_id = 1 + i * stride
        t = threading.Thread(
            target=_migration_worker,
            args=(dsn, stop, start_id),
            name=f"demo-migration-{i}",
            daemon=True,
        )
        load.add(t)
        t.start()
    prod = threading.Thread(
        target=_prod_worker, args=(dsn, stop), name="demo-prod", daemon=True
    )
    load.add(prod)
    prod.start()
    return load
