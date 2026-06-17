"""Integration test: the governed backfill must protect the checkout path.

Skipped automatically if the demo Postgres is not reachable. Bring it up with
``docker compose up -d`` to run this test.
"""

from __future__ import annotations

import psycopg
import pytest

from governor import Governor, GovernorConfig, Mode, PostgresSensor

from harness import DSN
from harness.backfill import run_backfill
from harness.checkout_probe import CheckoutProbe
from harness.demo import LoadMonitor
from harness.seed import seed


def _db_available() -> bool:
    try:
        with psycopg.connect(DSN, connect_timeout=2):
            return True
    except Exception:  # noqa: BLE001
        return False


pytestmark = pytest.mark.skipif(not _db_available(), reason="demo Postgres not running")


@pytest.fixture(scope="module")
def seeded():
    seed(DSN)
    yield


def _run(governor):
    monitor = LoadMonitor(DSN)
    probe = CheckoutProbe(DSN)
    monitor.start()
    probe.start()
    if governor is not None:
        governor.start()
    run_backfill(DSN, governor=governor)
    if governor is not None:
        governor.stop()
    probe.stop()
    monitor.stop()
    return probe, monitor


def test_governor_contains_db_load_and_protects_p99(seeded):
    ungoverned_probe, ungoverned_monitor = _run(None)

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("VACUUM (ANALYZE) projects;")

    gov = Governor(GovernorConfig(dsn=DSN), PostgresSensor(DSN), mode=Mode.ENFORCE)
    governed_probe, governed_monitor = _run(gov)

    # The governor must hold DB concurrency strictly below the unmanaged peak ...
    assert governed_monitor.max_active < ungoverned_monitor.max_active
    # ... eliminate lock contention ...
    assert governed_monitor.blocked_samples == 0
    # ... and keep customer-facing p99 meaningfully better.
    assert governed_probe.percentile(99) < ungoverned_probe.percentile(99)
