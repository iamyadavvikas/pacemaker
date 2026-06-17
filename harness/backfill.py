"""The abusive backfill.

N worker threads rewrite the ``projects`` table in batches, each batch doing
real CPU+WAL work (md5 over the row data). Run two ways:

- ungoverned: all N workers hammer flat-out  -> saturates the DB.
- governed:   every batch goes through ``Governor.batch()`` -> paced to headroom.
"""

from __future__ import annotations

import threading

import psycopg

from . import DSN

N_WORKERS = 12
BATCH_SIZE = 5_000
PROJECT_MAX_ID = 800_000
PASSES = 3

# Nested md5 makes each batch genuinely CPU+WAL heavy, so a 1-CPU Postgres stays
# saturated long enough to hurt the checkout path.
_UPDATE_SQL = """
UPDATE projects
SET state = 'v3',
    data = md5(md5(data || clock_timestamp()::text)),
    touched_at = now()
WHERE id >= %s AND id < %s;
"""


class _BatchCursor:
    """Hands out [lo, hi) id ranges to workers, once each, over several passes."""

    def __init__(self, max_id: int, batch: int, passes: int) -> None:
        self._next = 1
        self._max = max_id
        self._batch = batch
        self._passes = passes
        self._pass = 0
        self._lock = threading.Lock()

    def next_range(self) -> tuple[int, int] | None:
        with self._lock:
            if self._next > self._max:
                self._pass += 1
                if self._pass >= self._passes:
                    return None
                self._next = 1
            lo = self._next
            hi = min(lo + self._batch, self._max + 1)
            self._next = hi
            return lo, hi


def run_backfill(
    dsn: str = DSN, governor=None, n_workers: int = N_WORKERS, passes: int = PASSES
) -> None:
    """Run the full backfill. If ``governor`` is given, pace each batch through it."""
    cursor = _BatchCursor(PROJECT_MAX_ID, BATCH_SIZE, passes)

    def worker() -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            while True:
                rng = cursor.next_range()
                if rng is None:
                    return
                lo, hi = rng
                if governor is not None:
                    with governor.batch():
                        with conn.cursor() as cur:
                            cur.execute(_UPDATE_SQL, (lo, hi))
                else:
                    with conn.cursor() as cur:
                        cur.execute(_UPDATE_SQL, (lo, hi))

    threads = [threading.Thread(target=worker, name=f"backfill-{i}") for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
