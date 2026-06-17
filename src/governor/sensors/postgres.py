"""PostgreSQL headroom sensor.

Reads only ``pg_stat_activity``. In production this should connect with a
read-only role (e.g. ``pg_monitor`` membership). No writes, no locks.

Two sampling modes:

- aggregate (default): one ``count()`` query for active / lock-waiting backends.
- attributed (when a ``CohortClassifier`` is supplied): pulls one row per backend
  and splits the counts into the migration vs prod cohorts, so a downstream
  observer can pace only the migration cohort.
"""

from __future__ import annotations

import time

import psycopg

from ..attribution import MIGRATION, PROD, CohortClassifier
from ..config import Thresholds
from .base import CohortLoad, Headroom, Level, Sensor, LatencyTracker, level_from_signals

# Counts concurrently-active and lock-waiting backends in the current database,
# excluding this sensor's own connection.
_SAMPLE_SQL = """
SELECT
    count(*) FILTER (WHERE state = 'active')                  AS active,
    count(*) FILTER (WHERE wait_event_type = 'Lock')          AS blocked
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid();
"""

# One row per backend, so we can attribute each to a workload cohort.
_ATTRIBUTED_SQL = """
SELECT pid, usename, application_name, state, wait_event_type, query
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid();
"""

# Connection-pool saturation: backends in use vs the server's max_connections.
_CONN_POOL_SQL = """
SELECT (SELECT count(*) FROM pg_stat_activity)                  AS used,
       current_setting('max_connections')::int                 AS max_conn;
"""

# Worst replica lag in seconds, from the primary's view of its WAL senders.
# Returns NULL when there are no connected replicas (then we report no lag).
_REPLICATION_LAG_SQL = """
SELECT COALESCE(EXTRACT(EPOCH FROM max(
    CASE WHEN replay_lag IS NOT NULL THEN replay_lag ELSE write_lag END
)), 0.0) AS lag_s
FROM pg_stat_replication;
"""


class PostgresSensor(Sensor):
    def __init__(
        self,
        dsn: str,
        thresholds: Thresholds | None = None,
        classifier: CohortClassifier | None = None,
        track_replication: bool = False,
        track_connections: bool = False,
    ) -> None:
        self._thresholds = thresholds or Thresholds()
        self._classifier = classifier
        self._track_replication = track_replication
        self._track_connections = track_connections
        self._latency = LatencyTracker()
        # autocommit so each sample is a fresh snapshot and never holds a txn open.
        self._conn = psycopg.connect(dsn, autocommit=True)

    def sample(self) -> Headroom:
        if self._classifier is None:
            return self._sample_aggregate()
        return self._sample_attributed()

    def _level(self, active: int, blocked: int, lag_s=None, pool_frac=None, lat_ms=None) -> Level:
        return level_from_signals(self._thresholds, active, blocked, lag_s, pool_frac, lat_ms)

    def _secondary_signals(self) -> tuple[float | None, int | None, int | None]:
        """Read optional replication lag and connection-pool counts (best effort).

        Either query failing (e.g. the read-only role lacks ``pg_monitor``, or the
        target isn't a primary) degrades gracefully to ``None`` for that signal,
        so a missing grant never crashes the sample loop.
        """
        lag_s = used = max_conn = None
        if self._track_replication:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(_REPLICATION_LAG_SQL)
                    row = cur.fetchone()
                    lag_s = float(row[0]) if row and row[0] is not None else None
            except Exception:  # noqa: BLE001 - secondary signal must never crash the loop
                lag_s = None
        if self._track_connections:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(_CONN_POOL_SQL)
                    row = cur.fetchone()
                    if row:
                        used, max_conn = int(row[0]), int(row[1])
            except Exception:  # noqa: BLE001
                used = max_conn = None
        return lag_s, used, max_conn

    def _sample_aggregate(self) -> Headroom:
        with self._conn.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(_SAMPLE_SQL)
            active, blocked = cur.fetchone()
            self._latency.record((time.perf_counter() - t0) * 1000.0)
        active, blocked = int(active), int(blocked)
        lag_s, used, max_conn = self._secondary_signals()
        pool_frac = used / max_conn if used is not None and max_conn else None
        lat_ms = self._latency.p99()
        return Headroom(
            level=self._level(active, blocked, lag_s, pool_frac, lat_ms),
            active_backends=active,
            blocked_backends=blocked,
            raw={"active": active, "blocked": blocked},
            replication_lag_s=lag_s,
            conn_pool_used=used,
            conn_pool_max=max_conn,
            query_latency_ms=lat_ms,
        )

    def _sample_attributed(self) -> Headroom:
        with self._conn.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(_ATTRIBUTED_SQL)
            rows = cur.fetchall()
            self._latency.record((time.perf_counter() - t0) * 1000.0)

        active = blocked = 0
        cohorts = {"migration": CohortLoad(), "prod": CohortLoad()}
        for pid, usename, application_name, state, wait_event_type, query in rows:
            is_active = state == "active"
            is_blocked = wait_event_type == "Lock"
            if not (is_active or is_blocked):
                continue  # idle / idle-in-txn backends don't consume headroom
            if is_active:
                active += 1
            if is_blocked:
                blocked += 1
            signal = self._classifier.matched_signal(usename, application_name, query)
            name = MIGRATION if signal else PROD
            cohort = cohorts[name]
            if is_active:
                cohort.active += 1
                # only the migration cohort records cancellable victims; prod is
                # never a candidate. signal lets the enforcer prefer strong matches.
                if signal is not None and pid is not None:
                    cohort.victims.append((int(pid), signal))
            if is_blocked:
                cohort.blocked += 1

        lag_s, used, max_conn = self._secondary_signals()
        pool_frac = used / max_conn if used is not None and max_conn else None
        lat_ms = self._latency.p99()
        return Headroom(
            level=self._level(active, blocked, lag_s, pool_frac, lat_ms),
            active_backends=active,
            blocked_backends=blocked,
            raw={"active": active, "blocked": blocked},
            cohorts=cohorts,
            replication_lag_s=lag_s,
            conn_pool_used=used,
            conn_pool_max=max_conn,
            query_latency_ms=lat_ms,
        )

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover - best effort
            pass
