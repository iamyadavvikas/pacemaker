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

import psycopg

from ..attribution import MIGRATION, PROD, CohortClassifier
from ..config import Thresholds
from .base import CohortLoad, Headroom, Level, Sensor

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


class PostgresSensor(Sensor):
    def __init__(
        self,
        dsn: str,
        thresholds: Thresholds | None = None,
        classifier: CohortClassifier | None = None,
    ) -> None:
        self._thresholds = thresholds or Thresholds()
        self._classifier = classifier
        # autocommit so each sample is a fresh snapshot and never holds a txn open.
        self._conn = psycopg.connect(dsn, autocommit=True)

    def sample(self) -> Headroom:
        if self._classifier is None:
            return self._sample_aggregate()
        return self._sample_attributed()

    def _level(self, active: int, blocked: int) -> Level:
        t = self._thresholds
        if active >= t.critical_active:
            level = Level.CRITICAL
        elif active <= t.green_max_active:
            level = Level.GREEN
        elif active <= t.yellow_max_active:
            level = Level.YELLOW
        else:
            level = Level.RED

        if blocked > 0 and t.blocked_forces_red and level < Level.RED:
            level = Level.RED
        return level

    def _sample_aggregate(self) -> Headroom:
        with self._conn.cursor() as cur:
            cur.execute(_SAMPLE_SQL)
            active, blocked = cur.fetchone()
        active, blocked = int(active), int(blocked)
        return Headroom(
            level=self._level(active, blocked),
            active_backends=active,
            blocked_backends=blocked,
            raw={"active": active, "blocked": blocked},
        )

    def _sample_attributed(self) -> Headroom:
        with self._conn.cursor() as cur:
            cur.execute(_ATTRIBUTED_SQL)
            rows = cur.fetchall()

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

        return Headroom(
            level=self._level(active, blocked),
            active_backends=active,
            blocked_backends=blocked,
            raw={"active": active, "blocked": blocked},
            cohorts=cohorts,
        )

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover - best effort
            pass
