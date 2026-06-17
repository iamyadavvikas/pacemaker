"""MongoDB headroom sensor (covers MongoDB / Atlas replica sets).

This is the sensor the most recent incidents needed: the painful 2025–2026
outages saturated MongoDB, not Postgres. It reads only server-side, read-only
admin commands — ``currentOp`` (in-flight operations), ``serverStatus``
(connection counts), and ``replSetGetStatus`` (replica-set lag) — and never
writes to or locks the target. Same closed-loop contract as the Postgres sensor:

- aggregate (default): count active / lock-waiting operations.
- attributed (when a ``CohortClassifier`` is supplied): one entry per operation,
  split into the migration vs prod cohort so an enforcer can pace only migration
  ops (by ``killOp``), never customer traffic.

Mongo signals map onto the same precedence the SQL classifier uses:

  1. user        -- ``effectiveUsers`` on the op (the role the job authenticates as)
  2. comment tag -- a ``comment`` set on the operation, e.g. ``dbguard:migration``
                    (Mongo surfaces it inside ``command`` in ``currentOp``)
  3. appName     -- the driver ``appName`` (often unset/spoofable, consulted last)

``pymongo`` is an optional dependency: import it only when no client is injected,
so the rest of the package (and its tests) run without Mongo installed. A client
can be injected for testing — anything exposing ``.admin.command(...)`` works.
"""

from __future__ import annotations

import json
import time

from ..attribution import MIGRATION, PROD, CohortClassifier
from ..config import Thresholds
from .base import CohortLoad, Headroom, LatencyTracker, Sensor, level_from_signals


class MongoSensor(Sensor):
    """Read-only MongoDB headroom sensor (+ optional per-cohort attribution)."""

    def __init__(
        self,
        uri: str | None = None,
        thresholds: Thresholds | None = None,
        classifier: CohortClassifier | None = None,
        client=None,
        track_replication: bool = True,
        track_connections: bool = True,
    ) -> None:
        self._thresholds = thresholds or Thresholds()
        self._classifier = classifier
        self._track_replication = track_replication
        self._track_connections = track_connections
        self._latency = LatencyTracker()
        if client is None:
            if uri is None:
                raise ValueError("MongoSensor requires either a uri or an injected client")
            from pymongo import MongoClient  # lazy: pymongo is an optional dependency

            # A short server-selection timeout so a dead cluster surfaces as a
            # sensor error (handled fail-safe upstream) rather than hanging.
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        self._client = client

    # --- admin command wrappers (best-effort for secondary signals) ---
    def _current_op(self) -> list[dict]:
        # active=True drops idle connections; $all=False keeps it to real ops.
        t0 = time.perf_counter()
        res = self._client.admin.command({"currentOp": 1, "active": True, "$all": False})
        self._latency.record((time.perf_counter() - t0) * 1000.0)
        return list(res.get("inprog", []))

    def _conn_pool(self) -> tuple[int | None, int | None]:
        if not self._track_connections:
            return None, None
        try:
            conns = self._client.admin.command("serverStatus").get("connections", {})
            current = conns.get("current")
            available = conns.get("available")
            if current is None or available is None:
                return None, None
            # serverStatus reports current (in use) and available (still free);
            # the effective ceiling is the sum.
            return int(current), int(current) + int(available)
        except Exception:  # noqa: BLE001 - a missing grant must never crash the loop
            return None, None

    def _replication_lag_s(self) -> float | None:
        if not self._track_replication:
            return None
        try:
            status = self._client.admin.command("replSetGetStatus")
        except Exception:  # noqa: BLE001 - standalone / no perms -> no lag signal
            return None
        members = status.get("members", [])
        primary = next((m for m in members if m.get("stateStr") == "PRIMARY"), None)
        if primary is None:
            return None
        p_optime = _optime_seconds(primary.get("optimeDate"))
        if p_optime is None:
            return None
        lags = [
            p_optime - s
            for m in members
            if m.get("stateStr") == "SECONDARY"
            and (s := _optime_seconds(m.get("optimeDate"))) is not None
        ]
        return max((lag for lag in lags if lag >= 0), default=0.0)

    # --- sampling ---
    def sample(self) -> Headroom:
        ops = self._current_op()
        lag_s = self._replication_lag_s()
        used, max_conn = self._conn_pool()
        pool_frac = used / max_conn if used is not None and max_conn else None
        lat_ms = self._latency.p99()

        if self._classifier is None:
            active = sum(1 for op in ops if _is_active(op))
            blocked = sum(1 for op in ops if _is_blocked(op))
            return Headroom(
                level=level_from_signals(self._thresholds, active, blocked, lag_s, pool_frac, lat_ms),
                active_backends=active,
                blocked_backends=blocked,
                raw={"active": active, "blocked": blocked},
                replication_lag_s=lag_s,
                conn_pool_used=used,
                conn_pool_max=max_conn,
                query_latency_ms=lat_ms,
            )

        active = blocked = 0
        cohorts = {"migration": CohortLoad(), "prod": CohortLoad()}
        for op in ops:
            is_active = _is_active(op)
            is_blocked = _is_blocked(op)
            if not (is_active or is_blocked):
                continue
            if is_active:
                active += 1
            if is_blocked:
                blocked += 1
            signal = self._classify(op)
            cohort = cohorts[MIGRATION if signal else PROD]
            if is_active:
                cohort.active += 1
                opid = op.get("opid")
                # only migration ops are cancellable (via killOp); prod is never a
                # candidate. opid is an int on standalone/replica-set deployments.
                if signal is not None and isinstance(opid, int):
                    cohort.victims.append((opid, signal))
            if is_blocked:
                cohort.blocked += 1

        return Headroom(
            level=level_from_signals(self._thresholds, active, blocked, lag_s, pool_frac, lat_ms),
            active_backends=active,
            blocked_backends=blocked,
            raw={"active": active, "blocked": blocked},
            cohorts=cohorts,
            replication_lag_s=lag_s,
            conn_pool_used=used,
            conn_pool_max=max_conn,
            query_latency_ms=lat_ms,
        )

    def _classify(self, op: dict) -> str | None:
        eff = op.get("effectiveUsers") or []
        user = eff[0].get("user") if eff and isinstance(eff[0], dict) else None
        app_name = op.get("appName")
        # The migration job's comment (Mongo ``comment`` option) is surfaced inside
        # ``command`` in currentOp; serialize it so the query-tag substring match
        # can find e.g. ``dbguard:migration``.
        command_text = json.dumps(op.get("command", {}), default=str)
        return self._classifier.matched_signal(user, app_name, command_text)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 - close must never raise
            pass


def _is_active(op: dict) -> bool:
    """An op that is currently executing work (not idle / not lock-waiting)."""
    return bool(op.get("active", False)) and op.get("type", "op") != "idleSession"


def _is_blocked(op: dict) -> bool:
    """An op stalled waiting to acquire a lock."""
    return bool(op.get("waitingForLock", False))


def _optime_seconds(optime_date) -> float | None:
    """Convert a Mongo optimeDate (datetime) to epoch seconds, or None."""
    if optime_date is None:
        return None
    try:
        return optime_date.timestamp()
    except AttributeError:
        return None


class MongoKiller:
    """Applies back-pressure to one in-flight Mongo op via ``killOp``.

    The Mongo analogue of :class:`~governor.enforcer.PostgresCanceller`: it pairs
    with :class:`~governor.enforcer.EnforcerAgent` to pace the migration cohort.
    ``killOp`` cancels a single operation (the batch worker just retries), so
    repeated kills translate into a lower effective concurrency rather than
    severing the job. Requires the ``killop`` privilege (e.g. ``clusterMonitor``
    plus ``hostManager``, or ``clusterManager``).
    """

    def __init__(self, uri: str | None = None, client=None) -> None:
        if client is None:
            if uri is None:
                raise ValueError("MongoKiller requires either a uri or an injected client")
            from pymongo import MongoClient

            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        self._client = client

    def cancel(self, pid: int) -> bool:
        self._client.admin.command({"killOp": 1, "op": pid})
        return True

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass
