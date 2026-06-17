"""Observer agent: standalone, read-only, OBSERVE-only DB guardrail.

This is the productized wedge. Unlike ``Governor`` (an in-process library that
worker threads block inside), the observer is a *sidecar*: it attaches to a live
Postgres with a read-only role, watches the migration cohort's load via
attribution, and runs the same AIMD policy **in shadow** — it computes the limit
the migration workload *would* be paced to and records every moment it *would*
have throttled, but it never blocks, cancels, or otherwise touches the database.

It also publishes a single ``throttle`` verdict (proceed vs back-off) that real
migration tools can consult over HTTP (gh-ost ``--throttle-http`` / pt-osc), so
the same agent can demo against tools teams already run. In OBSERVE the verdict
is advisory only — the dashboard shows what it *would* have signalled.

    agent = ObserverAgent(GovernorConfig(dsn=...), classifier=CohortClassifier(...))
    agent.start()
    ...
    snap = agent.snapshot()          # JSON-ready, for the dashboard
    verdict = agent.throttle_verdict()  # {"throttle": bool, ...} for /throttle
    agent.stop()
    agent.report.write_json("reports/observe.json")

Everything here is read-only and side-effect free against the target DB.
"""

from __future__ import annotations

import contextlib
import threading
import time

from .attribution import MIGRATION
from .config import GovernorConfig
from .policy.aimd import next_limit
from .report import Report, Sample
from .sensors.base import CohortLoad, Headroom, Level, Sensor
from .tuning import apply_tuning, tuning_view as _tuning_view


class ObserverAgent:
    """Shadow-mode AIMD pacer for the migration cohort. Never enforces."""

    def __init__(
        self,
        config: GovernorConfig,
        sensor: Sensor,
        report: Report | None = None,
        notifier=None,
    ) -> None:
        self._cfg = config
        self._sensor = sensor
        self.report = report or Report(label="observe")
        from .notify import NullNotifier

        self._notifier = notifier or NullNotifier()

        # Shadow concurrency limit for the migration cohort.
        self._limit = config.start_limit
        self._last_level = Level.GREEN
        self._migration_active = 0
        self._migration_blocked = 0
        self._would_throttle = False
        self._would_throttle_total = 0
        # Last-read secondary signals (None until the sensor supplies them).
        self._replication_lag_s: float | None = None
        self._conn_pool_frac: float | None = None
        self._query_latency_ms: float | None = None

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sampler: threading.Thread | None = None

    # --- lifecycle ---
    def start(self) -> None:
        if self._sampler is not None:
            return
        self._stop.clear()
        self._sampler = threading.Thread(
            target=self._sample_loop, name="dbguard-observer", daemon=True
        )
        self._sampler.start()

    def stop(self) -> None:
        self._stop.set()
        if self._sampler is not None:
            self._sampler.join(timeout=5)
            self._sampler = None
        with contextlib.suppress(Exception):
            self._sensor.close()

    # --- the sampling/shadow-control loop ---
    def _sample_loop(self) -> None:
        was_throttling = False
        while not self._stop.is_set():
            try:
                hr = self._sensor.sample()
                sensor_error = None
            except Exception as exc:  # noqa: BLE001 - any sensor failure must fail safe
                # Fail-safe: an unreadable DB is treated as CRITICAL. In OBSERVE
                # this only means the SHADOW limit clamps and we'd-throttle; it
                # never blocks a real job, but it keeps the recommendation honest.
                hr = Headroom(level=Level.CRITICAL, active_backends=-1, blocked_backends=-1)
                sensor_error = exc

            mig = hr.cohorts.get(MIGRATION, CohortLoad())
            new_limit = next_limit(self._limit, hr.level, self._cfg)
            # In shadow we have no real in-flight counter to gate; the proxy for
            # "this workload would be throttled now" is: the migration cohort is
            # running at or above the limit the policy currently allows.
            would_throttle = mig.active >= new_limit or new_limit == 0

            with self._lock:
                old_limit = self._limit
                self._limit = new_limit
                self._last_level = hr.level
                self._migration_active = mig.active
                self._migration_blocked = mig.blocked
                self._would_throttle = would_throttle
                if would_throttle:
                    self._would_throttle_total += 1
                self._replication_lag_s = hr.replication_lag_s
                self._conn_pool_frac = hr.conn_pool_frac
                self._query_latency_ms = hr.query_latency_ms
                self.report.add_sample(
                    Sample(
                        t=time.time() - self.report.started_at,
                        active_backends=hr.active_backends,
                        blocked_backends=hr.blocked_backends,
                        level=hr.level.name,
                        limit=new_limit,
                        in_flight=mig.active,
                    )
                )

            if sensor_error is not None:
                self.report.add_event(
                    "sensor_error",
                    f"sensor unreadable ({sensor_error!r}) -> fail-safe pause",
                )
                self._notify("sensor_error", f"sensor unreadable ({sensor_error!r}) -> fail-safe pause")
            elif would_throttle and hr.level is Level.CRITICAL:
                self.report.add_event(
                    "would_circuit_break",
                    f"migration active={mig.active} at {hr.level.name} -> would pause",
                )
                self._notify(
                    "would_circuit_break",
                    f"migration active={mig.active} at CRITICAL -> would pause",
                    {"migration_active": mig.active, "level": hr.level.name},
                )
            elif would_throttle:
                self.report.add_event(
                    "would_throttle",
                    f"migration active={mig.active} >= shadow limit={new_limit} ({hr.level.name})",
                )
            elif new_limit < old_limit:
                self.report.add_event(
                    "would_backoff",
                    f"{hr.level.name}: shadow limit {old_limit} -> {new_limit}",
                )

            # Edge-detect a new throttling episode so on-call gets ONE alert per
            # episode, not one per ~4Hz poll.
            if would_throttle and not was_throttling:
                self._notify(
                    "throttle_started",
                    f"would begin throttling migration (active={mig.active}, "
                    f"shadow limit={new_limit}, {hr.level.name})",
                    {"migration_active": mig.active, "limit": new_limit, "level": hr.level.name},
                )
            was_throttling = would_throttle

            self._stop.wait(self._cfg.poll_interval_s)

    def _notify(self, kind: str, message: str, context: dict | None = None) -> None:
        try:
            self._notifier.notify(kind, message, context)
        except Exception:  # noqa: BLE001 - alerting must never break the loop
            pass

    # --- the throttle verdict (for gh-ost/pt-osc compatible endpoint) ---
    def throttle_verdict(self) -> dict:
        """Advisory throttle decision for the migration cohort.

        ``throttle: True`` means the policy would currently pace this workload
        (a real client should slow down). In OBSERVE this is purely advisory.
        """
        with self._lock:
            return {
                "throttle": self._would_throttle,
                "mode": "observe",
                "limit": self._limit,
                "migration_active": self._migration_active,
                "level": self._last_level.name,
            }

    # --- live tuning (self-service pacing knobs) ---
    def tuning(self) -> dict:
        """Current values of the live-tunable pacing knobs."""
        with self._lock:
            return _tuning_view(self._cfg)

    def set_tuning(self, updates: dict) -> dict:
        """Validate and apply pacing-knob ``updates`` at runtime (thread-safe).

        Clamps the shadow limit into any new ``[min_limit, max_limit]`` so a
        lowered ceiling takes effect immediately. Raises ``ValueError`` on a bad
        edit (the dashboard surfaces the message); nothing is half-applied.
        """
        with self._lock:
            new_cfg = apply_tuning(self._cfg, updates)
            self._cfg = new_cfg
            self._limit = max(new_cfg.min_limit, min(self._limit, new_cfg.max_limit))
            view = _tuning_view(new_cfg)
        self.report.add_event("tuning_changed", f"applied {updates}")
        self._notify("tuning_changed", "pacing knobs updated", dict(updates))
        return view

    def snapshot(self, window: int = 240, event_window: int = 40) -> dict:
        """JSON-ready view of the observer's live state, for the dashboard."""
        with self._lock:
            live = {
                "mode": "observe",
                "limit": self._limit,
                "in_flight": self._migration_active,
                "last_level": self._last_level.name,
                "running": self._sampler is not None and not self._stop.is_set(),
                "max_limit": self._cfg.max_limit,
                "min_limit": self._cfg.min_limit,
                "migration_active": self._migration_active,
                "migration_blocked": self._migration_blocked,
                "would_throttle_now": self._would_throttle,
                "would_throttle_total": self._would_throttle_total,
                "replication_lag_s": self._replication_lag_s,
                "conn_pool_frac": self._conn_pool_frac,
                "query_latency_ms": self._query_latency_ms,
                "tuning": _tuning_view(self._cfg),
            }
        timeline = self.report.snapshot(window=window, event_window=event_window)
        return {**live, **timeline}
