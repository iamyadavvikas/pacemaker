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


class ObserverAgent:
    """Shadow-mode AIMD pacer for the migration cohort. Never enforces."""

    def __init__(
        self,
        config: GovernorConfig,
        sensor: Sensor,
        report: Report | None = None,
    ) -> None:
        self._cfg = config
        self._sensor = sensor
        self.report = report or Report(label="observe")

        # Shadow concurrency limit for the migration cohort.
        self._limit = config.start_limit
        self._last_level = Level.GREEN
        self._migration_active = 0
        self._migration_blocked = 0
        self._would_throttle = False
        self._would_throttle_total = 0

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
            elif would_throttle and hr.level is Level.CRITICAL:
                self.report.add_event(
                    "would_circuit_break",
                    f"migration active={mig.active} at {hr.level.name} -> would pause",
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

            self._stop.wait(self._cfg.poll_interval_s)

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
            }
        timeline = self.report.snapshot(window=window, event_window=event_window)
        return {**live, **timeline}
