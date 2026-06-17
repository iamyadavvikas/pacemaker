"""Enforcer agent: out-of-band, non-cooperative pacing of the migration cohort.

This is Stage 2 \u2014 the part someone pays for. Where :class:`ObserverAgent` only
records what it *would* have throttled and the cooperative ``/throttle`` endpoint
needs the migration tool to opt in, the enforcer governs a job that will **not**
call us: it watches the migration cohort via attribution and, when that cohort
runs over the pace the AIMD policy allows, applies database-native back-pressure
to specific migration backends \u2014 cancelling their in-flight statement so a
batch loop retries more slowly. The net effect is a capped concurrency the job
never agreed to.

Hard invariants (these are the whole product promise):

* **Never in the prod data path.** Enforcement is an out-of-band control loop;
  prod queries never pass through dbguard. The control connection is separate
  from the read-only health sensor.
* **Never touch prod.** Only backends the classifier attributes to the migration
  cohort are candidates, and (by default) only those matched on a *strong* signal
  (DB role / query tag), never the spoofable ``application_name`` alone.
* **Fail open, not closed.** If the sensor can't be read we do NOT cancel blind;
  a dead dbguard means the migration runs unthrottled, it must never wedge prod.
* **Runaway guard.** If the enforcer would cancel more than a configured number
  of backends within a window, it trips its own breaker, stops enforcing, and
  keeps only observing \u2014 a misconfiguration can't turn into a cancel storm.

    agent = EnforcerAgent(GovernorConfig(dsn=..., control_dsn=...), sensor)
    agent.start()
    ...
    agent.stop()
    agent.report.write_json("reports/enforce.json")
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections import deque

import psycopg

from .attribution import MIGRATION
from .config import GovernorConfig
from .policy.aimd import next_limit
from .report import Report, Sample
from .sensors.base import CohortLoad, Headroom, Level, Sensor

# Attribution signals, strongest (hardest to spoof) first. The enforcer cancels
# the strongest-signal backends first and, by default, refuses the weakest.
_STRONG_SIGNALS = ("usename", "query_tag")
_SIGNAL_RANK = {"usename": 0, "query_tag": 1, "application_name": 2}


class Canceller:
    """Applies back-pressure to a single backend. Inject a fake one in tests."""

    def cancel(self, pid: int) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover - optional
        pass


class PostgresCanceller(Canceller):
    """Cancels a backend's in-flight statement via ``pg_cancel_backend``.

    Uses ``pg_cancel_backend`` (cancel the running query) rather than
    ``pg_terminate_backend`` (kill the whole connection): we want to *pace* the
    job, not sever it. A batch worker whose statement is cancelled simply retries
    the batch, so repeated cancels translate into a lower effective concurrency.
    The connection is privileged/write and must be the migration role itself or a
    role with ``pg_signal_backend``.
    """

    def __init__(self, dsn: str) -> None:
        self._conn = psycopg.connect(dsn, autocommit=True)

    def cancel(self, pid: int) -> bool:
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_cancel_backend(%s);", (pid,))
            row = cur.fetchone()
        return bool(row and row[0])

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()


def _select_victims(
    victims: list[tuple[int, str]],
    excess: int,
    require_strong_signal: bool,
) -> list[tuple[int, str]]:
    """Pick which migration backends to pace down, strongest signal first.

    ``excess`` is how many backends over the allowed pace we are. With
    ``require_strong_signal`` we drop any backend matched only on the weak,
    spoofable ``application_name`` so a mislabelled prod connection can never be
    cancelled.
    """
    if excess <= 0:
        return []
    ranked = sorted(victims, key=lambda v: _SIGNAL_RANK.get(v[1], 99))
    if require_strong_signal:
        ranked = [v for v in ranked if v[1] in _STRONG_SIGNALS]
    return ranked[:excess]


class EnforcerAgent:
    """AIMD-paced, non-cooperative enforcer for the migration cohort."""

    def __init__(
        self,
        config: GovernorConfig,
        sensor: Sensor,
        canceller: Canceller | None = None,
        report: Report | None = None,
    ) -> None:
        self._cfg = config
        self._sensor = sensor
        # Separate privileged control connection; defaults to the main DSN (fine
        # for the demo where the same role owns the migration backends).
        self._canceller = canceller or PostgresCanceller(config.control_dsn or config.dsn)
        self.report = report or Report(label="enforce")

        self._limit = config.start_limit
        self._last_level = Level.GREEN
        self._migration_active = 0
        self._migration_blocked = 0
        self._would_throttle = False
        self._cancels_total = 0
        self._enforcing = True  # tripped to False by the runaway guard
        self._cancel_times: deque[float] = deque()

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sampler: threading.Thread | None = None

    # --- lifecycle ---
    def start(self) -> None:
        if self._sampler is not None:
            return
        self._stop.clear()
        self._sampler = threading.Thread(
            target=self._sample_loop, name="dbguard-enforcer", daemon=True
        )
        self._sampler.start()

    def stop(self) -> None:
        self._stop.set()
        if self._sampler is not None:
            self._sampler.join(timeout=5)
            self._sampler = None
        with contextlib.suppress(Exception):
            self._sensor.close()
        with contextlib.suppress(Exception):
            self._canceller.close()

    # --- runaway guard ---
    def _record_cancels(self, n: int) -> bool:
        """Track cancels in a rolling window; trip the breaker if it runs away.

        Returns True if the enforcer is still allowed to enforce after this tick.
        """
        now = time.monotonic()
        for _ in range(n):
            self._cancel_times.append(now)
        window_start = now - self._cfg.runaway_window_s
        while self._cancel_times and self._cancel_times[0] < window_start:
            self._cancel_times.popleft()
        if len(self._cancel_times) > self._cfg.runaway_cancel_threshold:
            self._enforcing = False
            return False
        return True

    # --- the sampling/enforcement loop ---
    def _sample_loop(self) -> None:
        while not self._stop.is_set():
            try:
                hr = self._sensor.sample()
                sensor_error = None
            except Exception as exc:  # noqa: BLE001 - any sensor failure must fail OPEN
                # Fail OPEN: an unreadable DB means we do NOT cancel anything this
                # tick (we can't see whom to pace, and a blind cancel could hit
                # prod). We still record the level so the report shows the gap.
                hr = Headroom(level=Level.CRITICAL, active_backends=-1, blocked_backends=-1)
                sensor_error = exc

            mig = hr.cohorts.get(MIGRATION, CohortLoad())
            new_limit = next_limit(self._limit, hr.level, self._cfg)

            cancelled: list[tuple[int, str]] = []
            if sensor_error is None and self._enforcing and self._cfg.enforce_mechanism == "cancel":
                excess = mig.active - new_limit  # new_limit==0 -> cancel all active
                budget = min(max(excess, 0), self._cfg.max_cancels_per_interval)
                for pid, signal in _select_victims(mig.victims, budget, self._cfg.require_strong_signal):
                    try:
                        if self._canceller.cancel(pid):
                            cancelled.append((pid, signal))
                    except Exception as exc:  # noqa: BLE001 - a failed cancel must not crash the loop
                        self.report.add_event("cancel_error", f"pid={pid}: {exc!r}")

            still_enforcing = True
            if cancelled:
                still_enforcing = self._record_cancels(len(cancelled))

            with self._lock:
                old_limit = self._limit
                self._limit = new_limit
                self._last_level = hr.level
                self._migration_active = mig.active
                self._migration_blocked = mig.blocked
                self._would_throttle = mig.active >= new_limit or new_limit == 0
                self._cancels_total += len(cancelled)
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
                    f"sensor unreadable ({sensor_error!r}) -> fail-open, no cancels this tick",
                )
            for pid, signal in cancelled:
                self.report.add_event(
                    "cancel",
                    f"paced migration backend pid={pid} (signal={signal}) "
                    f"active={mig.active} > limit={new_limit} ({hr.level.name})",
                )
            if not still_enforcing:
                self.report.add_event(
                    "enforcer_tripped",
                    f"runaway guard: > {self._cfg.runaway_cancel_threshold} cancels in "
                    f"{self._cfg.runaway_window_s}s -> enforcement disabled, observing only",
                )
            elif not cancelled and new_limit < old_limit:
                self.report.add_event(
                    "backoff",
                    f"{hr.level.name}: shadow limit {old_limit} -> {new_limit}",
                )

            self._stop.wait(self._cfg.poll_interval_s)

    # --- throttle verdict (for gh-ost/pt-osc compatible endpoint) ---
    def throttle_verdict(self) -> dict:
        """Advisory throttle decision for the migration cohort.

        Cooperative tools can still consult this; meanwhile the enforcer is also
        actively pacing non-cooperative ones.
        """
        with self._lock:
            return {
                "throttle": self._would_throttle,
                "mode": "enforce",
                "limit": self._limit,
                "migration_active": self._migration_active,
                "level": self._last_level.name,
            }

    def snapshot(self, window: int = 240, event_window: int = 40) -> dict:
        """JSON-ready view of the enforcer's live state, for the dashboard."""
        with self._lock:
            live = {
                "mode": "enforce",
                "limit": self._limit,
                "in_flight": self._migration_active,
                "last_level": self._last_level.name,
                "running": self._sampler is not None and not self._stop.is_set(),
                "max_limit": self._cfg.max_limit,
                "min_limit": self._cfg.min_limit,
                "migration_active": self._migration_active,
                "migration_blocked": self._migration_blocked,
                "would_throttle_now": self._would_throttle,
                "cancels_total": self._cancels_total,
                "enforcing": self._enforcing,
            }
        timeline = self.report.snapshot(window=window, event_window=event_window)
        return {**live, **timeline}
