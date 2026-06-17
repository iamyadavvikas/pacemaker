"""The Governor: paces in-flight migration batches against live DB headroom.

Usage (threaded backfill workers each call ``batch()``):

    gov = Governor(GovernorConfig(dsn=...), PostgresSensor(dsn))
    gov.start()
    ...
    with gov.batch():
        run_one_batch()
    ...
    gov.stop()

A background thread samples the sensor every ``poll_interval_s`` and updates the
allowed concurrency limit via AIMD. Worker threads block in ``batch()`` until a
slot is available (ENFORCE) or pass straight through while logging what the
limit WOULD have been (OBSERVE).
"""

from __future__ import annotations

import contextlib
import enum
import threading
import time

from .config import GovernorConfig
from .policy.aimd import next_limit
from .report import Report, Sample
from .sensors.base import Headroom, Level, Sensor


class Mode(enum.Enum):
    ENFORCE = "enforce"  # actually throttle the job
    OBSERVE = "observe"  # never throttle; just record what it would have done


class Governor:
    def __init__(
        self,
        config: GovernorConfig,
        sensor: Sensor,
        mode: Mode = Mode.ENFORCE,
        report: Report | None = None,
    ) -> None:
        self._cfg = config
        self._sensor = sensor
        self._mode = mode
        self.report = report or Report(label=mode.value)

        self._limit = config.start_limit
        self._in_flight = 0
        self._last_level = Level.GREEN

        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._sampler: threading.Thread | None = None

    # --- lifecycle ---
    def start(self) -> None:
        if self._sampler is not None:
            return
        self._stop.clear()
        self._sampler = threading.Thread(target=self._sample_loop, name="gov-sampler", daemon=True)
        self._sampler.start()

    def stop(self) -> None:
        self._stop.set()
        with self._cv:
            self._cv.notify_all()
        if self._sampler is not None:
            self._sampler.join(timeout=5)
            self._sampler = None
        with contextlib.suppress(Exception):
            self._sensor.close()

    # --- the sampling/control loop ---
    def _sample_loop(self) -> None:
        while not self._stop.is_set():
            try:
                hr = self._sensor.sample()
                sensor_error = None
            except Exception as exc:  # noqa: BLE001 - any sensor failure must fail safe
                # Fail-safe: if we can't see the DB, assume the worst (pause), never
                # plow ahead blind. A frozen-open limit is exactly how backfills take
                # prod down, so an unreadable sensor must clamp, not coast.
                hr = Headroom(level=Level.CRITICAL, active_backends=-1, blocked_backends=-1)
                sensor_error = exc

            new_limit = next_limit(self._limit, hr.level, self._cfg)

            with self._cv:
                old_limit = self._limit
                self._limit = new_limit
                self.report.add_sample(
                    Sample(
                        t=time.time() - self.report.started_at,
                        active_backends=hr.active_backends,
                        blocked_backends=hr.blocked_backends,
                        level=hr.level.name,
                        limit=new_limit,
                        in_flight=self._in_flight,
                    )
                )
                if sensor_error is not None:
                    self.report.add_event(
                        "sensor_error",
                        f"sensor unreadable ({sensor_error!r}) -> fail-safe pause",
                    )
                elif hr.level is Level.CRITICAL and old_limit > 0:
                    self.report.add_event(
                        "circuit_break",
                        f"active={hr.active_backends} blocked={hr.blocked_backends} -> pause",
                    )
                elif new_limit < old_limit:
                    self.report.add_event(
                        "backoff",
                        f"{hr.level.name}: limit {old_limit} -> {new_limit}",
                    )
                self._last_level = hr.level
                # waking workers in case the limit just grew
                self._cv.notify_all()

            self._stop.wait(self._cfg.poll_interval_s)

    # --- the worker entry point ---
    @contextlib.contextmanager
    def batch(self):
        """Acquire a slot, run the batch, release the slot.

        ENFORCE: blocks until ``in_flight < limit``.
        OBSERVE: never blocks; records that it would have throttled.
        """
        self._acquire()
        try:
            yield
        finally:
            self._release()

    def _acquire(self) -> None:
        with self._cv:
            if self._mode is Mode.OBSERVE:
                if self._in_flight >= self._limit:
                    self.report.add_event(
                        "would_throttle",
                        f"in_flight={self._in_flight} >= limit={self._limit}",
                    )
                self._in_flight += 1
                return

            # ENFORCE: wait for a slot (limit may be 0 during a circuit-break).
            while not self._stop.is_set() and self._in_flight >= self._limit:
                self._cv.wait(timeout=1.0)
            self._in_flight += 1

    def _release(self) -> None:
        with self._cv:
            self._in_flight -= 1
            self._cv.notify_all()

    # --- introspection (handy for tests/demo) ---
    @property
    def current_limit(self) -> int:
        return self._limit

    @property
    def mode(self) -> Mode:
        return self._mode

    def set_mode(self, mode: Mode | str) -> Mode:
        """Switch ENFORCE<->OBSERVE at runtime (thread-safe).

        Flipping to OBSERVE wakes any workers currently blocked in ENFORCE so
        they re-check and pass straight through; flipping to ENFORCE simply
        starts gating new acquisitions. Accepts a ``Mode`` or its string value.
        """
        if isinstance(mode, str):
            try:
                mode = Mode(mode)
            except ValueError as exc:
                raise ValueError(f"unknown mode: {mode!r}") from exc
        with self._cv:
            self._mode = mode
            self._cv.notify_all()
        return mode

    def snapshot(self, window: int = 240, event_window: int = 40) -> dict:
        """A thread-safe, JSON-ready view of the governor's live state.

        Reads the volatile scalars (limit, in-flight, last level) under the
        same condition the sampler/worker threads mutate them, then merges the
        recent timeline from the report. Safe to call from another thread (e.g.
        a dashboard HTTP handler) while the governor is running.
        """
        with self._cv:
            live = {
                "mode": self._mode.value,
                "limit": self._limit,
                "in_flight": self._in_flight,
                "last_level": self._last_level.name,
                "running": self._sampler is not None and not self._stop.is_set(),
                "max_limit": self._cfg.max_limit,
                "min_limit": self._cfg.min_limit,
            }
        timeline = self.report.snapshot(window=window, event_window=event_window)
        return {**live, **timeline}
