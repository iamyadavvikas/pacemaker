"""Configuration models for the governor."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Thresholds:
    """Maps raw DB signals to a headroom level.

    Tuned for a 1-CPU demo Postgres. ``active_backends`` is the dominant signal:
    the number of concurrently-executing queries. Heavy backfill workers show up
    here directly, so capping in-flight batches caps this number.
    """

    green_max_active: int = 2
    yellow_max_active: int = 3
    critical_active: int = 6
    # Any blocked (lock-waiting) backend forces at least RED.
    blocked_forces_red: bool = True


@dataclass(frozen=True)
class GovernorConfig:
    dsn: str
    poll_interval_s: float = 0.25
    # AIMD bounds on concurrent in-flight batches.
    min_limit: int = 1
    max_limit: int = 6
    start_limit: int = 1  # start low and ramp up == the "gradual ramp-up" everyone recommends
    additive_increase: int = 1
    decrease_factor: float = 0.5
    thresholds: Thresholds = field(default_factory=Thresholds)

    # --- Stage 2 enforcement (EnforcerAgent only; ignored by Governor/Observer) ---
    # A SEPARATE privileged/write connection used only to apply back-pressure to
    # the migration cohort (e.g. pg_cancel_backend). Never used to read health and
    # never in the prod data path. Falls back to ``dsn`` if unset (fine for the
    # demo, where the same role owns the synthetic migration backends).
    control_dsn: str | None = None
    # Role the migration job logs in as, for ALTER ROLE-based mechanisms.
    migration_role: str | None = None
    # Back-pressure mechanism: "cancel" (pg_cancel_backend the excess backends).
    enforce_mechanism: str = "cancel"
    # Max backends cancelled per poll, so pacing is gradual, not a thundering kill.
    max_cancels_per_interval: int = 2
    # Refuse to cancel a backend matched only on a weak/spoofable signal
    # (application_name). Only strong signals (usename / query_tag) are actioned.
    require_strong_signal: bool = True
    # Enforcer self circuit-breaker: if more than this many cancels fire within
    # ``runaway_window_s``, stop enforcing and alert (runaway guard).
    runaway_cancel_threshold: int = 50
    runaway_window_s: float = 10.0
