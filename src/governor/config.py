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

    # --- Secondary signals (optional; ``None`` disables that signal) ---
    # These let a sensor escalate the headroom level on signals other than raw
    # active-backend count. A breach can only raise the level (never lower it),
    # so adding them is always at least as conservative. Wired up by sensors that
    # can read them (e.g. the Postgres sensor reads replication lag / connection
    # saturation; the Mongo sensor reads replica-set lag and connection counts).

    # Replication lag (seconds) — protects read replicas / Aurora reader fleets
    # that a heavy backfill can stall. The first breached bound wins.
    yellow_replication_lag_s: float | None = None
    red_replication_lag_s: float | None = None
    critical_replication_lag_s: float | None = None

    # Connection-pool saturation as a fraction in [0, 1] of used/max connections.
    # Running out of connections is its own outage mode (the May 2025 / 2020
    # "consumed all sessions" incidents), independent of active-query count.
    yellow_conn_pool_frac: float | None = None
    red_conn_pool_frac: float | None = None
    critical_conn_pool_frac: float | None = None

    # Query latency (p99, milliseconds) measured at the DB layer by the sensor's
    # own probe — the canary that degrades when a backfill saturates the engine.
    # Lets the governor react to *user-visible slowness* directly, not just to
    # backend counts. The first breached bound wins.
    yellow_query_latency_ms: float | None = None
    red_query_latency_ms: float | None = None
    critical_query_latency_ms: float | None = None


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

    # Throttle-only vs pause. When True (default) a CRITICAL reading drops the
    # limit to 0 — a full circuit-break (pause-all). When False the policy never
    # fully stops the job: even at CRITICAL it only multiplicatively *decreases*
    # toward ``min_limit``. This is the "slow down, don't stop" mode for teams
    # that have proven they can already manually halt a migration and instead
    # want adaptive throttling with no 2am "restart the job" page.
    pause_on_critical: bool = True

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
