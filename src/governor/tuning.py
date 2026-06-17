"""Self-service tuning: let migration owners adjust pacing knobs at runtime.

The whole point of the product is that a migration squad can pilot it WITHOUT a
DBA setting up a policy for them. These helpers define the small, safe set of
knobs that can be changed live from the dashboard (or an API call) and validate
each one, so a team can dial their own pace — raise ``max_limit`` when the DB has
room, flip ``pause_on_critical`` off for throttle-only — without a restart or a
config-file round-trip through another team's backlog.

Only AIMD/agent-owned knobs are exposed (never the DSN or enforcement
credentials). Each is range-checked; an out-of-range or unknown key is rejected
with a clear message rather than silently applied.
"""

from __future__ import annotations

from dataclasses import replace

from .config import GovernorConfig

# name -> (caster, validator, human description). Validator returns True if OK.
_KNOBS: dict[str, tuple] = {
    "min_limit": (int, lambda v: v >= 1, "minimum in-flight batches (>=1)"),
    "max_limit": (int, lambda v: v >= 1, "maximum in-flight batches (>=1)"),
    "additive_increase": (int, lambda v: v >= 1, "AIMD additive increase (>=1)"),
    "decrease_factor": (float, lambda v: 0.0 < v < 1.0, "AIMD decrease factor (0,1)"),
    "poll_interval_s": (float, lambda v: 0.01 <= v <= 10.0, "sensor poll interval seconds [0.01,10]"),
    "pause_on_critical": (bool, lambda v: True, "pause fully at CRITICAL (else throttle-only)"),
}


def tuning_view(cfg: GovernorConfig) -> dict:
    """The current value of every live-tunable knob (JSON-ready)."""
    return {name: getattr(cfg, name) for name in _KNOBS}


def _coerce(name: str, value, caster) -> object:
    if caster is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(value)
    return caster(value)


def apply_tuning(cfg: GovernorConfig, updates: dict) -> GovernorConfig:
    """Return a new config with validated ``updates`` applied.

    Raises ``ValueError`` on an unknown key, an uncastable value, or a value that
    fails its range check — and on the cross-field invariant ``min_limit <=
    max_limit`` — so a bad edit from the UI is rejected, never half-applied.
    """
    if not isinstance(updates, dict) or not updates:
        raise ValueError("tuning updates must be a non-empty object")
    coerced: dict[str, object] = {}
    for name, raw in updates.items():
        if name not in _KNOBS:
            raise ValueError(f"unknown or non-tunable knob: {name!r}")
        caster, validator, desc = _KNOBS[name]
        try:
            value = _coerce(name, raw, caster)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name}: not a valid value ({desc})") from exc
        if not validator(value):
            raise ValueError(f"{name}: out of range — {desc}")
        coerced[name] = value

    new_min = coerced.get("min_limit", cfg.min_limit)
    new_max = coerced.get("max_limit", cfg.max_limit)
    if new_min > new_max:
        raise ValueError(f"min_limit ({new_min}) must be <= max_limit ({new_max})")
    return replace(cfg, **coerced)
