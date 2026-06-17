"""AIMD pacing policy.

Additive-increase / multiplicative-decrease control of the concurrency limit
(in-flight batches), modelled on TCP congestion control. Start low, ramp up
while the DB has headroom, back off hard the moment it doesn't, pause on breach.
"""

from __future__ import annotations

import math

from ..config import GovernorConfig
from ..sensors.base import Level


def next_limit(current: int, level: Level, cfg: GovernorConfig) -> int:
    """Compute the next concurrency limit given the current one and headroom."""
    if level is Level.CRITICAL:
        if cfg.pause_on_critical:
            return 0  # circuit-break: pause all new batches
        # Throttle-only: never fully stop, just decrease hard toward min_limit.
        return max(cfg.min_limit, math.floor(current * cfg.decrease_factor))
    if level is Level.RED:
        decreased = math.floor(current * cfg.decrease_factor)
        return max(cfg.min_limit, decreased)
    if level is Level.YELLOW:
        return max(cfg.min_limit, current)  # hold
    # GREEN
    return min(cfg.max_limit, current + cfg.additive_increase)
