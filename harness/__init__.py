"""Harness package: reproduces a 'backfill saturates the DB' incident locally.

Everything here uses synthetic, generated data only.
"""

import os

DSN = os.environ.get(
    "GOV_DEMO_DSN", "postgresql://gov:govpass@localhost:5544/govdemo"
)
