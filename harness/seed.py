"""Seed a synthetic OLTP-ish schema.

- ``orders``   : small hot table standing in for the checkout path.
- ``projects`` : large table the backfill rewrites (stands in for a data migration).
"""

from __future__ import annotations

import sys

import psycopg

from . import DSN

N_ORDERS = 5_000
N_PROJECTS = 800_000


def seed(dsn: str = DSN) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders;")
        cur.execute("DROP TABLE IF EXISTS projects;")
        cur.execute(
            """
            CREATE TABLE orders (
                id          int PRIMARY KEY,
                status      text NOT NULL,
                updated_at  timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE projects (
                id         int PRIMARY KEY,
                state      text NOT NULL,
                data       text NOT NULL,
                touched_at timestamptz
            );
            """
        )
        print(f"seeding {N_ORDERS} orders ...", flush=True)
        cur.execute(
            """
            INSERT INTO orders (id, status)
            SELECT g, 'open' FROM generate_series(1, %s) AS g;
            """,
            (N_ORDERS,),
        )
        print(f"seeding {N_PROJECTS} projects ...", flush=True)
        cur.execute(
            """
            INSERT INTO projects (id, state, data)
            SELECT g, 'v2', md5(g::text) || md5((g + 1)::text)
            FROM generate_series(1, %s) AS g;
            """,
            (N_PROJECTS,),
        )
        cur.execute("VACUUM (ANALYZE) orders;")
        cur.execute("VACUUM (ANALYZE) projects;")
    print("seed complete.", flush=True)


if __name__ == "__main__":
    seed(sys.argv[1] if len(sys.argv) > 1 else DSN)
