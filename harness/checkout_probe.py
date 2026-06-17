"""Checkout latency probe.

Continuously runs a small representative transaction and records its latency.
This is the customer-facing 'checkout' experience we are protecting; the
migration/backfill must not be allowed to tank its p99.

By default it runs the bundled read+touch transaction against ``orders``. Pass
``probe_sql`` to run a caller-supplied statement instead — e.g. a cheap,
read-only ``SELECT`` against a hot table when probing a real target database
where writes are not acceptable.
"""

from __future__ import annotations

import random
import threading
import time

import psycopg

from . import DSN


class CheckoutProbe:
    def __init__(
        self,
        dsn: str = DSN,
        interval_s: float = 0.01,
        probe_sql: str | None = None,
    ) -> None:
        self._dsn = dsn
        self._interval = interval_s
        self._probe_sql = probe_sql
        self._latencies_ms: list[float] = []
        self._series: list[tuple[float, float]] = []  # (t_seconds, latency_ms)
        self._t0 = time.perf_counter()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="checkout-probe", daemon=True)

    def start(self) -> None:
        self._t0 = time.perf_counter()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _default_txn(self, cur) -> None:
        # representative checkout txn: read the order, touch it.
        oid = random.randint(1, 5_000)
        cur.execute("SELECT status FROM orders WHERE id = %s;", (oid,))
        cur.fetchone()
        cur.execute("UPDATE orders SET updated_at = now() WHERE id = %s;", (oid,))

    def _custom_txn(self, cur) -> None:
        # caller-supplied probe (expected read-only against a real target DB).
        cur.execute(self._probe_sql)
        if cur.description is not None:
            cur.fetchall()

    def _run(self) -> None:
        txn = self._custom_txn if self._probe_sql else self._default_txn
        with psycopg.connect(self._dsn, autocommit=True) as conn:
            while not self._stop.is_set():
                t0 = time.perf_counter()
                with conn.cursor() as cur:
                    txn(cur)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                self._latencies_ms.append(elapsed_ms)
                self._series.append((t0 - self._t0, elapsed_ms))
                self._stop.wait(self._interval)

    @property
    def series(self) -> list[tuple[float, float]]:
        """Timestamped (seconds_since_start, latency_ms) samples for plotting."""
        return list(self._series)

    def latency_payload(self, window_s: float = 40.0, buckets: int = 120) -> dict:
        """Bucketed rolling p95 of recent checkout latency, for the live chart.

        Raw per-request samples (interval ~10ms) are far too noisy to plot
        directly, so we window the last ``window_s`` seconds and emit one p95 per
        time bucket.
        """
        series = self.series
        if not series:
            return {"latency_series": []}
        t_now = series[-1][0]
        recent = [(t, ms) for t, ms in series if t >= t_now - window_s]
        if len(recent) < 2:
            return {"latency_series": [[round(t, 2), round(ms, 1)] for t, ms in recent]}

        t_start = recent[0][0]
        span = max(recent[-1][0] - t_start, 1e-6)
        width = span / buckets
        by_bucket: dict[int, list[float]] = {}
        for t, ms in recent:
            b = min(int((t - t_start) / width), buckets - 1)
            by_bucket.setdefault(b, []).append(ms)

        out: list[list[float]] = []
        for b in sorted(by_bucket):
            vals = sorted(by_bucket[b])
            k = (len(vals) - 1) * 0.95
            lo = int(k)
            hi = min(lo + 1, len(vals) - 1)
            p95 = vals[lo] * (1 - (k - lo)) + vals[hi] * (k - lo)
            t_center = t_start + (b + 0.5) * width
            out.append([round(t_center, 2), round(p95, 1)])
        return {"latency_series": out}


    # --- metrics ---
    def percentile(self, p: float) -> float:
        if not self._latencies_ms:
            return 0.0
        data = sorted(self._latencies_ms)
        k = (len(data) - 1) * (p / 100.0)
        lo = int(k)
        hi = min(lo + 1, len(data) - 1)
        frac = k - lo
        return data[lo] * (1 - frac) + data[hi] * frac

    def summary(self) -> dict:
        return {
            "samples": len(self._latencies_ms),
            "p50_ms": round(self.percentile(50), 1),
            "p95_ms": round(self.percentile(95), 1),
            "p99_ms": round(self.percentile(99), 1),
            "max_ms": round(max(self._latencies_ms), 1) if self._latencies_ms else 0.0,
        }
