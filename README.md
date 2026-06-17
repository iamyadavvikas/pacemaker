# pacing-governor

Runtime governor that throttles database **migration / backfill jobs** against
**live DB headroom** — so a data migration can't take production down.

This is the wedge that incident retros keep recommending ("rate-limit / gradually
ramp up the migration job") and that almost nobody ships as a durable, enforced
control. Existing tools (Atlas, Squawk) statically lint *schema* changes; this
governs *job throughput at runtime*.

## How it works

A closed feedback loop, modelled on TCP congestion control (AIMD):

- **Sense** — a read-only `PostgresSensor` polls `pg_stat_activity` for active and
  lock-waiting backends and maps them to GREEN / YELLOW / RED / CRITICAL.
- **Decide** — AIMD adjusts the allowed number of in-flight batches: additive
  increase while green, multiplicative decrease on red, **pause** on critical.
- **Act** — backfill workers call `with governor.batch():`; the governor blocks
  them until there's headroom (ENFORCE) or just records what it *would* have done
  (OBSERVE — the zero-risk way to land in a skeptical team).

## Demo: reproduce a 'backfill saturates the DB' incident, then fix it

Synthetic data only. Requires Docker + Python 3.11+.

```bash
# 1. start a small, CPU-limited Postgres
docker compose up -d db   # Postgres on host port 5544

# 2. set up the environment
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 3. run the demo (seeds data, runs ungoverned vs governed, prints a scorecard)
python -m harness.demo
```

You'll see the same backfill run twice while a checkout probe measures
customer-facing latency: ungoverned spikes p99, governed keeps it flat.

```bash
docker compose down -v   # tear down
```

## Live dashboard

Watch the governor react in real time in your browser: a GREEN/YELLOW/RED/CRITICAL
status light, the live AIMD concurrency limit, active/blocked DB backends, checkout
latency, and a feed of every backoff / circuit-break decision. It runs a looping
governed backfill so there's always something to watch.

On startup it first runs a short (~8s) **ungoverned warmup** to capture an honest
"no governor" baseline on the same machine and data. That baseline drives:

- a faint **ghost band** (ungoverned p50→max latency + a dashed p99 line) behind the
  live latency chart, so the governed line visibly sits below it;
- an **"outage avoided"** counter (lock-jam samples prevented, ungoverned → governed).

A header button flips **ENFORCE ⇄ OBSERVE** live (no restart). Pass `--no-calibrate`
to skip the warmup.

**Locally** (with the `db` service running and the package installed):

```bash
docker compose up -d db
python -m harness.live              # ENFORCE — actually throttles
# or: python -m harness.live --mode observe   # logs would-throttle, never blocks
# or: governor-live                  # installed console entry point
```

Then open <http://127.0.0.1:8765>. Ctrl-C to stop.

**In a container** (builds the image, binds `0.0.0.0`, reaches the DB over the
compose network):

```bash
docker compose up --build dashboard   # dashboard on http://localhost:8765
```

Configurable via env: `GOV_DASH_HOST` (default `127.0.0.1`; `0.0.0.0` in the
container), `GOV_DASH_PORT` (default `8765`), `GOV_DASH_TOKEN` (auth password —
unset = open), `GOV_MODE` (`enforce` | `observe`), `GOV_DEMO_DSN`.

> **Auth gate.** The dashboard exposes headroom counts and a control endpoint
> (`POST /api/mode`, the ENFORCE⇄OBSERVE toggle) that changes governor behavior.
> Set `GOV_DASH_TOKEN` to require **HTTP Basic auth** on every route except the
> `/api/health` liveness probe (any username; password = the token). With no token
> it stays open, so keep it bound to localhost or a private network until a token is
> set — always set one before any non-local / cloud exposure.

```bash
GOV_DASH_TOKEN=choose-a-strong-secret python -m harness.live
```

## `dbguard observe` — read-only sidecar (observe-only)

The library above is in-process: workers call `with governor.batch():`. The
**observer** is the productized form — a standalone, **read-only** sidecar you
point at a live database. It attributes backends into a **migration cohort** vs
protected **prod traffic**, runs the same AIMD policy **in shadow** (recording
exactly when it *would* have throttled the migration), and **never** blocks,
cancels, or writes anything. It's the zero-risk way to prove value before any
enforcement is switched on.

It also publishes a **gh-ost / pt-osc-compatible throttle signal** at
`GET /throttle` (HTTP `200` = proceed, `429` = back off), so a migration tool you
already run can consult it natively, and the same agent governs the app-level
backfill jobs those tools can't.

```bash
# watch the bundled synthetic multi-squad demo (migration + checkout share a role,
# attributed apart by application_name / query tag):
docker compose up -d db
python -m harness.observe --demo
# -> dashboard at http://127.0.0.1:8765, throttle signal at /throttle

# against your own database (use a read-only role, e.g. pg_monitor):
dbguard observe \
  --dsn postgresql://gov_sensor:pw@host:5432/app \
  --migration-user backfill_job \
  --report reports/observe.json
```

Attribution signals are consulted by precedence (most reliable first):
`--migration-user` (role) → `--migration-tag` (an inline `/* dbguard:migration */`
SQL comment) → `--migration-app` (`application_name`). On exit, `--report` writes
an evidence summary (peak migration concurrency, would-throttle count, projected
safe pace) — the leave-behind for a "would this have helped?" review.

Point a real migration tool at it:

```bash
gh-ost ... --throttle-http=http://127.0.0.1:8765/throttle
```

## Layout

```
src/governor/        # the library
  attribution.py       migration-vs-prod cohort classifier (signal precedence)
  sensors/postgres.py  read-only headroom sensor (+ per-cohort attribution)
  policy/aimd.py       AIMD pacing policy
  governor.py          in-process control loop + ENFORCE/OBSERVE gating
  observer.py          standalone OBSERVE-only shadow pacer + throttle verdict
  report.py            timeline + run summary
  dashboard.py         stdlib HTTP server (+ gh-ost-compatible /throttle)
  web/index.html       self-contained dashboard UI (Chart.js)
harness/             # the local incident reproduction / demo (synthetic data)
  demo.py              ungoverned-vs-governed scorecard run
  live.py              looping governed backfill + live dashboard
  observe.py           `dbguard observe` CLI (read-only sidecar)
  multicohort.py       synthetic migration + checkout load for the observer demo
```

## Production note

The sensor should connect with a **read-only** role (e.g. `pg_monitor`). The
governor never writes to or locks the target database.
