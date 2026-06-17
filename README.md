# pacing-governor

Runtime governor that throttles database **migration / backfill jobs** against
**live DB headroom** — so a data migration can't take production down.

This is the wedge that incident retros keep recommending ("rate-limit / gradually
ramp up the migration job") and that almost nobody ships as a durable, enforced
control. Existing tools (Atlas, Squawk) statically lint *schema* changes; this
governs *job throughput at runtime*.

## Problem Statement

**The scenario:** You have a production database (Postgres or MongoDB) serving live
customer traffic. You need to run a large data migration or backfill job — say,
backfilling millions of rows.

**What goes wrong:** The migration hammers the DB, exhausts connections, and causes
lock contention. Customer-facing queries slow to a crawl or time out. You've just
taken production down with a backfill.

**Why it keeps happening:**
- Every incident retro recommends "rate-limit the migration job" or "gradually ramp it up"
- But nobody ships a *durable, enforced* control — it's ad-hoc `sleep()` calls or manual "go/stop" Slack commands
- Existing tools (Atlas, Squawk) lint *schema changes* statically; nothing governs *job throughput at runtime*

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Production Database                            │
│  ┌─────────────┐                                      ┌─────────────┐       │
│  │ Prod Traffic│ ──────────────┐      ┌───────────────│  Migration  │       │
│  └─────────────┘               │      │               │    Job      │       │
│                                ▼      ▼               └─────────────┘       │
│                         ┌─────────────────┐                  ▲              │
│                         │   PostgreSQL    │                  │              │
│                         │    / MongoDB    │                  │              │
│                         └────────┬────────┘                  │              │
│                                  │                           │              │
└──────────────────────────────────┼───────────────────────────┼──────────────┘
                                   │                           │
                     pg_stat_activity / currentOp              │
                                   │                           │
                                   ▼                           │
┌──────────────────────────────────────────────────────────────┼──────────────┐
│                         Pacing Governor                      │              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┴───────────┐  │
│  │     Sensor      │───▶│   AIMD Policy   │───▶│    Governor / Agent     │  │
│  │ (Postgres/Mongo)│    │  (GREEN→RED)    │    │ (block / cancel / log)  │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘  │
│         │                                                    │              │
│         │ Headroom{level, active, blocked, lag}              │              │
│         ▼                                                    ▼              │
│  ┌─────────────────┐                              ┌─────────────────┐       │
│  │    Dashboard    │                              │    Notifiers    │       │
│  │  /api/state     │                              │  (Slack, SFx)   │       │
│  │  /throttle      │                              └─────────────────┘       │
│  └─────────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Control Loop (TCP Congestion-Style AIMD)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  1️⃣  SENSE                2️⃣  DECIDE               3️⃣  ACT                  │
│  ─────────────────────    ─────────────────────    ─────────────────────     │
│  Poll DB every N sec      What level?              Update concurrency limit  │
│  Count active backends    ├─ GREEN  → +1 limit     Block waiting workers     │
│  Check blocked queries    ├─ YELLOW → hold         Or cancel excess backends │
│  Measure replication lag  ├─ RED    → limit × 0.5  Notify (Slack, etc.)      │
│  Track latency p99        └─ CRITICAL → pause                                │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Health Levels

| Level | Condition | AIMD Action |
|-------|-----------|-------------|
| 🟢 **GREEN** | active < threshold, no blocked, lag < 1s | **Increase** limit +1 |
| 🟡 **YELLOW** | approaching threshold, or lag 1-5s | **Hold** limit |
| 🔴 **RED** | over threshold, or blocked > 0, or lag > 5s | **Decrease** limit × 0.5 |
| ⚫ **CRITICAL** | severe overload | **Pause** (limit → 0) or throttle to min |

### Three Operating Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **OBSERVE** | Shadow mode — logs what it *would* throttle, never blocks | Zero-risk proof-of-value |
| **ENFORCE (Library)** | Workers call `gov.batch()`, blocked until headroom | In-process backfill jobs |
| **ENFORCE (Canceller)** | Out-of-band `pg_cancel_backend()` / `killOp()` | Non-cooperative migrations |

### Workload Attribution

The governor surgically targets only the migration cohort, **never touching prod queries**:

```
Signal Precedence (strongest → weakest):
  usename (DB role)  →  query_tag (SQL comment)  →  application_name
```

## How it works

A closed feedback loop, modelled on TCP congestion control (AIMD):

- **Sense** — a read-only `PostgresSensor` polls `pg_stat_activity` for active and
  lock-waiting backends and maps them to GREEN / YELLOW / RED / CRITICAL.
- **Decide** — AIMD adjusts the allowed number of in-flight batches: additive
  increase while green, multiplicative decrease on red, **pause** on critical.
- **Act** — backfill workers call `with governor.batch():`; the governor blocks
  them until there's headroom (ENFORCE) or just records what it *would* have done
  (OBSERVE — the zero-risk way to land in a skeptical team).

## Databases

The control loop is database-agnostic; sensors translate each engine's health
into the same GREEN/YELLOW/RED/CRITICAL signal.

- **PostgreSQL** (`PostgresSensor`) — reads `pg_stat_activity`; enforced via
  `pg_cancel_backend` (`PostgresCanceller`).
- **MongoDB / Atlas** (`MongoSensor`) — reads `currentOp` (active / lock-waiting
  ops), `serverStatus` (connections) and `replSetGetStatus` (replica-set lag);
  enforced via `killOp` (`MongoKiller`). Attribution maps onto the same signal
  precedence (effective user → `comment` tag → `appName`). `pymongo` is an
  optional dependency: `pip install -e '.[mongo]'`.

Both sensors read the same **secondary signals** when configured, and a breach on
any of them can only *raise* the level (never lower it):

- **replication lag** (`*_replication_lag_s` thresholds) — protect read replicas /
  Aurora readers a heavy backfill can stall;
- **connection-pool saturation** (`*_conn_pool_frac` thresholds, used/max) — catch
  the "consumed all sessions" outage mode independent of active-query count;
- **query latency p99** (`*_query_latency_ms` thresholds) — the sensor times its own
  read-only probe round-trip every poll and tracks a rolling p99, so the governor
  reacts to *user-visible slowness* directly (no `pg_stat_statements` grant or engine
  extension required — the probe is the canary).

### Throttle-only mode (slow down, don't stop)

By default CRITICAL is a full circuit-break (limit → 0, pause-all). Set
`GovernorConfig(pause_on_critical=False)` for **adaptive throttling that never
fully stops** the job: even at CRITICAL it only decreases toward `min_limit`, so
there's no 2am "the migration paused, go restart it" page — it just runs slower
until the DB recovers, then ramps back up.

### Alerting (the durable evidence trail)

Pass a `notifier` to `ObserverAgent` / `EnforcerAgent` to push the high-signal
decisions (`throttle_started`, `would_circuit_break`, `enforcer_tripped`,
`sensor_error`) to where on-call sees them. Notifiers are stdlib-only and
fire-and-forget (never add latency to the loop):

- `SlackNotifier(webhook_url=...)` — or any `{"text": ...}` webhook sink;
- `SignalFxNotifier(token=..., realm=...)` — custom events for charts/detectors;
- `MultiNotifier(...)` — fan out to several; `CallbackNotifier(fn)` — custom.

```python
from governor import ObserverAgent, GovernorConfig, MongoSensor, SlackNotifier, CohortClassifier

agent = ObserverAgent(
    GovernorConfig(dsn="mongodb://...", pause_on_critical=False),
    MongoSensor("mongodb://gov_ro@host/?readPreference=secondaryPreferred",
                classifier=CohortClassifier.from_lists(usenames=["backfill_job"])),
    notifier=SlackNotifier(webhook_url="https://hooks.slack.com/services/..."),
)
agent.start()
```

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

> **Auth gate.** The dashboard exposes headroom counts and control endpoints
> (`POST /api/mode`, the ENFORCE⇄OBSERVE toggle; `POST /api/tuning`, the live
> pacing knobs) that change governor behavior. Set `GOV_DASH_TOKEN` to require
> **HTTP Basic auth** on every route except the `/api/health` liveness probe (any
> username; password = the token). With no token it stays open, so keep it bound to
> localhost or a private network until a token is set — always set one before any
> non-local / cloud exposure.

```bash
GOV_DASH_TOKEN=choose-a-strong-secret python -m harness.live
```

### Self-service tuning (dial your own pace, no restart)

The dashboard has a **Tuning** panel (and a `POST /api/tuning` endpoint) so a
migration squad can adjust the pacing knobs live — `min_limit`, `max_limit`,
`additive_increase`, `decrease_factor`, `poll_interval_s`, and the
`pause_on_critical` (throttle-only) toggle — without a config-file round-trip
through another team. Every change is range-checked and the cross-field invariant
`min_limit <= max_limit` is enforced; a bad edit is rejected with a clear message,
never half-applied. The DSN and enforcement credentials are **never** exposed.

```bash
# raise the ceiling and switch to throttle-only, live:
curl -X POST localhost:8765/api/tuning \
  -H 'content-type: application/json' \
  -d '{"max_limit": 16, "pause_on_critical": false}'
# -> {"tuning": {"max_limit": 16, "pause_on_critical": false, ...}}
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

# against your own Postgres (use a read-only role — deploy/governor_readonly_role.sql):
dbguard observe \
  --dsn postgresql://gov_sensor:pw@host:5432/app \
  --migration-user backfill_job \
  --report reports/observe.json

# against MongoDB / Atlas (engine auto-detected from the mongodb:// scheme; use a
# read-only clusterMonitor user — deploy/governor_readonly_role.js — on a secondary):
dbguard observe \
  --dsn "mongodb://gov_sensor:pw@host:27017/?readPreference=secondaryPreferred" \
  --migration-tag dbguard:migration \
  --report reports/observe-mongo.json
```

The engine is **auto-detected from the DSN scheme** (`mongodb://` /
`mongodb+srv://` → MongoDB, otherwise Postgres) — one read-only sidecar, both
engines. The Mongo path seeds nothing and writes nothing.

Two flags help against a real target:

- `--calibrate SECONDS` first observes ambient load read-only and derives the
  GREEN/YELLOW/CRITICAL active-backend thresholds from *this* database's baseline
  (the shipped defaults are tuned for a 1-CPU demo and read RED at idle on a busy
  cluster);
- `--track-secondary` also reads replication lag + connection-pool saturation
  (Postgres reads these only when enabled; Mongo reads them by default).

Attribution signals are consulted by precedence (most reliable first):
`--migration-user` (role) → `--migration-tag` (an inline `/* dbguard:migration */`
SQL comment, or a Mongo op `comment`) → `--migration-app` (`application_name` /
driver `appName`). On exit, `--report` writes an evidence summary (peak migration
concurrency, would-throttle count, projected safe pace) — the leave-behind for a
"would this have helped?" review.

Run it as a **read-only container sidecar** (profile-gated; set `GOV_DASH_TOKEN`
and a read-only `GOV_DEMO_DSN` for a real target):

```bash
docker compose --profile observe up observe   # dashboard on http://localhost:8765
```

Point a real migration tool at it:

```bash
gh-ost ... --throttle-http=http://127.0.0.1:8765/throttle
```

### MongoDB demo (`harness.mongodemo`)

The Mongo counterpart to `harness.live` — a governed Mongo backfill you can watch
in the browser. It drives a synthetic migration cohort (workers rewriting
documents, each op tagged `comment="dbguard:migration"` and `appName=
dbguard_demo_migration`) plus light prod traffic against a small, CPU-limited
MongoDB, while a read-only `MongoSensor` shadow-paces it and serves the same
dashboard + gh-ost-compatible `/throttle` endpoint. Requires `pymongo`.

```bash
pip install -e '.[mongo]'
docker compose up -d mongo            # Mongo on host port 27017
python -m harness.mongodemo           # OBSERVE — read-only, throttle-only
# or: governor-mongo                  # installed console entry point
# -> dashboard at http://127.0.0.1:8765, throttle signal at /throttle
docker compose down -v
```

## Layout

```
src/governor/        # the library
  attribution.py       migration-vs-prod cohort classifier (signal precedence)
  sensors/postgres.py  read-only headroom sensor (+ per-cohort attribution)
  sensors/mongo.py     read-only MongoDB/Atlas sensor (+ killOp enforcement)
  policy/aimd.py       AIMD pacing policy (+ throttle-only mode)
  governor.py          in-process control loop + ENFORCE/OBSERVE gating
  observer.py          standalone OBSERVE-only shadow pacer + throttle verdict
  enforcer.py          non-cooperative enforcer (cancel/killOp the migration cohort)
  notify.py            alerting hooks (Slack / SignalFx / webhook), fire-and-forget
  tuning.py            self-service live pacing knobs (validated; powers /api/tuning)
  report.py            timeline + run summary
  dashboard.py         stdlib HTTP server (+ gh-ost-compatible /throttle, /api/tuning)
  web/index.html       self-contained dashboard UI (Chart.js)
harness/             # the local incident reproduction / demo (synthetic data)
  demo.py              ungoverned-vs-governed scorecard run
  live.py              looping governed backfill + live dashboard
  observe.py           `dbguard observe` CLI (read-only sidecar)
  multicohort.py       synthetic migration + checkout load for the observer demo
  mongodemo.py         synthetic governed MongoDB backfill + live dashboard
```

## Production note

The sensor should connect with a **read-only** role (e.g. `pg_monitor`). The
governor never writes to or locks the target database.
