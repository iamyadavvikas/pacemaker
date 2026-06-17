# Architecture

How **pacing-governor** senses live database health and throttles migration /
backfill jobs so they can't take production down. A closed feedback loop modelled
on TCP congestion control (AIMD): **sense → decide → act**.

## High-Level Architecture

```mermaid
flowchart TB
    subgraph Database["Production Database"]
        PG[(PostgreSQL/MongoDB)]
        PROD[Prod Traffic]
        MIG[Migration Job]
        PROD --> PG
        MIG --> PG
    end

    subgraph Governor["Pacing Governor"]
        SENSOR[Sensor]
        AIMD[AIMD Policy]
        AGENT[Agent]
        SENSOR -->|Headroom| AIMD
        AIMD -->|Limit| AGENT
    end

    PG -.->|pg_stat_activity<br>currentOp| SENSOR
    AGENT -->|Blocks/Cancels| MIG
    AGENT -.->|Never touches| PROD
```

## The Control Loop (TCP Congestion-Style)

```mermaid
flowchart LR
    subgraph SENSE["1.SENSE"]
        S1[Poll DB every N sec]
        S2[Count active backends]
        S3[Check blocked queries]
        S4[Measure replication lag]
        S5[Track latency p99]
        S1 --> S2 --> S3 --> S4 --> S5
    end

    subgraph DECIDE["2.DECIDE"]
        D1{What level?}
        D2[GREEN: +1 limit]
        D3[YELLOW: hold]
        D4[RED: limit x 0.5]
        D5[CRITICAL: pause]
        D1 -->|healthy| D2
        D1 -->|warning| D3
        D1 -->|overloaded| D4
        D1 -->|emergency| D5
    end

    subgraph ACT["3.ACT"]
        A1[Update limit]
        A2[Block waiting workers]
        A3[Or cancel excess backends]
    end

    S5 --> D1
    D2 & D3 & D4 & D5 --> A1 --> A2
    A1 --> A3
```

## Three Operating Modes

```mermaid
flowchart TB
    subgraph MODE1["OBSERVE (Zero Risk)"]
        O1[Sensor samples DB]
        O2[Computes would-throttle]
        O3[Logs only, no blocking]
        O4["/throttle endpoint (advisory)"]
        O1 --> O2 --> O3 --> O4
    end

    subgraph MODE2["ENFORCE - Library"]
        E1["Worker calls gov.batch()"]
        E2{in_flight < limit?}
        E3[Run batch]
        E4[Wait for slot]
        E1 --> E2
        E2 -->|Yes| E3
        E2 -->|No| E4 --> E2
    end

    subgraph MODE3["ENFORCE - Canceller"]
        C1[Enforcer watches backends]
        C2{Migration cohort<br>over limit?}
        C3["pg_cancel_backend(pid)"]
        C4[Pass]
        C1 --> C2
        C2 -->|Yes| C3
        C2 -->|No| C4
    end
```

| Mode | Description | Use Case |
|------|-------------|----------|
| **OBSERVE** | Shadow mode — logs what it *would* throttle, never blocks | Zero-risk proof-of-value |
| **ENFORCE (Library)** | Workers call `gov.batch()`, blocked until headroom | In-process backfill jobs |
| **ENFORCE (Canceller)** | Out-of-band `pg_cancel_backend()` / `killOp()` | Non-cooperative migrations |

## Signal Flow: Sense -> Decide -> Act

```mermaid
sequenceDiagram
    participant DB as Database
    participant Sensor as PostgresSensor/MongoSensor
    participant AIMD as AIMD Policy
    participant Agent as Governor/Observer/Enforcer
    participant Worker as Migration Worker
    participant Notifier as Slack/SignalFx

    loop Every poll_interval (1s default)
        Sensor->>DB: SELECT from pg_stat_activity
        DB-->>Sensor: {active: 8, blocked: 2, lag: 0.3s}
        Sensor->>Sensor: level_from_signals() -> RED
        Sensor->>AIMD: Headroom{level=RED, ...}
        AIMD->>AIMD: next_limit(10, RED) -> 5
        AIMD-->>Agent: new_limit = 5
        Agent->>Agent: broadcast condition variable
        Agent->>Notifier: throttle_started event
    end

    Worker->>Agent: batch() acquire
    Agent-->>Worker: blocked until in_flight < 5
    Worker->>DB: run migration batch
    Worker->>Agent: batch() release
```

## Health Levels

| Level | Condition | AIMD Action |
|-------|-----------|-------------|
| 🟢 **GREEN** | active < threshold, no blocked, lag < 1s | **Increase** limit +1 |
| 🟡 **YELLOW** | approaching threshold, or lag 1-5s | **Hold** limit |
| 🔴 **RED** | over threshold, or blocked > 0, or lag > 5s | **Decrease** limit × 0.5 |
| ⚫ **CRITICAL** | severe overload | **Pause** (limit → 0) or throttle to min |

Secondary signals (replication lag, connection-pool saturation, query latency p99)
can only **raise** the level, never lower it.

## Workload Attribution (Surgical Enforcement)

The governor surgically targets only the migration cohort, **never touching prod queries**.

```mermaid
flowchart LR
    subgraph Backend["Each DB Backend"]
        B1[usename: backfill_user]
        B2["query: /* dbguard:mig */"]
        B3[application_name: migration-job]
    end

    subgraph Classifier["CohortClassifier"]
        C1{usename match?}
        C2{query_tag match?}
        C3{app_name match?}
        C4[MIGRATION cohort]
        C5[PROD cohort]
    end

    B1 --> C1
    B2 --> C2
    B3 --> C3
    C1 -->|Yes| C4
    C1 -->|No| C2
    C2 -->|Yes| C4
    C2 -->|No| C3
    C3 -->|Yes| C4
    C3 -->|No| C5

    subgraph Action
        A1["Cancel"]
        A2["Never touch"]
    end

    C4 --> A1
    C5 --> A2
```

**Signal precedence:** `usename` (strongest) → `query_tag` → `application_name` (weakest)

## Deployment Modes

```mermaid
flowchart TB
    subgraph Library["Mode A: In-Process Library"]
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker 3]
        GOV[Governor]
        W1 & W2 & W3 -->|"with gov.batch()"| GOV
        GOV -->|blocks| W1 & W2 & W3
    end

    subgraph Sidecar["Mode B: Observe Sidecar"]
        OBS[ObserverAgent]
        JOB[gh-ost / pt-osc / any tool]
        OBS -->|"/throttle: yes/no"| JOB
        JOB -.->|"polls advisory"| OBS
    end

    subgraph Enforcer["Mode C: Out-of-Band Enforcer"]
        ENF[EnforcerAgent]
        ENF -->|"pg_cancel_backend()"| DB[(DB)]
    end
```

## Result: Before vs After

```mermaid
gantt
    title Without Governor
    dateFormat X
    axisFormat %s
    section Migration
    Backfill runs full speed :a, 0, 60
    section Prod Latency
    p99 spikes to 5s :crit, b, 10, 50
    section Outcome
    Checkout failures :crit, c, 20, 40
```

```mermaid
gantt
    title With Governor (AIMD throttling)
    dateFormat X
    axisFormat %s
    section Migration
    Batch 1-5 (GREEN) :a, 0, 10
    Batch 6-8 (YELLOW hold) :b, 10, 20
    Batch 9-10 (RED backoff) :c, 20, 30
    Resume (GREEN) :d, 30, 60
    section Prod Latency
    p99 stays under 200ms :done, e, 0, 60
    section Outcome
    Zero checkout failures :done, f, 0, 60
```

## Summary

A closed feedback loop that senses DB health → decides using AIMD → acts by
blocking workers or cancelling backends. Production queries are **never touched**;
migrations self-throttle or get cancelled.
