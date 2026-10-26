# System name

> **The answer in three sentences.** Say the core mechanism first: what decides correctness, what makes it
> fast, and what happens when the slow or unreliable part misbehaves. An interviewer who stops you after
> this paragraph should already know your design.

| Category | Difficulty | Interview length | Core problems |
|---|---|---|---|
| e.g. Booking and commerce | Medium | 45 min | contention, idempotency, payments |

**Contents:** [Requirements](#1-requirements) · [Capacity](#2-capacity-estimate) · [API](#3-api) ·
[Data model](#4-data-model) · [Architecture](#5-high-level-architecture) · [Flows](#6-request-flows) ·
[Deep dives](#7-deep-dives) · [Failures](#8-failure-cases) · [Trade-offs](#9-trade-offs) ·
[Follow-ups](#10-likely-follow-ups)

## 1. Requirements

**Functional**

- What a user can do, as verbs. Four to six bullets.

**Non-functional**

- Latency targets with a percentile (p99 under 200 ms), availability, consistency per operation, durability.

**Out of scope**

- What you are deliberately not designing, so the interviewer can object now rather than at minute 40.

**Scale**

- The numbers you will build the estimate on: users, daily actions, read/write ratio, data per item.

## 2. Capacity estimate

Show the arithmetic, one line each. State the result as an estimate and say what it means for the design.

| Quantity | Arithmetic | Result | So what |
|---|---|---|---|
| Write QPS | 100M/day / 100K s | ~1,000/s avg, ~5,000/s peak | One PostgreSQL primary is enough |

## 3. API

```http
POST /things          Idempotency-Key: <uuid>
{ ... }
-> 201 { ... }
```

Call out which endpoints are idempotent and how.

## 4. Data model

The source of truth first, then the caches and logs that are derived from it.

```sql
CREATE TABLE things (...);
```

**Redis keys**

| Key | Value | TTL | Purpose |
|---|---|---|---|

Every key needs an answer to "what if Redis loses it?". Write that answer under the table.

**Kafka topics** (or other queues)

| Topic | Key | Producer | Consumers |
|---|---|---|---|

## 5. High-level architecture

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="diagrams/architecture-dark.png">
  <img alt="..." src="diagrams/architecture-light.png">
</picture>

A short walkthrough of the diagram: one sentence per box that is not obvious, and why each store holds
what it holds.

## 6. Request flows

Numbered steps for the two or three flows that matter, each with a sequence diagram where the order of
calls is the interesting part.

## 7. Deep dives

Two to four sections on the parts an interviewer will push on. Each one: the problem, the options, the
choice, the cost of the choice. Code or SQL where it makes the point faster than prose.

## 8. Failure cases

| Failure | Effect | Recovery |
|---|---|---|

## 9. Trade-offs

| Decision | Chosen | Rejected | Why |
|---|---|---|---|

## 10. Likely follow-ups

- **Question?** Short answer, the way you would say it out loud.

## Phrases worth using

- One-liners that sum up a decision. Three to five of them.
