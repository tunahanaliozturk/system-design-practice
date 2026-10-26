# Building blocks

The patterns every design in this repo reuses, each with what it buys and what it costs. When a chapter
says "outbox" or "conditional update", this is what it means.

## The default toolbox

Most designs here use the same four stores, each in one role. Deviate when another tool clearly wins (a
wide-column store for chat messages, an OLAP store for click analytics) and say why.

| Store | Role | Rule of thumb |
|---|---|---|
| PostgreSQL | Source of truth: anything involving money or a promise to a customer | Lives in a transaction, protected by constraints |
| Redis | Speed and short-lived state: caches, holds, counters, dedup markers, rate limits | Every key needs an answer to "what if Redis loses it?" |
| Elasticsearch | Search: geo, filters, full text, ranking | An eventually consistent copy, never the system of record |
| Kafka | Communication between services: events, retries, CDC | Partition by entity id so one entity's events stay ordered |

## Correctness patterns

### Conditional update

Put the check and the write in one statement, so the database serialises racing writers.

```sql
UPDATE room_inventory SET held = held + 1
WHERE hotel_id = $1 AND room_type_id = $2 AND night = $3 AND booked + held < total;
-- 0 rows updated = sold out
```

The same shape guards state machines: `UPDATE ... SET status = 'CANCELLED' WHERE id = $1 AND status =
'CONFIRMED'`. Zero rows means someone else already moved it.

**Costs:** hot rows serialise. At thousands of writers per second on one row, queue in front or split the
row into buckets.

### Idempotency key

Anything retried will be retried: client retry buttons, load balancers, queue redelivery, deploys in the
middle of a request.

- The key comes from the caller (`Idempotency-Key` header) or from the payload (a hash of the order and its
  lines). Never from a timestamp or a fresh UUID generated on retry.
- Store it with the result under a unique constraint. The second attempt returns the first result.
- Let the database enforce it: `INSERT ... ON CONFLICT DO NOTHING`. "Check then insert" in application
  code loses the race under exactly the load that caused the retries.
- Pass your own id to every external system as their idempotency key: payments, suppliers, push providers.

### Transactional outbox

Write the state change and the event in the same transaction: the event goes into an `outbox` table, and a
relay (Debezium CDC or a polling loop) publishes it to Kafka.

**Buys:** the state and the event never diverge. **Costs:** a relay to run and watch, at-least-once
delivery downstream (consumers dedup on `event_id`), ordering only per partition key. Alert on the age of
the oldest unpublished row, not on the row count.

### Saga

A multi-step workflow across owners without a distributed transaction. Each step has a compensation
(release inventory, refund payment).

- **Orchestrated:** one coordinator stores the current step and drives it. Easy to see where something is
  stuck. Use it for refunds and anything support has to debug.
- **Choreographed:** each service reacts to the previous one's event. Fewer moving parts, harder to
  follow. Use it for short, low-stakes chains.

### Reconciliation

When an external call times out, you do not know whether it happened. Record `UNKNOWN`, then ask the other
side ("do you have a booking with my reference X?") with backoff. Never blindly repeat a non-idempotent
call. A reconciler is also the safety net for lost webhooks.

## Performance patterns

| Pattern | How | Watch out for |
|---|---|---|
| Cache-aside | Read Redis; on miss read the DB and write back with a TTL | Stampede when a hot key expires: single-flight with `SET lock NX PX` |
| Write via CDC | DB change -> Kafka -> consumer rewrites the cache key | Lag; alert on replication slot lag |
| TTL as state | Key expiry means "this hold or quote is over" | Expiry events are not guaranteed; keep a DB sweeper |
| Atomic counter | Lua script or `DECR`; the DB keeps the durable record | Counter loss: rebuild from the durable record before reopening |
| Dedup marker | `SET key NX EX` before doing the work; DB unique key as the backstop | Marker set but work failed: let the DB key decide |
| Read replica | Route reads that tolerate lag to replicas | Read-your-own-writes: send the writer's next read to the primary |
| Materialised read model | Precompute the shape the reader needs (timelines, availability) | You now own a rebuild path; test it before you need it |
| Fan-out on write vs read | Push work to write time or pull at read time | Pick per entity: celebrities pull, everyone else is pushed |

## Protection patterns

| Pattern | Purpose | Notes |
|---|---|---|
| Timeouts that shrink inwards | The outer layer must not give up while inner work continues | Gateway 10 s, service 5 s, DB 2 s |
| Retries with jittered backoff | Ride out blips without a retry storm | Budget by total time; never retry non-idempotent calls |
| Circuit breaker | One dead dependency stops eating every thread | Useless without a fallback answer |
| Rate limiting | Protect yourself and your dependencies | Token bucket in Redis with a Lua script, shared by all replicas |
| Bulkhead | Separate pools per dependency | A slow supplier cannot starve the payment path |
| Dead letter queue | Park messages that keep failing | Alert on it and name who reads it, or it is a folder of forgotten bugs |

## When a component fails

| Failure | Typical effect | Typical recovery |
|---|---|---|
| Redis node lost | Slower reads, short-lived state gone | Rebuild keys from the source of truth, DB sweepers take over |
| Kafka down | Events pile up in the outbox | Nothing lost; the relay drains it when Kafka returns |
| Elasticsearch down | Search degraded | Fallback to a narrow DB query with fewer filters |
| PostgreSQL primary down | Writes fail for seconds | Automatic failover to a synchronous replica |
| Third-party API down | Calls time out | Breaker opens, degrade the feature, reconcile later |

## Phrases worth using

- "PostgreSQL decides, Redis accelerates."
- "Every Redis key can be rebuilt from the source of truth."
- "Every external call is idempotent on our own id."
- "A timeout is not a failure, it is an unknown."
- "Exactly-once delivery does not exist; exactly-once effect is something you build."
