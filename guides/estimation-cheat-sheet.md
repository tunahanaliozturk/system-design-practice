# Estimation cheat sheet

Rough, defensible numbers to quote in an interview. State them as estimates ("about", "on the order of").
Being within a factor of two is plenty; being off by a factor of 1,000 is what loses the room.

## Time

| Quantity | Value | Round to |
|---|---|---|
| Seconds in a day | 86,400 | 100K |
| Seconds in a month | ~2.6M | 2.5M |
| Seconds in a year | ~31.5M | 30M |
| Peak to average | 2x to 3x for steady traffic, 10x for campaigns and launches | say which one you assume |

Requests per day to per second: divide by 100K. 1M/day is about 10/s. 1B/day is about 10K/s.

## Latency

| Operation | Rough latency |
|---|---|
| Main memory reference | 100 ns |
| Read 1 MB sequentially from memory | 5 to 10 µs |
| SSD random read | 100 µs |
| Round trip inside one availability zone | 0.5 ms |
| Redis `GET` over the network | 0.5 to 1 ms |
| PostgreSQL primary key lookup | 1 to 2 ms |
| Read 1 MB sequentially from SSD | ~1 ms |
| Elasticsearch filtered query | 10 to 100 ms |
| Round trip across a continent | 30 to 80 ms |
| Round trip across an ocean | 100 to 150 ms |
| Third-party API (payments, SMS) | 200 ms to several seconds, sometimes never |

The lesson in this table: one loop that makes 5,000 sequential network calls costs 2.5 to 5 seconds no
matter how fast each call is. Batch, pipeline, or move the work.

## Throughput of one node

These are ballpark figures for a reasonable modern machine and simple operations.

| Component | Rough capacity |
|---|---|
| PostgreSQL primary | a few thousand simple writes/s, tens of thousands of indexed reads/s |
| Redis node | ~100K simple ops/s, more with pipelining |
| Kafka partition | ~10 MB/s writes; a broker handles hundreds of MB/s |
| Elasticsearch data node | hundreds to a few thousand queries/s, depending on the query |
| Stateless HTTP service instance | 1K to 10K requests/s for light handlers |
| WebSocket gateway node | 100K to 1M idle connections, depending on memory per connection |

## Sizes

| Item | Rough size |
|---|---|
| UUID / BIGINT id | 16 B / 8 B |
| Typical row with a dozen columns | 200 B to 1 KB |
| Short text message | 100 to 300 B with metadata |
| Tweet-sized post with metadata | ~1 KB |
| Thumbnail image | 10 to 50 KB |
| Phone photo | 2 to 5 MB |
| 1 minute of 720p video | 20 to 50 MB |

Useful multipliers: indexes add 20% to 100% on top of row data; replication factor 3 triples raw storage.

## Little's law

```text
in-flight requests = arrival rate x time in system
```

50 requests/s at 200 ms each is 10 in flight, so a pool of 10 connections is the floor. If the
dependency slows to 2 s, the same traffic needs 100. This is the calculation that tells you when a slow
dependency will exhaust your thread pool or connection pool, and it is the one that pages people.

## A worked example

"Design a URL shortener for 100M new links a month and 100:1 reads."

```text
writes: 100M / 2.5M s           = 40/s average, ~400/s peak
reads:  40/s x 100              = 4,000/s average, ~40K/s peak
storage: 100M x 12 months x 500 B = 600 GB/year before replication
cache:  20% of links get 80% of clicks; 1 month of hot links = 20M x 500 B = 10 GB, fits in one Redis
```

Then the sentence that matters: "Writes are trivial; the redirect path is a cache problem."
