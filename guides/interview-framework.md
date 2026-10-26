# How to run a 45-minute system design interview

The interviewer is not checking whether you know the "right" architecture. They are checking whether you
can turn a vague problem into numbers, pick a shape that fits those numbers, and say what breaks. Every
design in this repo is written in the order below, so you can practise the same order out loud.

## The six steps

| Step | Time | What you produce |
|---|---|---|
| 1. Clarify requirements | 5 min | Functional list, non-functional targets, out of scope, scale numbers |
| 2. Capacity estimate | 5 min | Peak QPS, storage per year, the one number that shapes the design |
| 3. API and data model | 10 min | Endpoints, tables with keys, cache keys, topics |
| 4. High-level architecture | 10 min | The diagram, walked through one request at a time |
| 5. Deep dive | 10 min | The hardest part, usually the one the interviewer picks |
| 6. Failures and trade-offs | 5 min | What breaks, what the user sees, how it recovers |

If you run out of time, it should be in step 5, not step 2. A deep dive on a design with no numbers is a
guess.

## 1. Clarify requirements

Ask, then write the answers where the interviewer can see them.

- **Who uses it and doing what?** Turn it into four to six verbs: search, book, cancel, pay.
- **What must never happen?** Double booking, a lost message, charging twice. This is your correctness
  invariant, and the whole design bends around it.
- **What can be stale, and for how long?** "Search may be 5 seconds behind, checkout may not."
- **How big?** Daily active users, actions per user, read/write ratio, size of one item.
- **What is out of scope?** Say it now: "I will not design the admin UI or fraud scoring unless you want
  me to."

If the interviewer will not give numbers, propose some: "Let me assume 10M daily users and 20 reads each;
stop me if that is off by an order of magnitude."

## 2. Capacity estimate

One line of arithmetic per number. Round aggressively: a day is 100K seconds, not 86,400.

```text
writes: 500K bookings/day / 100K s  = 5/s average, x10 peak = 50/s
reads:  5M searches/day / 100K s    = 50/s average, x10 peak = 500/s
storage: 500K/day x 1 KB            = 0.5 GB/day, ~180 GB/year
```

Then say what the numbers mean. "50 writes per second is one PostgreSQL primary. The hard part is not
throughput, it is the last room under contention." That sentence is worth more than the arithmetic.

See [estimation-cheat-sheet.md](estimation-cheat-sheet.md) for the reference numbers.

## 3. API and data model

- Write the two or three endpoints that matter, with the request and response bodies.
- Mark which are idempotent and how (`Idempotency-Key` header, natural key, conditional update).
- Write real tables: primary key, the index each hot query needs, the constraint that protects your
  invariant (`CHECK (booked + held <= total)`).
- List cache keys with their TTL, and say what happens if the cache is lost.
- List topics with their partition key, and say why that key keeps the right events in order.

## 4. High-level architecture

Draw boxes for clients, the edge, the services, the stores, the async workers. Then walk one request
through it, numbering the arrows. A diagram nobody walks through is decoration.

Place each piece of data deliberately and say why: "Inventory lives in PostgreSQL because a promise to a
customer must be in a transaction. Availability for search is copied to Redis because search runs 100
times more often than booking."

## 5. Deep dive

Pick, or let the interviewer pick, the part where the design could be wrong:

- contention on one row or one key,
- a call to something you do not control (payments, suppliers, push providers),
- fan-out (one write that becomes millions of reads or writes),
- a cache or index that must stay close to the source of truth.

For each: state the problem, give two or three options, pick one, and say what it costs.

## 6. Failures and trade-offs

For every call that leaves the process, answer three questions: what if it is slow, what if it answers
twice, what if it never answers. Put the answers in a table: failure, effect, recovery.

End with the trade-offs you made and what you would revisit at 10x scale. "I chose fan-out on write for
normal users; at 10x I would move more authors to fan-out on read and measure the merge cost."

## Habits that help

- Say numbers out loud and write them down. Numbers survive review; adjectives do not.
- Name the source of truth for every piece of state, and the one component allowed to write it.
- Prefer boring technology and explain the one place you chose something unusual.
- When you change your mind, say so and say why. That is a signal, not a mistake.
- Do not draw a box you cannot explain. Every box costs you a minute of questions.
