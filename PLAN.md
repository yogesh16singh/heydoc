# Plan: WhatsApp Agent Gateway

A diagnostics lab receives test-booking requests on WhatsApp. Webhooks arrive in bursts and sometimes twice. The lab API is slow and fails about one call in five. Two patients must never receive the same slot.

This document is the design. Code follows it.

## Users

- **Patient.** Sends a WhatsApp message and expects one reply: which slots exist, a booking reference, or a clear handoff. A retry from Meta must not create a second appointment.
- **Lab desk.** Needs a durable record of who holds each slot, including when the lab API timed out after it may already have committed.
- **On-call human.** Receives a handoff with the reason, the structured memory (test, date, offered slots), and the last few turns. They should not inherit a slot that the lab might already have sold.

## Architecture

One container image, two processes: `api` and `worker`. Postgres 16 is the only data store. The mock lab is a separate process and writes its own tables in schema `lab` in that same database, so a lab restart does not forget a committed booking.

```
WhatsApp / load script / demo
        │  POST /webhook   raw-body HMAC
        ▼
  api
   └─ one SQL function: insert message, rate-limit, enqueue job
   └─ 200                          no LLM, no lab call on this path

  Postgres
   ├─ inbound_messages     UNIQUE(wa_message_id)
   ├─ jobs                 SKIP LOCKED + lease + one running job per conversation
   ├─ bookings             partial UNIQUE(slot_id) WHERE status IN (pending, confirmed)
   ├─ conversations, outbox, handoffs, llm_usage, llm_budget, lab_calls
   └─ schema lab           slots, bookings (idempotency key PK, slot UNIQUE)

  worker (16 loops)
   ├─ claim oldest pending job whose conversation is idle
   ├─ agent, at most 4 tool rounds
   │    check_slots · book_test · handoff_to_human
   ├─ reserve the slot, then call the lab with a stable attempt id
   ├─ reconciler reads the lab by that id and confirms or releases
   └─ outbox for the WhatsApp reply (mock sender by default)
```

**Ack path.** Verify `X-Hub-Signature-256` over the raw body in constant time, then call `accept_inbound(...)`. That function inserts the message `ON CONFLICT (wa_message_id) DO NOTHING`, and only if the insert happened does it bump the per-phone minute counter, upsert the conversation, and insert a job. A duplicate returns 200 and does not consume rate-limit quota. Over 20 messages a minute the row is stored as `rate_limited` and no job is created, and the response is still 200 so Meta stops retrying. Non-text messages and empty status callbacks are stored or ignored and are not sent to the model. Postgres down returns 503 so Meta retries. Bad signature returns 401. Malformed JSON returns 400.

The handler does not take a second database round trip, and it does not call the lab or the model. Client-side latency from the load script is the published number: p50, p95, p99, and max. The pass line is p99 under 200 ms. Max is reported on its own.

**Why the queue is Postgres.** The process has already returned 200 by the time a worker runs. The job row is committed in the same transaction as the message, so a crash cannot ack a message and then lose it. `SELECT … FOR UPDATE SKIP LOCKED` lets several loops consume the queue. A partial unique index allows one `running` job per conversation, and the claim query only takes the oldest pending job of an idle conversation, so one phone stays in order while other phones run in parallel.

A Redis list would ack first and then pop. A pop is destructive: a worker crash after the pop drops the message, and a dedupe key with a TTL disappears on restart. A slot mutex with a short TTL expires while a 500–1500 ms lab call plus backoff is still in flight, so a second worker would call the lab. Those locks are not part of this design. The partial unique index does not expire.

**Delivery.** Jobs are at-least-once. Effects are idempotent. A lease of 90 seconds is heartbeat every 15 seconds. On expiry the job returns to `pending` with a new claim token; a stale worker whose token no longer matches does not mark the job done. `SIGTERM` stops new claims and releases an unfinished lease. `LISTEN/NOTIFY` wakes a loop, and a 250 ms poll covers a missed notify.

**Why the lab shares Postgres.** The mock lab is still a separate service with its own HTTP API, latency, and faults. Its bookings live in `lab.bookings`, with the idempotency key as the primary key and `slot_id` unique. Replaying a key returns the original row. Restarting the lab process does not drop that row. An in-memory lab would forget a commit and the next retry could insert a second appointment.

## Booking invariant

The model may ask for a booking. It cannot create one.

1. `check_slots` stores the returned slot ids on the conversation. The model cannot add to that list.
2. `book_test` runs only in state `offering`, and only when the latest user text selects that slot (a number, the label, or the id). Otherwise the tool returns an error and does not call the lab.
3. The first legal confirmation creates an `attempt_id` (UUID) and stores it on the conversation. Retries of the same confirmation reuse it. A new confirmation after `failed` or `cancelled` creates a new id. The key is not `hash(phone, test, slot)`: that would make a later rebook look like a replay. Two patients are stopped by the slot unique index, not by the hash.
4. Insert the booking as `pending`. The partial unique index is `slot_id WHERE status IN ('pending', 'confirmed')`. The loser gets `SLOT_TAKEN` and does not call the lab.
5. `POST /bookings` with `Idempotency-Key: attempt_id`.
6. A definite failure before commit (HTTP 503, no row) is retried with full jitter: 5 attempts, base 200 ms, cap 5 s. HTTP 409 is not retried.
7. A timeout after the lab may have committed leaves the row `pending`. The reservation is not deleted and the attempt id is not replaced.
8. The reconciler, every couple of seconds, calls `GET /bookings/by-key/{attempt_id}` for pending rows that are idle and older than the client timeout. `200` marks the booking `confirmed` and notifies the patient. `404` means the lab did not commit, so the reservation is released and the patient can pick again. A connection error leaves the row `pending`. A human is involved when the lab stays unreachable past the deadline, or when the agent calls `handoff_to_human`.

After the queue is idle, every `confirmed` booking has exactly one `lab.bookings` row with the same key and slot, and no active slot appears twice. The load report asserts that.

Default lab behaviour for the 1,000-message run: 500–1500 ms latency and a seeded 20% fault rate before any write. Tests do not use that dice roll. They send `X-Lab-Chaos: fail-before | timeout-after-commit | conflict | none`.

## Agent

Conversation states: `collecting`, `offering`, `booked`, `handed_off`.

| Tool | When it may run |
|---|---|
| `check_slots(test_code, date)` | `collecting` or `offering`. The wrapper stores the slots and moves to `offering`. |
| `book_test(test_code, slot_id, patient_name)` | `offering`, and only through the gate above. `patient_name` is a synthetic display name. |
| `handoff_to_human(reason)` | Any non-terminal state. Writes a summary and stops the loop. `handed_off` and `booked` reject another booking. |

At most 4 tool rounds. The prompt is the system text, the memory object, and the last few turns, not the full transcript.

`LLM_PROVIDER=mock` is the default. It applies the same gates and is what tests and the load run use, so that spend is ₹0. A live OpenAI-compatible provider is used only when a key is present. If the call errors, is rate-limited, or returns a tool call the gate rejects, the deterministic policy finishes the turn and `llm_usage.outcome` is `fallback`.

The spend cap is a single `llm_budget` row: `UPDATE … WHERE spent_inr + delta <= cap AND calls + 1 <= max_calls`. If the update touches nothing, every worker uses the deterministic policy. The cap is global.

Catalog: CBC, LFT, THYROID, HBA1C, VITD. Four slots a day, one holder each.

## Data model

- `conversations`: `phone` unique, `state`, `memory` jsonb (test, date, offered slots, attempt id, patient name, booking id).
- `inbound_messages`: `wa_message_id` unique, phone, body, type, raw jsonb, status (`queued`, `rate_limited`, `ignored`).
- `rate_limits`: primary key `(phone, window_start)`, count. Duplicates do not increment it.
- `jobs`: one per inbound message, status `pending | running | done | dead`, `attempts`, `next_run_at`, `locked_until`, `claim_token`. Partial unique index on `conversation_id` where `status = 'running'`.
- `bookings`: `attempt_id` unique, `lab_booking_id` unique, partial unique `slot_id` while `pending` or `confirmed`.
- `outbox`: one reply per inbound message (`message_id` unique). Reconciler notices use a null message id.
- `handoffs`, `llm_usage`, `llm_budget`, `lab_calls`, `event_counters`.
- `lab.slots`, `lab.bookings`, `lab.call_log`.

`accept_inbound` is a PL/pgSQL function. A chain of data-modifying CTEs would share one snapshot and could not see each other's inserts. One function call is still one round trip, and the statements inside it run in order.

## API

| Method | Path | Behaviour |
|---|---|---|
| `GET` | `/webhook` | Meta handshake. Echo `hub.challenge` when the verify token matches. |
| `POST` | `/webhook` | Signature required. 200 accepted, duplicate, rate-limited, or ignored. 401 bad signature. 400 malformed. 503 database unavailable. |
| `GET` | `/healthz` | Process is up. |
| `GET` | `/readyz` | `SELECT 1` succeeds. |
| `GET` | `/metrics` | Prometheus text. Business gauges are queries at scrape time, so the API and the worker agree. Ack latency is a process-local histogram because only the API sees that clock. |
| `GET` | `/metrics/summary` | The same figures as JSON, for the demo and the load script. |
| `GET` | `/metrics/conversations` | Paginated JSON: masked phone, state, tokens, INR, booking id. Those values are not Prometheus labels. |
| `GET` | `/demo` | Operator page when `DEV_MODE=true`. |
| `POST` | `/dev/simulate`, `/dev/race`, `/dev/reset` | Dev only. Unmounted when `DEV_MODE=false`. |

Per-conversation series are not exported to Prometheus. At a million conversations that label set is an outage.

## Failure matrix

| Failure | Behaviour |
|---|---|
| Duplicate `wamid` | No second job, no rate-limit bump, HTTP 200. |
| Burst | One indexed write. Load script publishes p99 and max. |
| 21st message in a minute | Stored `rate_limited`, no model call, HTTP 200. |
| Lab 503 before commit | Retry with backoff, same attempt id. |
| Lab committed, response lost | Same key replays the original row. If the client gives up, the reconciler confirms from `GET`. |
| Two patients, one slot | One `pending` insert. The other gets `SLOT_TAKEN` and does not call the lab. |
| Worker crash | Lease expires, job is claimed again, effects idempotent. |
| `SIGTERM` | Unfinished lease released. |
| Attempts exhausted | Job `dead`, conversation `handed_off`, apology on the outbox. A `pending` booking is not released by the dead-letter path. |
| Model error or spend cap | Deterministic policy. Booking rules unchanged. |
| Lab unreachable to the reconciler | Stay `pending`. Do not free a slot that might exist. |

## 1,000-message run

`scripts/loadtest.py` sends 1,000 signed posts: 800 unique ids across 100 phones (8 messages each, in order, 25 phones in flight) and 200 replays of earlier ids. Each phone books CBC on its own date so the run measures throughput, not an artificial pile-up on one slot. The 10-way race is a separate test. The lab stays at 500–1500 ms and 20% pre-commit faults. The script waits until `pending` and `running` jobs are zero, then checks double-booked slots = 0 and lab-vs-database mismatches = 0. A short extra burst from one new phone shows the rate limiter is not zero. Results go to `LOAD_REPORT.md` and `load-report.json`.

## Cost sketch (1 million user messages / month)

Average rate is about 0.4 messages/s. A 10× burst is about 4 messages/s. Infra is one small app VM and a small Postgres, on the order of a few thousand rupees a month. There is no Redis.

The model is the real cost. Duplicates, non-text, and rate-limited messages never call it. A turn sends memory plus a few recent messages, not the whole history. `COST.md` prices that volume at a current small-model rate, with a second column for prompt caching, and states the worker count that drains a burst when a lab attempt costs about a second. The assignment itself stays on the mock provider (₹0) plus at most one optional live transcript, under a ₹500 cap enforced in `llm_budget`.

## Build order

1. This file, alone, is the first commit.
2. Schema, compose, health checks.
3. Webhook, signature, dedupe, rate limit.
4. Mock lab with durable idempotency and chaos headers.
5. Worker, lease, booking, reconciler.
6. Agent gates, budget, metrics, demo.
7. Tests and the 1,000-message report.
8. `README.md`, `COST.md`, `AI_USAGE.md`, `VIDEO_SCRIPT.md`.
