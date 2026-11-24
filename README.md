# WhatsApp Agent Gateway

A diagnostics lab takes test bookings on WhatsApp. Webhooks arrive in bursts and sometimes twice. The lab API is slow and fails about one call in five. The gateway acks in one database round trip, then a worker books through tools. Two patients cannot hold the same slot.

The first commit is [`PLAN.md`](PLAN.md). This repository is that design.

| | |
|---|---|
| Ack, 1,000 messages, 25 phones in flight | p50 **28.8 ms**, p95 **57.3 ms**, p99 **74.8 ms**, max **88.2 ms** |
| Duplicates | **200 / 200** suppressed |
| Jobs | **820 done, 0 dead** (800 unique messages plus 20 that stayed under the rate limit) |
| Bookings | **100 confirmed**, **0** double-booked slots, **0** lab/database mismatches |
| Lab | 500–1500 ms, 20% faults before commit, **54** retries, queue idle in **18.9 s** |
| Rate limit | **5** messages stored and not sent to the model |
| LLM spend | **₹0.00** (mock provider) |
| Tests | **11** passing against Postgres and the mock lab |

Full table: [`LOAD_REPORT.md`](LOAD_REPORT.md). Raw numbers: [`load-report.json`](load-report.json).

## Run

```bash
cp .env.example .env
docker compose up --build
```

| URL | What |
|---|---|
| http://localhost:8000/demo | Desk: send a message, send the same id again, race 10 phones for one slot |
| http://localhost:8000/metrics | Prometheus text. Business numbers are SQL at scrape time |
| http://localhost:8000/metrics/summary | Same figures as JSON |
| http://localhost:8000/metrics/conversations | Per-conversation tokens and INR, phone masked |
| http://localhost:8000/healthz | Process up |
| http://localhost:8000/readyz | Database reachable |
| http://localhost:8001/health | Mock lab |

No API key is required. `LLM_PROVIDER=mock` by default.

Try it: open `/demo`, send `I want to book a CBC test on 2099-08-01`, then `1`. **Send duplicate** posts the same `wamid` again: HTTP 200, no second job. **Race 10 phones** asks ten patients for one morning slot.

```bash
# 1,000 signed posts, then wait until the queue and pending bookings are idle
python scripts/loadtest.py
```

## Tests

The tests use the same Postgres and mock lab as compose. Stop the worker first so it does not claim jobs the tests insert.

```bash
docker compose up -d postgres mocklab
docker compose stop worker
pytest
docker compose start worker
```

Covered: HMAC and the verify challenge, a duplicate that does not consume rate-limit quota, the 21st message in a minute, ack under 200 ms, one running job per conversation, lease reclaim, ten conversations racing for one slot (the losers do not call the lab), a 503 before commit then a successful retry, a timeout after the lab has committed (the reconciler confirms, still one lab row), the tool gate that refuses a slot the patient did not pick, a second book after `booked`, a terminal handoff, the global spend cap, and one worker pass that books once.

## How a message moves

```
POST /webhook
  HMAC over the raw body
  accept_inbound() in one round trip
    insert wa_message_id ON CONFLICT DO NOTHING
    rate-limit only the new row
    insert a job
  200

worker, 16 loops
  SKIP LOCKED, oldest job of a conversation that is not already running
  check_slots / book_test / handoff_to_human
  reserve the slot, then call the lab with a stable attempt id
  reconciler GETs that id if the call times out
```

Postgres is the queue and the lab's booking store (schema `lab`). A Redis list would already have returned 200 before a pop, and a pop drops the message if the worker dies. A slot lock with a short TTL expires while a slow call is still in flight. Neither is used here.

Jobs are at-least-once. Effects are idempotent. The lease is 90 seconds with a 15 second heartbeat. `SIGTERM` releases an unfinished claim. A duplicate webhook does not bump the phone's rate limit.

`book_test` runs only when the latest patient text selects a slot that `check_slots` actually returned. The attempt id is a UUID stored on the conversation, not a hash the model picks. The partial unique index on `slot_id` while a booking is `pending` or `confirmed` is what stops two patients. The loser does not call the lab. If the lab commits and the response is lost, the same key replays the original row; if the client gives up, the reconciler reads `GET /bookings/by-key/{attempt_id}` and confirms. It does not free a slot while the lab cannot be reached.

`/metrics` gauges are queries, so the API and the worker cannot disagree, and a restart does not zero the bookings. Per-conversation series are not Prometheus labels. Ack latency is the one process-local histogram, because only the API sees that clock. The numbers above are the load script's client-side timings.

## Spend

The load and the tests used the mock provider. **Actual spend: ₹0.00.** A live OpenAI-compatible model is used only when `LLM_PROVIDER=openrouter` and `OPENROUTER_API_KEY` are set. `llm_budget` is one row: every worker stops calling the model once the rupee cap or the daily call cap would be crossed, and the deterministic policy finishes the turn. The cap defaults to ₹500.

[`COST.md`](COST.md) prices 1 million user messages a month. [`AI_USAGE.md`](AI_USAGE.md) records how this repo was written. [`VIDEO_SCRIPT.md`](VIDEO_SCRIPT.md) is the 4 minute walkthrough.

## Layout

```
PLAN.md                  first commit
migrations/001_init.sql  schema, accept_inbound()
src/gateway/             api, worker, booking, agent, metrics
src/mocklab/             slow lab, durable idempotency, chaos header
web/demo.html
scripts/loadtest.py
tests/
```

Synthetic phones only. `.env` is gitignored. `.env.example` has no secrets.
