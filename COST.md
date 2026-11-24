# Cost at 1 million messages a month

Prices below are Google's published Gemini 2.5 Flash-Lite rates on 21 Nov 2026: **$0.10 / 1M input tokens**, **$0.40 / 1M output tokens**, cached input **$0.01 / 1M**. Source: [Gemini API pricing](https://ai.google.dev/gemini-api/docs/pricing). INR uses a planning rate of **₹84 per USD**. Rescale the rupee column if the invoice rate differs. The assignment run itself does not call this model; see the README for actual spend.

## What reaches the model

1,000,000 is the count of user messages, not webhook deliveries.

- Duplicate deliveries are about 15% on top (150,000). They stop at the unique `wa_message_id` and never call the model.
- About 3% of user messages are non-text (images, stickers) and are stored as `ignored`.
- About 2% are over the per-phone limit and are stored as `rate_limited`.

Model turns ≈ 1,000,000 × 0.95 = **950,000**.

Each turn sends the system prompt, the conversation memory, and a few recent turns, not the full history.

| Piece | Tokens |
|---|---|
| System prompt | 180 |
| Memory (test, date, offered slots, attempt) | 150 |
| Last few turns | 220 |
| New user message | 50 |
| **Input** | **600** |
| **Output** (short WhatsApp reply, no hidden reasoning) | **80** |

## LLM

| | Input | Output | USD | INR |
|---|---:|---:|---:|---:|
| No cache | 570M × $0.10 = $57.00 | 76M × $0.40 = $30.40 | $87.40 | ₹7,340 |
| Cached system + memory | 313.5M × $0.01 + 256.5M × $0.10 = $28.79 | $30.40 | **$59.19** | **₹4,970** |

570M = 950,000 × 600. 76M = 950,000 × 80. Cached input is 950,000 × 330 tokens at $0.01 and 950,000 × 270 tokens at $0.10.

A larger model (Haiku-class, on the order of $1 / $5 per million tokens) would put the same uncached volume near **$950** (about ₹80,000). The booking gates do not need that. Hidden reasoning tokens would multiply the output line; the prompt tells the model to stay short, and the mock path used for load tests does not spend at all.

The `llm_budget` row is a global cap (`MAX_SPEND_INR`, default 500). Every worker reads the same row. Past the cap, turns use the deterministic policy.

## Infra

Average rate is 1e6 / 2.6e6 seconds ≈ **0.4 messages/s**. A 10× burst for a few minutes is about **4 messages/s**. A fifth of those calls touch the lab for about a second (check slots or book, including a retry), so the burst needs on the order of **one concurrent lab call**, not a fleet. Sixteen worker loops drain a 1,000-message burst with a 1 second lab in well under a minute of lab time (see `LOAD_REPORT.md` for the measured drain).

| Piece | Monthly |
|---|---|
| One 2 vCPU app VM (api + worker + mock is not in prod; the lab is the client's) | ~$20 |
| Small Postgres (burstable, same region). No Redis. | ~$25 |
| Disk, egress, snapshots | ~$10 |
| **Infra** | **~$55 · about ₹4,600** |

## Total

| | USD / month | INR / month |
|---|---:|---:|
| Infra | $55 | ₹4,600 |
| LLM, Flash-Lite, cached prompt | $59 | ₹4,970 |
| **Together** | **$114** | **₹9,600** |

The model is the line that moves. Cutting turns (the dedupe, the non-text skip, the rate limit, and the short memory) matters more than another database.
