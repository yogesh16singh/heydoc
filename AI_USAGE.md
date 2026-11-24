# AI usage

The gateway was written with Cursor as the editor and Grok as the coding agent. The agent drafted `PLAN.md` first, then the schema, webhook, mock lab, worker, and tests. I reviewed the failure rules in that plan before the code: the queue commits in the same database transaction as the webhook ack, the slot is reserved before the lab is called, and a timeout does not delete that reservation.

## Where the model wrote code

- Schema and `accept_inbound`, including the choice of a function over a CTE chain so each statement can see the previous insert.
- Worker claim query, lease, and reconciler.
- Tool gates: the model may ask `book_test`, and the wrapper refuses unless the patient's latest text selects an offered slot.
- Tests for the duplicate race, the 10-way slot race, fail-before, and timeout-after-commit.
- The load script, the demo page, and these write-ups.

## Token and cost choices

- `LLM_PROVIDER=mock` is the default. The 1,000-message run and the pytest suite use it. Recorded cost for those turns is ₹0. Token counts are still stored (`len/4`) so `/metrics` shows volume separately from spend.
- A live call is optional (`LLM_PROVIDER=openrouter` plus `OPENROUTER_API_KEY`). One global `llm_budget` row refuses the next call once `spent_inr` or `calls` would pass the cap, in every worker, not once per process.
- The prompt is the system text, the memory object, and the latest user message. The transcript is not replayed in full.
- Idempotency keys are UUIDs created on the server when the patient confirms. The model does not choose them.
- Actual spend for this submission is in the README. No patient data and no API key is in the repo. `.env` is gitignored. `.env.example` has empty key fields.
