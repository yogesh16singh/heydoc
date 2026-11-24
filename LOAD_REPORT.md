# Load report

1,000 signed posts: 800 unique messages (100 phones, 8 each, in order) and 200 replays of earlier ids. 25 phones in flight. Each phone books CBC on its own date. The lab is slow (500–1500 ms) and fails about 20% of calls before commit. A 25-message burst from one new phone follows, to show the rate limit.

The script waits until pending and running jobs are zero and no booking is still `pending`.

| Metric | Result |
|---|---|
| HTTP 200 (first 1,000) | 1000 |
| Ack p50 / p95 / p99 / max | 28.76 / 57.31 / 74.78 / 88.22 ms |
| Send phase | 1.43 s |
| Queue idle | 18.93 s |
| Accepted | 820 |
| Duplicates suppressed | 200 |
| Rate limited | 5 |
| Jobs done / dead | 820 / 0 (done includes the 20 burst messages under the cap) |
| Confirmed bookings | 100 |
| Double-booked slots | 0 |
| Lab vs database mismatches | 0 |
| Lab retries recorded | 54 |
| Handoffs | 0 |
| LLM spend | ₹0.00 |
| Lab RNG seed | 7 |

Pass line: p99 under 200 ms, duplicates 200, dead jobs 0, double-booked slots 0, mismatches 0, rate limited greater than 0.
