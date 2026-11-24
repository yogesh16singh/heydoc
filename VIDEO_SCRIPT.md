# Video script (about 4 minutes)

Record the screen with `docker compose up` already healthy and the demo at http://localhost:8000/demo.

1. **Open (20s).** Say what the lab needs: WhatsApp bookings, webhooks that arrive twice, a slow lab that fails a fifth of the time, and no double booking. Point at `PLAN.md` as the first commit.

2. **One booking (60s).** In the demo, phone `919810000099`, send `I want to book a CBC test on 2099-08-01`. Wait for the numbered slots. Send `1`. Show the reference in the transcript, the booking row, and the matching row under lab bookings.

3. **Duplicate (40s).** Press **Send duplicate**. The webhook still returns success. Point at the dupes counter going up by one, and at the lab bookings staying at one row.

4. **Race (50s).** Press **Race 10 phones**. Say that ten patients asked for the same CBC morning slot. When it finishes, confirmed is 1 and the lab has one row for that slot. The double-booked counter stays 0.

5. **Timeout after commit (40s).** From a shell, with the stack up:

   ```bash
   docker compose exec api python -c "
   import asyncio, uuid
   from gateway.db import create_pool
   from gateway.booking import reserve_and_book
   from gateway.reconcile import reconcile_once

   async def main():
       pool = await create_pool()
       attempt = uuid.uuid4()
       async with pool.acquire() as conn:
           cid = await conn.fetchval(\"INSERT INTO conversations (phone) VALUES ('919855500001') RETURNING id\")
           result = await reserve_and_book(conn, conversation_id=cid, phone='919855500001', test_code='CBC', slot_id='CBC:2099-09-01:1100', attempt_id=attempt, patient_name='Synthetic Patient', memory={}, chaos='timeout-after-commit', max_attempts=1, timeout_s=0.5)
           print('client', result['status'])
           print('lab', await conn.fetchval(\"SELECT count(*) FROM lab.bookings WHERE slot_id='CBC:2099-09-01:1100'\"))
       print('reconciled', await reconcile_once(pool, min_age_s=0))
       async with pool.acquire() as conn:
           print('status', await conn.fetchval('SELECT status FROM bookings WHERE attempt_id=$1', attempt))
           print('lab', await conn.fetchval(\"SELECT count(*) FROM lab.bookings WHERE slot_id='CBC:2099-09-01:1100'\"))
       await pool.close()
   asyncio.run(main())
   "
   ```

   Say: the client timed out, the lab already had the row, the reconciler confirmed it, and there is still one lab row.

6. **Metrics and close (30s).** Open http://localhost:8000/metrics and point at `gateway_bookings`, `gateway_double_booked_slots`, and `gateway_llm_cost_inr`. Mention the load report: 1,000 messages, p99 under 200 ms, duplicates removed, dead jobs 0. Actual LLM spend is ₹0 on the mock provider.
