import asyncio
import json
import time
import uuid

import httpx
import pytest

from gateway.agent import deterministic, run_agent
from gateway.booking import reserve_and_book
from gateway.metrics import render_prometheus, snapshot
from gateway.payload import cloud_payload
from gateway.queue import claim_job, release_claim
from gateway.reconcile import reconcile_once
from gateway.settings import settings
from gateway.signature import sign
from gateway.tools import execute_tool
from gateway.webhook import accept_messages
from gateway.worker import process_job


def _post(http, phone, text, wamid):
    raw = json.dumps(cloud_payload(phone, text, wamid)).encode()
    started = time.perf_counter()
    response = http.post(
        "/webhook",
        content=raw,
        headers={"x-hub-signature-256": sign(raw, "test-secret")},
    )
    return response, time.perf_counter() - started


def test_ack_dedupe_and_rate_limit(http):
    durations = []
    for index in range(30):
        response, elapsed = _post(http, f"91982000{index:04d}", "Hi", f"wamid.ack.{index}")
        assert response.status_code == 200
        durations.append(elapsed)
    assert max(durations) < 0.2

    phone = "919821000001"
    first, _ = _post(http, phone, "Hi", "wamid.same")
    second, _ = _post(http, phone, "Hi", "wamid.same")
    assert first.status_code == second.status_code == 200
    summary = http.get("/metrics/summary").json()
    assert summary["duplicates"] >= 1

    limited = "919821000002"
    for index in range(21):
        response, _ = _post(http, limited, f"hello {index}", f"wamid.rl.{index}")
        assert response.status_code == 200
    summary = http.get("/metrics/summary").json()
    assert summary["rate_limited"] >= 1
    assert summary["jobs"]["pending"] >= 20

    image = {
        "object": "whatsapp_business_account",
        "entry": [{"changes": [{"value": {"messages": [{"from": "919821000003", "id": "wamid.img", "type": "image", "image": {"id": "1"}}]}}]}],
    }
    raw = json.dumps(image).encode()
    response = http.post("/webhook", content=raw, headers={"x-hub-signature-256": sign(raw, "test-secret")})
    assert response.status_code == 200
    assert http.get("/metrics/summary").json()["ignored"] >= 1


@pytest.mark.asyncio
async def test_duplicate_does_not_consume_quota(pool):
    payload = cloud_payload("919822000001", "Hi", "wamid.dup.1")

    async def once():
        async with pool.acquire() as conn:
            return await accept_messages(conn, payload)

    first, second = await asyncio.gather(once(), once())
    flags = [first[0]["queued"], second[0]["queued"]]
    assert flags.count(True) == 1
    assert flags.count(False) == 1
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT count FROM rate_limits WHERE phone = $1", "919822000001")
        jobs = await conn.fetchval("SELECT count(*) FROM jobs")
    assert count == 1
    assert jobs == 1


@pytest.mark.asyncio
async def test_fifo_and_lease(pool):
    async with pool.acquire() as conn:
        conv = await conn.fetchrow("INSERT INTO conversations (phone) VALUES ('919823000001') RETURNING id")
        other = await conn.fetchrow("INSERT INTO conversations (phone) VALUES ('919823000002') RETURNING id")

        async def enqueue(conversation_id, wamid):
            message_id = await conn.fetchval(
                """
                INSERT INTO inbound_messages (wa_message_id, phone, body, msg_type, raw, status)
                VALUES ($1, '919823000001', 'hello', 'text', '{}'::jsonb, 'queued')
                RETURNING id
                """,
                wamid,
            )
            return await conn.fetchval(
                """
                INSERT INTO jobs (message_id, conversation_id, status)
                VALUES ($1, $2, 'pending') RETURNING id
                """,
                message_id,
                conversation_id,
            )

        first = await enqueue(conv["id"], "wamid.fifo.1")
        second = await enqueue(conv["id"], "wamid.fifo.2")
        await conn.execute(
            """
            UPDATE jobs
            SET status = 'running',
                locked_until = now() + interval '60 seconds',
                claim_token = gen_random_uuid(),
                attempts = 1
            WHERE id = $1
            """,
            first,
        )
        third = await conn.fetchval(
            """
            WITH msg AS (
              INSERT INTO inbound_messages (wa_message_id, phone, body, msg_type, raw, status)
              VALUES ('wamid.fifo.3', '919823000002', 'hello', 'text', '{}'::jsonb, 'queued')
              RETURNING id
            )
            INSERT INTO jobs (message_id, conversation_id, status)
            SELECT id, $1, 'pending' FROM msg
            RETURNING id
            """,
            other["id"],
        )

    claimed = await claim_job(pool)
    assert claimed is not None
    assert claimed["id"] == third
    assert claimed["id"] != second
    await release_claim(pool, claimed["id"], claimed["claim_token"])

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
            SET locked_until = now() - interval '5 seconds'
            WHERE id = $1
            """,
            first,
        )
    reclaimed = await claim_job(pool)
    assert reclaimed is not None
    assert reclaimed["id"] == first
    assert reclaimed["attempts"] >= 2


@pytest.mark.asyncio
async def test_slot_race_and_chaos(pool):
    async with httpx.AsyncClient() as client:
        health = await client.get("http://127.0.0.1:8001/health", timeout=2)
        health.raise_for_status()

    slot = "CBC:2099-05-01:0900"

    async def one(index: int):
        async with pool.acquire() as conn:
            conversation_id = await conn.fetchval(
                "INSERT INTO conversations (phone) VALUES ($1) RETURNING id",
                f"91971000{index:04d}",
            )
            return await reserve_and_book(
                conn,
                conversation_id=conversation_id,
                phone=f"91971000{index:04d}",
                test_code="CBC",
                slot_id=slot,
                attempt_id=uuid.uuid4(),
                patient_name="Synthetic Patient",
                memory={},
                chaos="none",
                max_attempts=1,
            )

    results = await asyncio.gather(*[one(index) for index in range(10)])
    confirmed = [item for item in results if item["status"] == "confirmed"]
    taken = [item for item in results if item["status"] == "slot_taken"]
    assert len(confirmed) == 1
    assert len(taken) == 9
    async with pool.acquire() as conn:
        lab_rows = await conn.fetchval("SELECT count(*) FROM lab.bookings")
        book_calls = await conn.fetchval(
            "SELECT count(*) FROM lab.call_log WHERE op = 'book' AND outcome = 'ok'"
        )
        conflicts = await conn.fetchval(
            "SELECT count(*) FROM lab.call_log WHERE op = 'book' AND outcome = 'conflict'"
        )
    assert lab_rows == 1
    assert book_calls == 1
    assert conflicts == 0

    await reset_for(pool)
    attempt = uuid.uuid4()
    async with pool.acquire() as conn:
        conversation_id = await conn.fetchval(
            "INSERT INTO conversations (phone) VALUES ('919710009999') RETURNING id"
        )
        pending = await reserve_and_book(
            conn,
            conversation_id=conversation_id,
            phone="919710009999",
            test_code="CBC",
            slot_id="CBC:2099-05-02:0900",
            attempt_id=attempt,
            patient_name="Synthetic Patient",
            memory={},
            chaos="fail-before",
            max_attempts=1,
        )
        assert pending["status"] == "pending"
        assert await conn.fetchval("SELECT count(*) FROM lab.bookings") == 0
        confirmed_row = await reserve_and_book(
            conn,
            conversation_id=conversation_id,
            phone="919710009999",
            test_code="CBC",
            slot_id="CBC:2099-05-02:0900",
            attempt_id=attempt,
            patient_name="Synthetic Patient",
            memory={"booking_status": "pending", "selected_slot": "CBC:2099-05-02:0900", "attempt_id": str(attempt)},
            chaos="none",
            max_attempts=2,
        )
        assert confirmed_row["status"] == "confirmed"
        assert await conn.fetchval("SELECT count(*) FROM lab.bookings") == 1


@pytest.mark.asyncio
async def test_timeout_after_commit_reconciles_once(pool):
    async with httpx.AsyncClient() as client:
        (await client.get("http://127.0.0.1:8001/health", timeout=2)).raise_for_status()
    attempt = uuid.uuid4()
    slot = "CBC:2099-05-03:1000"
    async with pool.acquire() as conn:
        conversation_id = await conn.fetchval(
            "INSERT INTO conversations (phone) VALUES ('919710008888') RETURNING id"
        )
        result = await reserve_and_book(
            conn,
            conversation_id=conversation_id,
            phone="919710008888",
            test_code="CBC",
            slot_id=slot,
            attempt_id=attempt,
            patient_name="Synthetic Patient",
            memory={},
            chaos="timeout-after-commit",
            max_attempts=1,
            timeout_s=0.4,
        )
        assert result["status"] == "pending"
        assert await conn.fetchval("SELECT count(*) FROM lab.bookings WHERE slot_id = $1", slot) == 1
    changed = await reconcile_once(pool, min_age_s=0)
    assert changed == 1
    async with pool.acquire() as conn:
        status = await conn.fetchval("SELECT status FROM bookings WHERE attempt_id = $1", attempt)
        lab_rows = await conn.fetchval("SELECT count(*) FROM lab.bookings WHERE slot_id = $1", slot)
        data = await snapshot(conn)
    assert status == "confirmed"
    assert lab_rows == 1
    assert data["double_booked_slots"] == 0
    assert data["lab_db_mismatches"] == 0
    text = render_prometheus(data)
    assert 'gateway_bookings{status="confirmed"} 1' in text


@pytest.mark.asyncio
async def test_agent_gates_and_single_booking(pool):
    async with httpx.AsyncClient() as client:
        (await client.get("http://127.0.0.1:8001/health", timeout=2)).raise_for_status()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO conversations (phone) VALUES ('919830000001') RETURNING *")
        conv = {"id": row["id"], "phone": row["phone"], "state": row["state"], "memory": {}}
        listed = await deterministic(conn, conv, "I want to book a CBC test on 2099-06-01")
        assert "09:00" in listed
        refused = await execute_tool(
            conn,
            conv,
            "no thanks",
            "book_test",
            {"test_code": "CBC", "slot_id": conv["memory"]["offered_slots"][0]["id"], "patient_name": "A"},
        )
        assert refused["error"] == "patient did not select this slot"
        assert await conn.fetchval("SELECT count(*) FROM bookings") == 0
        booked = await deterministic(conn, conv, "1")
        assert "Reference" in booked
        assert conv["state"] == "booked"
        again = await deterministic(conn, conv, "1")
        assert "already booked" in again.lower()
        assert await conn.fetchval("SELECT count(*) FROM lab.bookings") == 1
        blocked = await execute_tool(
            conn,
            conv,
            "1",
            "book_test",
            {"test_code": "CBC", "slot_id": "CBC:2099-06-01:0900", "patient_name": "A"},
        )
        assert "cannot book" in blocked["error"]

        row = await conn.fetchrow("INSERT INTO conversations (phone) VALUES ('919830000002') RETURNING *")
        other = {"id": row["id"], "phone": row["phone"], "state": row["state"], "memory": {}}
        handed = await deterministic(conn, other, "I need a doctor right away")
        assert other["state"] == "handed_off"
        assert "teammate" in handed.lower()
        follow = await deterministic(conn, other, "Book a CBC test on 2099-06-02")
        assert "already has this chat" in follow


@pytest.mark.asyncio
async def test_worker_processes_message_once(pool):
    async with httpx.AsyncClient() as client:
        (await client.get("http://127.0.0.1:8001/health", timeout=2)).raise_for_status()
    phone = "919830000010"
    async with pool.acquire() as conn:
        await accept_messages(conn, cloud_payload(phone, "I want to book a CBC test on 2099-07-01", "wamid.worker.1"))
    job = await claim_job(pool)
    await process_job(pool, job)
    async with pool.acquire() as conn:
        await accept_messages(conn, cloud_payload(phone, "1", "wamid.worker.2"))
    job = await claim_job(pool)
    await process_job(pool, job)
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM jobs WHERE status = 'done'") == 2
        assert await conn.fetchval("SELECT count(*) FROM outbox") == 2
        assert await conn.fetchval("SELECT count(*) FROM bookings WHERE status = 'confirmed'") == 1
        assert await conn.fetchval("SELECT count(*) FROM lab.bookings") == 1
        data = await snapshot(conn)
    assert data["double_booked_slots"] == 0
    assert data["lab_db_mismatches"] == 0


@pytest.mark.asyncio
async def test_spend_cap_is_global(pool, monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "openrouter")
    monkeypatch.setattr(settings, "openrouter_api_key", "sk-test")

    async def forbid(*_args, **_kwargs):
        raise AssertionError("live model was called after the cap was spent")

    monkeypatch.setattr("gateway.agent.live_turn", forbid)
    async with pool.acquire() as conn:
        from gateway.budget import commit_spend

        assert await commit_spend(conn, 400) is True
        assert await commit_spend(conn, 200) is False
        await conn.execute("UPDATE llm_budget SET spent_inr = cap_inr")
        row = await conn.fetchrow("INSERT INTO conversations (phone) VALUES ('919830000099') RETURNING *")
        conv = {"id": row["id"], "phone": row["phone"], "state": "collecting", "memory": {}}
        reply = await run_agent(conn, conv, "Hi")
        outcome = await conn.fetchval(
            "SELECT outcome FROM llm_usage WHERE conversation_id = $1",
            conv["id"],
        )
    assert "Which test" in reply
    assert outcome == "fallback"


async def reset_for(pool):
    from gateway.db import reset_database

    await reset_database(pool)
