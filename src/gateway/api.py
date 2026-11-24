import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from gateway.db import create_pool, migrate, reset_database
from gateway.metrics import ack_latency, conversation_page, render_prometheus, snapshot
from gateway.payload import cloud_payload
from gateway.settings import settings
from gateway.signature import sign, verify
from gateway.webhook import MalformedPayload, accept_messages, loads_payload

log = logging.getLogger("gateway.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await migrate()
    app.state.pool = await create_pool()
    yield
    await app.state.pool.close()


app = FastAPI(title="WhatsApp Agent Gateway", lifespan=lifespan)


def _dev() -> None:
    if not settings.dev_mode:
        raise HTTPException(status_code=404, detail="not found")


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(request: Request):
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    return {"status": "ready"}


@app.get("/webhook")
async def verify_webhook(request: Request):
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    if mode == "subscribe" and token == settings.verify_token and challenge is not None:
        return PlainTextResponse(challenge)
    return PlainTextResponse("forbidden", status_code=403)


@app.post("/webhook")
async def webhook(request: Request):
    raw = await request.body()
    started = time.perf_counter()
    header = request.headers.get("x-hub-signature-256")
    if not verify(raw, header, settings.app_secret):
        ack_latency.observe(time.perf_counter() - started)
        return JSONResponse({"error": "bad signature"}, status_code=401)
    try:
        payload = loads_payload(raw)
    except MalformedPayload:
        ack_latency.observe(time.perf_counter() - started)
        return JSONResponse({"error": "malformed"}, status_code=400)
    try:
        async with request.app.state.pool.acquire() as conn:
            await accept_messages(conn, payload)
    except MalformedPayload:
        ack_latency.observe(time.perf_counter() - started)
        return JSONResponse({"error": "malformed"}, status_code=400)
    except (asyncpg.PostgresError, OSError):
        log.exception("webhook database error")
        ack_latency.observe(time.perf_counter() - started)
        return JSONResponse({"error": "unavailable"}, status_code=503)
    ack_latency.observe(time.perf_counter() - started)
    return {"status": "ok"}


@app.get("/metrics")
async def metrics(request: Request):
    async with request.app.state.pool.acquire() as conn:
        data = await snapshot(conn)
    return PlainTextResponse(render_prometheus(data), media_type="text/plain; version=0.0.4")


@app.get("/metrics/summary")
async def metrics_summary(request: Request):
    async with request.app.state.pool.acquire() as conn:
        return await snapshot(conn)


@app.get("/metrics/conversations")
async def metrics_conversations(request: Request, limit: int = 50, offset: int = 0):
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    async with request.app.state.pool.acquire() as conn:
        rows = await conversation_page(conn, limit, offset)
    return {"conversations": rows, "limit": limit, "offset": offset}


@app.get("/demo", response_class=HTMLResponse)
async def demo():
    _dev()
    path = Path(__file__).resolve().parents[2] / "web" / "demo.html"
    return HTMLResponse(path.read_text())


class SimulateIn(BaseModel):
    phone: str = Field(min_length=8, max_length=15)
    text: str = Field(min_length=1, max_length=500)
    wamid: str | None = None


@app.post("/dev/simulate")
async def simulate(body: SimulateIn, request: Request):
    _dev()
    wamid = body.wamid or f"wamid.{uuid.uuid4().hex}"
    payload = cloud_payload(body.phone, body.text, wamid)
    raw = json.dumps(payload).encode()
    header = sign(raw, settings.app_secret)
    started = time.perf_counter()
    if not verify(raw, header, settings.app_secret):
        return JSONResponse({"error": "bad signature"}, status_code=401)
    try:
        async with request.app.state.pool.acquire() as conn:
            results = await accept_messages(conn, payload)
    except (asyncpg.PostgresError, OSError):
        return JSONResponse({"error": "unavailable"}, status_code=503)
    ack_latency.observe(time.perf_counter() - started)
    return {"status": "ok", "wamid": wamid, "results": results}


@app.post("/dev/reset")
async def reset(request: Request):
    _dev()
    await reset_database(request.app.state.pool)
    return {"status": "reset"}


@app.get("/dev/conversation")
async def conversation(request: Request, phone: str):
    _dev()
    async with request.app.state.pool.acquire() as conn:
        conv = await conn.fetchrow("SELECT id, phone, state, memory FROM conversations WHERE phone = $1", phone)
        if conv is None:
            return {"phone": phone, "state": None, "turns": [], "bookings": []}
        turns = await conn.fetch(
            """
            SELECT body, direction, ts FROM (
              SELECT body, 'in' AS direction, received_at AS ts
              FROM inbound_messages WHERE phone = $1
              UNION ALL
              SELECT body, 'out' AS direction, created_at AS ts
              FROM outbox WHERE phone = $1
            ) transcript
            ORDER BY ts
            """,
            phone,
        )
        bookings = await conn.fetch(
            """
            SELECT slot_id, status, lab_booking_id, attempt_id::text AS attempt_id
            FROM bookings WHERE phone = $1 ORDER BY created_at
            """,
            phone,
        )
    return {
        "phone": phone,
        "state": conv["state"],
        "memory": conv["memory"],
        "turns": [{"body": row["body"], "direction": row["direction"]} for row in turns],
        "bookings": [dict(row) for row in bookings],
    }


@app.get("/dev/lab")
async def dev_lab(request: Request):
    _dev()
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT idempotency_key, slot_id, test_code, phone, lab_booking_id
            FROM lab.bookings ORDER BY created_at
            """
        )
    return {"bookings": [dict(row) for row in rows]}


@app.post("/dev/race")
async def race(request: Request):
    """Ten patients ask for the same CBC slot. One booking must win."""
    _dev()
    day = "2099-03-01"
    phones = [f"91970000{index:04d}" for index in range(10)]
    pool = request.app.state.pool
    for phone in phones:
        payload = cloud_payload(phone, f"I want to book a CBC test on {day}", f"wamid.race.{phone}.a.{uuid.uuid4().hex[:8]}")
        async with pool.acquire() as conn:
            await accept_messages(conn, payload)

    states = await _wait_states(pool, phones, {"offering", "booked", "handed_off"}, timeout_s=45)
    for phone in phones:
        if states.get(phone) != "offering":
            continue
        payload = cloud_payload(phone, "1", f"wamid.race.{phone}.b.{uuid.uuid4().hex[:8]}")
        async with pool.acquire() as conn:
            await accept_messages(conn, payload)

    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                """
                SELECT count(*) FROM jobs j
                JOIN conversations c ON c.id = j.conversation_id
                WHERE c.phone = ANY($1::text[])
                  AND j.status IN ('pending', 'running')
                """,
                phones,
            )
        if pending == 0:
            break
        await asyncio.sleep(0.25)

    async with pool.acquire() as conn:
        confirmed = await conn.fetchval(
            """
            SELECT count(*) FROM bookings
            WHERE phone = ANY($1::text[]) AND status = 'confirmed'
            """,
            phones,
        )
        lab_rows = await conn.fetchval(
            "SELECT count(*) FROM lab.bookings WHERE slot_id = $1",
            f"CBC:{day}:0900",
        )
    return {"date": day, "phones": phones, "confirmed": int(confirmed or 0), "lab_rows_for_first_slot": int(lab_rows or 0)}


async def _wait_states(pool, phones: list[str], wanted: set[str], timeout_s: float) -> dict[str, str]:
    states: dict[str, str] = {}
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT phone, state FROM conversations WHERE phone = ANY($1::text[])",
                phones,
            )
        states = {row["phone"]: row["state"] for row in rows}
        if len(states) == len(phones) and all(state in wanted for state in states.values()):
            return states
        await asyncio.sleep(0.25)
    return states


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=settings.api_port, log_level="info")


if __name__ == "__main__":
    main()
