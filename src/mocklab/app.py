import asyncio
import logging
import random
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timezone

import asyncpg
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from gateway.db import create_pool, migrate
from gateway.settings import settings

log = logging.getLogger("mocklab")
_rng = random.Random()
_rng_lock = asyncio.Lock()
_HOURS = (9, 10, 11, 14)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _rng.seed(settings.lab_seed)
    await migrate()
    app.state.pool = await create_pool()
    yield
    await app.state.pool.close()


app = FastAPI(title="mock lab", lifespan=lifespan)


def _pool(request: Request):
    return request.app.state.pool


async def _sleep_latency() -> None:
    high = settings.lab_max_latency_ms
    if high <= 0:
        return
    low = min(settings.lab_min_latency_ms, high)
    async with _rng_lock:
        delay_ms = _rng.randint(low, high)
    await asyncio.sleep(delay_ms / 1000)


async def _roll_fail() -> bool:
    async with _rng_lock:
        return _rng.random() < settings.lab_fail_rate


async def _log(conn, op: str, outcome: str) -> None:
    await conn.execute("INSERT INTO lab.call_log (op, outcome) VALUES ($1, $2)", op, outcome)


def _booking_body(row) -> dict:
    return {
        "idempotency_key": row["idempotency_key"],
        "slot_id": row["slot_id"],
        "test_code": row["test_code"],
        "phone": row["phone"],
        "patient_name": row["patient_name"],
        "lab_booking_id": row["lab_booking_id"],
    }


async def _ensure_slots(conn, test_code: str, day: date) -> list:
    rows = await conn.fetch(
        """
        SELECT slot_id, label FROM lab.slots
        WHERE test_code = $1 AND slot_date = $2
        ORDER BY starts_at
        """,
        test_code,
        day,
    )
    if rows:
        return rows
    for hour in _HOURS:
        slot_id = f"{test_code}:{day.isoformat()}:{hour:02d}00"
        starts = datetime.combine(day, time(hour, 0), tzinfo=timezone.utc)
        await conn.execute(
            """
            INSERT INTO lab.slots (slot_id, test_code, slot_date, label, starts_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (slot_id) DO NOTHING
            """,
            slot_id,
            test_code,
            day,
            f"{hour:02d}:00",
            starts,
        )
    return await conn.fetch(
        """
        SELECT slot_id, label FROM lab.slots
        WHERE test_code = $1 AND slot_date = $2
        ORDER BY starts_at
        """,
        test_code,
        day,
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/slots")
async def slots(request: Request, test_code: str, date: str):
    chaos = request.headers.get("x-lab-chaos")
    try:
        day = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        return JSONResponse({"error": "bad date"}, status_code=400)
    async with _pool(request).acquire() as conn:
        failure = await _maybe_fail(conn, "slots", chaos)
        if failure is not None:
            return failure
        rows = await _ensure_slots(conn, test_code.upper(), day)
        taken = {
            row["slot_id"]
            for row in await conn.fetch("SELECT slot_id FROM lab.bookings WHERE test_code = $1", test_code.upper())
        }
        await _log(conn, "slots", "ok")
    return {
        "test_code": test_code.upper(),
        "date": date,
        "slots": [{"id": row["slot_id"], "label": row["label"], "taken": row["slot_id"] in taken} for row in rows],
    }


@app.post("/bookings")
async def create_booking(request: Request):
    key = request.headers.get("idempotency-key")
    if not key:
        return JSONResponse({"error": "missing idempotency key"}, status_code=400)
    body = await request.json()
    chaos = request.headers.get("x-lab-chaos")
    required = ("slot_id", "test_code", "phone", "patient_name")
    if any(not body.get(field) for field in required):
        return JSONResponse({"error": "missing field"}, status_code=400)

    async with _pool(request).acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM lab.bookings WHERE idempotency_key = $1", key)
        if existing:
            await _log(conn, "book", "replay")
            return _booking_body(existing)

        if chaos == "conflict":
            await _log(conn, "book", "conflict")
            return JSONResponse({"error": "conflict"}, status_code=409)

        failure = await _maybe_fail(conn, "book", chaos)
        if failure is not None:
            return failure

        owner = await conn.fetchrow("SELECT idempotency_key FROM lab.bookings WHERE slot_id = $1", body["slot_id"])
        if owner:
            await _log(conn, "book", "conflict")
            return JSONResponse({"error": "slot taken"}, status_code=409)

        lab_booking_id = f"lab_{uuid.uuid4().hex[:12]}"
        try:
            await conn.execute(
                """
                INSERT INTO lab.bookings (
                  idempotency_key, slot_id, test_code, phone, patient_name, lab_booking_id
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                key,
                body["slot_id"],
                str(body["test_code"]).upper(),
                body["phone"],
                body["patient_name"],
                lab_booking_id,
            )
        except asyncpg.UniqueViolationError:
            existing = await conn.fetchrow("SELECT * FROM lab.bookings WHERE idempotency_key = $1", key)
            if existing:
                await _log(conn, "book", "replay")
                return _booking_body(existing)
            await _log(conn, "book", "conflict")
            return JSONResponse({"error": "slot taken"}, status_code=409)

        await _log(conn, "book", "ok")
        payload = {
            "idempotency_key": key,
            "slot_id": body["slot_id"],
            "test_code": str(body["test_code"]).upper(),
            "phone": body["phone"],
            "patient_name": body["patient_name"],
            "lab_booking_id": lab_booking_id,
        }

    if chaos == "timeout-after-commit":
        await asyncio.sleep(settings.chaos_hold_s)
    return JSONResponse(payload, status_code=201)


@app.get("/bookings/by-key/{attempt_id}")
async def booking_by_key(request: Request, attempt_id: str):
    chaos = request.headers.get("x-lab-chaos")
    async with _pool(request).acquire() as conn:
        if chaos not in (None, "", "none"):
            failure = await _maybe_fail(conn, "get", chaos)
            if failure is not None:
                return failure
        row = await conn.fetchrow("SELECT * FROM lab.bookings WHERE idempotency_key = $1", attempt_id)
        await _log(conn, "get", "ok" if row else "miss")
    if row is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return _booking_body(row)


@app.get("/__admin/state")
async def admin_state(request: Request):
    async with _pool(request).acquire() as conn:
        bookings = await conn.fetch("SELECT * FROM lab.bookings ORDER BY created_at")
        calls = await conn.fetch(
            "SELECT op, outcome, count(*) AS n FROM lab.call_log GROUP BY op, outcome ORDER BY op"
        )
    return {
        "bookings": [_booking_body(row) for row in bookings],
        "calls": [{"op": row["op"], "outcome": row["outcome"], "count": int(row["n"])} for row in calls],
    }


@app.post("/__admin/reset")
async def admin_reset(request: Request):
    async with _pool(request).acquire() as conn:
        await conn.execute("TRUNCATE lab.bookings, lab.call_log, lab.slots")
    return {"status": "reset"}


async def _maybe_fail(conn, op: str, chaos: str | None):
    if chaos == "none":
        return None
    if chaos == "fail-before":
        await _sleep_latency()
        await _log(conn, op, "fail")
        return JSONResponse({"error": "injected failure"}, status_code=503)
    if chaos in (None, ""):
        await _sleep_latency()
        if await _roll_fail():
            await _log(conn, op, "fail")
            return JSONResponse({"error": "injected failure"}, status_code=503)
    return None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=settings.lab_port, log_level="info")
