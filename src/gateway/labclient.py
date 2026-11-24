import logging
import random

import httpx

from gateway.settings import settings

log = logging.getLogger("gateway.lab")


class LabUnavailable(Exception):
    pass


def _headers(chaos: str | None) -> dict[str, str]:
    chosen = settings.lab_client_chaos if chaos is None else chaos
    if not chosen:
        return {}
    return {"X-Lab-Chaos": chosen}


def _backoff(attempt: int) -> float:
    base = min(5.0, 0.2 * (2**attempt))
    return base * random.uniform(0.1, 1.0)


async def _record(conn, op: str, outcome: str, attempt_id: str | None) -> None:
    if conn is None:
        return
    await conn.execute(
        "INSERT INTO lab_calls (op, outcome, attempt_id) VALUES ($1, $2, $3)",
        op,
        outcome,
        attempt_id,
    )


async def check_slots(test_code: str, day: str, *, chaos: str | None = None, conn=None) -> dict:
    url = f"{settings.lab_base_url.rstrip('/')}/slots"
    headers = _headers(chaos)
    last_error = "unavailable"
    async with httpx.AsyncClient() as client:
        for attempt in range(settings.lab_client_max_attempts):
            try:
                response = await client.get(
                    url,
                    params={"test_code": test_code, "date": day},
                    headers=headers,
                    timeout=settings.lab_client_timeout_s,
                )
            except httpx.TimeoutException:
                await _record(conn, "check_slots", "timeout", None)
                last_error = "timeout"
            except httpx.HTTPError as exc:
                log.warning("check_slots transport error: %s", exc)
                await _record(conn, "check_slots", "error", None)
                last_error = "unavailable"
            else:
                if response.status_code == 200:
                    await _record(conn, "check_slots", "ok", None)
                    return response.json()
                if response.status_code in (429, 500, 503):
                    await _record(conn, "check_slots", "http_503", None)
                    last_error = "unavailable"
                else:
                    await _record(conn, "check_slots", "error", None)
                    return {"error": f"lab_{response.status_code}", "slots": []}
            if attempt + 1 < settings.lab_client_max_attempts:
                await _async_sleep(_backoff(attempt))
    return {"error": last_error, "slots": []}


async def book(
    *,
    attempt_id: str,
    slot_id: str,
    test_code: str,
    phone: str,
    patient_name: str,
    chaos: str | None = None,
    conn=None,
    max_attempts: int | None = None,
    timeout_s: float | None = None,
) -> dict:
    url = f"{settings.lab_base_url.rstrip('/')}/bookings"
    headers = _headers(chaos)
    headers["Idempotency-Key"] = attempt_id
    body = {
        "slot_id": slot_id,
        "test_code": test_code,
        "phone": phone,
        "patient_name": patient_name,
    }
    attempts = max_attempts or settings.lab_client_max_attempts
    timeout = settings.lab_client_timeout_s if timeout_s is None else timeout_s
    last = "unavailable"
    async with httpx.AsyncClient() as client:
        for attempt in range(attempts):
            try:
                response = await client.post(url, json=body, headers=headers, timeout=timeout)
            except httpx.TimeoutException:
                await _record(conn, "book", "timeout", attempt_id)
                last = "timeout"
            except httpx.HTTPError as exc:
                log.warning("book transport error: %s", exc)
                await _record(conn, "book", "error", attempt_id)
                last = "unavailable"
            else:
                if response.status_code in (200, 201):
                    await _record(conn, "book", "ok", attempt_id)
                    data = response.json()
                    return {
                        "status": "confirmed",
                        "lab_booking_id": data["lab_booking_id"],
                        "slot_id": data["slot_id"],
                    }
                if response.status_code == 409:
                    await _record(conn, "book", "conflict", attempt_id)
                    return {"status": "conflict", "slot_id": slot_id}
                if response.status_code in (429, 500, 503):
                    await _record(conn, "book", "http_503", attempt_id)
                    last = "unavailable"
                else:
                    await _record(conn, "book", "error", attempt_id)
                    return {"status": "error", "slot_id": slot_id, "code": response.status_code}
            if attempt + 1 < attempts:
                await _async_sleep(_backoff(attempt))
    return {"status": "timeout" if last == "timeout" else "unavailable", "slot_id": slot_id}


async def get_booking(attempt_id: str, *, chaos: str | None = "none") -> dict | None:
    url = f"{settings.lab_base_url.rstrip('/')}/bookings/by-key/{attempt_id}"
    headers = _headers(chaos)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=settings.lab_client_timeout_s)
    except httpx.HTTPError as exc:
        raise LabUnavailable(str(exc)) from exc
    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise LabUnavailable(f"lab status {response.status_code}")
    return response.json()


async def _async_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
