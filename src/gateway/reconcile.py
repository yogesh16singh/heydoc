import logging

import asyncpg

from gateway import labclient
from gateway.settings import settings

log = logging.getLogger("gateway.reconcile")


async def reconcile_once(pool: asyncpg.Pool, min_age_s: float | None = None) -> int:
    age = settings.reconcile_min_age_s if min_age_s is None else min_age_s
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT b.id, b.attempt_id, b.conversation_id, b.phone, b.test_code, b.slot_id,
                   c.memory, c.state
            FROM bookings b
            JOIN conversations c ON c.id = b.conversation_id
            WHERE b.status = 'pending'
              AND b.updated_at < now() - make_interval(secs => $1)
              AND NOT EXISTS (
                SELECT 1 FROM jobs j
                WHERE j.conversation_id = b.conversation_id
                  AND j.status = 'running'
                  AND j.locked_until > now()
              )
            ORDER BY b.updated_at
            LIMIT 50
            """,
            age,
        )
    changed = 0
    for row in rows:
        attempt = str(row["attempt_id"])
        try:
            found = await labclient.get_booking(attempt)
        except labclient.LabUnavailable:
            log.info("reconcile left %s pending; lab unreachable", attempt)
            continue
        async with pool.acquire() as conn:
            if found:
                await _confirm(conn, row, found)
            else:
                await _release(conn, row)
        changed += 1
    return changed


async def _confirm(conn: asyncpg.Connection, row, found: dict) -> None:
    updated = await conn.fetchval(
        """
        UPDATE bookings
        SET status = 'confirmed',
            lab_booking_id = $2,
            last_error = NULL,
            updated_at = now()
        WHERE id = $1 AND status = 'pending'
        RETURNING id
        """,
        row["id"],
        found["lab_booking_id"],
    )
    if updated is None:
        return
    memory = dict(row["memory"] or {})
    memory["booking_status"] = "confirmed"
    memory["booking_id"] = found["lab_booking_id"]
    memory["attempt_id"] = str(row["attempt_id"])
    label = _label(memory, row["slot_id"])
    await conn.execute(
        """
        UPDATE conversations
        SET state = 'booked', memory = $2, updated_at = now()
        WHERE id = $1
        """,
        row["conversation_id"],
        memory,
    )
    await conn.execute(
        """
        INSERT INTO outbox (conversation_id, phone, body, status)
        VALUES ($1, $2, $3, 'sent')
        """,
        row["conversation_id"],
        row["phone"],
        f"Booked {row['test_code']} at {label}. Reference {found['lab_booking_id']}.",
    )


async def _release(conn: asyncpg.Connection, row) -> None:
    updated = await conn.fetchval(
        """
        UPDATE bookings
        SET status = 'failed', last_error = 'lab has no record', updated_at = now()
        WHERE id = $1 AND status = 'pending'
        RETURNING id
        """,
        row["id"],
    )
    if updated is None:
        return
    memory = dict(row["memory"] or {})
    memory["booking_status"] = "failed"
    memory.pop("attempt_id", None)
    memory.pop("selected_slot", None)
    await conn.execute(
        """
        UPDATE conversations
        SET state = 'offering', memory = $2, updated_at = now()
        WHERE id = $1 AND state <> 'handed_off'
        """,
        row["conversation_id"],
        memory,
    )
    await conn.execute(
        """
        INSERT INTO outbox (conversation_id, phone, body, status)
        VALUES ($1, $2, $3, 'sent')
        """,
        row["conversation_id"],
        row["phone"],
        "The lab did not confirm that slot. Reply with another number from the list.",
    )


def _label(memory: dict, slot_id: str) -> str:
    for slot in memory.get("offered_slots") or []:
        if slot.get("id") == slot_id:
            return slot.get("label") or slot_id
    return slot_id
