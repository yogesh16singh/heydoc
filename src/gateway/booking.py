import uuid

import asyncpg

from gateway import labclient


async def _save_conversation(conn: asyncpg.Connection, conversation_id, state: str, memory: dict) -> None:
    await conn.execute(
        """
        UPDATE conversations
        SET state = $2, memory = $3, updated_at = now()
        WHERE id = $1
        """,
        conversation_id,
        state,
        memory,
    )


async def reserve_and_book(
    conn: asyncpg.Connection,
    *,
    conversation_id,
    phone: str,
    test_code: str,
    slot_id: str,
    attempt_id: uuid.UUID,
    patient_name: str,
    memory: dict,
    chaos: str | None = None,
    max_attempts: int | None = None,
    timeout_s: float | None = None,
) -> dict:
    """Reserve the slot locally, then call the lab.

    The insert commits before the HTTP call (autocommit). The loser of the
    partial unique index never reaches the lab.
    """
    existing = await conn.fetchrow("SELECT * FROM bookings WHERE attempt_id = $1", attempt_id)
    if existing and existing["status"] == "confirmed":
        return {
            "status": "confirmed",
            "lab_booking_id": existing["lab_booking_id"],
            "slot_id": existing["slot_id"],
        }

    if existing and existing["status"] == "pending":
        if existing["slot_id"] != slot_id:
            return {"status": "error", "error": "attempt already reserved a different slot"}
    else:
        try:
            if existing and existing["status"] in ("failed", "cancelled"):
                await conn.execute(
                    """
                    UPDATE bookings
                    SET status = 'pending',
                        slot_id = $2,
                        test_code = $3,
                        patient_name = $4,
                        last_error = NULL,
                        updated_at = now()
                    WHERE attempt_id = $1
                      AND status IN ('failed', 'cancelled')
                    """,
                    attempt_id,
                    slot_id,
                    test_code,
                    patient_name,
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO bookings (
                      conversation_id, phone, test_code, slot_id, attempt_id, patient_name, status
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, 'pending')
                    """,
                    conversation_id,
                    phone,
                    test_code,
                    slot_id,
                    attempt_id,
                    patient_name,
                )
        except asyncpg.UniqueViolationError:
            holder = await conn.fetchrow(
                """
                SELECT attempt_id FROM bookings
                WHERE slot_id = $1 AND status IN ('pending', 'confirmed')
                """,
                slot_id,
            )
            if holder is None or holder["attempt_id"] != attempt_id:
                return {"status": "slot_taken", "slot_id": slot_id}

    await conn.execute(
        "UPDATE bookings SET updated_at = now() WHERE attempt_id = $1",
        attempt_id,
    )
    memory = dict(memory)
    memory["attempt_id"] = str(attempt_id)
    memory["selected_slot"] = slot_id
    memory["booking_status"] = "pending"
    memory["patient_name"] = patient_name
    await _save_conversation(conn, conversation_id, "offering", memory)

    result = await labclient.book(
        attempt_id=str(attempt_id),
        slot_id=slot_id,
        test_code=test_code,
        phone=phone,
        patient_name=patient_name,
        chaos=chaos,
        conn=conn,
        max_attempts=max_attempts,
        timeout_s=timeout_s,
    )

    if result["status"] == "confirmed":
        await _confirm(conn, attempt_id, result["lab_booking_id"], memory, conversation_id)
        return result

    if result["status"] == "conflict":
        try:
            found = await labclient.get_booking(str(attempt_id))
        except labclient.LabUnavailable:
            found = None
            await conn.execute(
                """
                UPDATE bookings
                SET last_error = 'conflict_unconfirmed', updated_at = now()
                WHERE attempt_id = $1 AND status = 'pending'
                """,
                attempt_id,
            )
            return {"status": "pending", "slot_id": slot_id}
        if found:
            await _confirm(conn, attempt_id, found["lab_booking_id"], memory, conversation_id)
            return {
                "status": "confirmed",
                "lab_booking_id": found["lab_booking_id"],
                "slot_id": found["slot_id"],
            }
        await conn.execute(
            """
            UPDATE bookings
            SET status = 'failed', last_error = 'conflict', updated_at = now()
            WHERE attempt_id = $1 AND status = 'pending'
            """,
            attempt_id,
        )
        memory["booking_status"] = "failed"
        memory.pop("attempt_id", None)
        await _save_conversation(conn, conversation_id, "offering", memory)
        return {"status": "slot_taken", "slot_id": slot_id}

    await conn.execute(
        """
        UPDATE bookings
        SET last_error = $2, updated_at = now()
        WHERE attempt_id = $1 AND status = 'pending'
        """,
        attempt_id,
        result["status"],
    )
    return {"status": "pending", "slot_id": slot_id, "error": result["status"]}


async def _confirm(conn, attempt_id, lab_booking_id: str, memory: dict, conversation_id) -> None:
    await conn.execute(
        """
        UPDATE bookings
        SET status = 'confirmed',
            lab_booking_id = $2,
            last_error = NULL,
            updated_at = now()
        WHERE attempt_id = $1 AND status = 'pending'
        """,
        attempt_id,
        lab_booking_id,
    )
    memory = dict(memory)
    memory["booking_status"] = "confirmed"
    memory["booking_id"] = lab_booking_id
    memory["attempt_id"] = str(attempt_id)
    await _save_conversation(conn, conversation_id, "booked", memory)
