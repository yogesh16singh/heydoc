import json
import uuid

import asyncpg

from gateway import labclient
from gateway.booking import reserve_and_book
from gateway.catalog import ALLOWED, selected_slot


async def _save(conn: asyncpg.Connection, conv: dict, state: str, memory: dict) -> None:
    await conn.execute(
        """
        UPDATE conversations
        SET state = $2, memory = $3, updated_at = now()
        WHERE id = $1
        """,
        conv["id"],
        state,
        memory,
    )
    conv["state"] = state
    conv["memory"] = memory


async def execute_tool(conn: asyncpg.Connection, conv: dict, user_text: str, name: str, args: dict) -> dict:
    state = conv["state"]
    memory = dict(conv.get("memory") or {})

    if name == "check_slots":
        if state not in ("collecting", "offering"):
            return {"error": f"cannot check slots from {state}"}
        test = str(args.get("test_code", "")).upper()
        if test not in ALLOWED:
            return {"error": "unknown test"}
        day = str(args.get("date") or "")
        if len(day) != 10:
            return {"error": "date must be YYYY-MM-DD"}
        result = await labclient.check_slots(test, day, conn=conn)
        if result.get("error"):
            return {"error": result["error"], "slots": []}
        open_slots = [slot for slot in (result.get("slots") or []) if not slot.get("taken")]
        if not open_slots:
            return {"error": "no slots", "slots": []}
        memory["test_code"] = test
        memory["date"] = day
        memory["offered_slots"] = [{"id": slot["id"], "label": slot["label"]} for slot in open_slots]
        if memory.get("booking_status") not in ("pending", "confirmed"):
            memory.pop("attempt_id", None)
            memory.pop("selected_slot", None)
        await _save(conn, conv, "offering", memory)
        return {"slots": memory["offered_slots"]}

    if name == "book_test":
        if state != "offering":
            return {"error": f"cannot book from {state}"}
        if memory.get("booking_status") == "confirmed":
            return {"error": "already booked"}
        slot_id = args.get("slot_id")
        offered = memory.get("offered_slots") or []
        offered_ids = {slot["id"] for slot in offered}
        if slot_id not in offered_ids:
            return {"error": "slot was not offered"}
        picked = selected_slot(user_text, offered)
        if picked != slot_id:
            return {"error": "patient did not select this slot"}
        if memory.get("booking_status") == "pending" and memory.get("selected_slot") == slot_id and memory.get("attempt_id"):
            attempt = uuid.UUID(str(memory["attempt_id"]))
        else:
            attempt = uuid.uuid4()
        patient = str(args.get("patient_name") or "Synthetic Patient")[:80]
        result = await reserve_and_book(
            conn,
            conversation_id=conv["id"],
            phone=conv["phone"],
            test_code=str(args.get("test_code") or memory.get("test_code") or ""),
            slot_id=slot_id,
            attempt_id=attempt,
            patient_name=patient,
            memory=memory,
        )
        fresh = await conn.fetchrow("SELECT state, memory FROM conversations WHERE id = $1", conv["id"])
        conv["state"] = fresh["state"]
        conv["memory"] = fresh["memory"] or {}
        return result

    if name == "handoff_to_human":
        if state == "handed_off":
            return {"error": "already handed off"}
        reason = str(args.get("reason") or "unspecified")[:500]
        summary = json.dumps({"state": state, "memory": memory, "reason": reason}, default=str)[:2000]
        await conn.execute(
            "INSERT INTO handoffs (conversation_id, reason, summary) VALUES ($1, $2, $3)",
            conv["id"],
            reason,
            summary,
        )
        await _save(conn, conv, "handed_off", memory)
        return {"status": "handed_off", "reason": reason}

    return {"error": f"unknown tool {name}"}
