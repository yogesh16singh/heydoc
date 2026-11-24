import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from gateway.budget import budget_allows, commit_spend
from gateway.catalog import detect_test, parse_date, wants_handoff
from gateway.llm import (
    SYSTEM,
    cost_inr,
    estimate_tokens,
    live_turn,
    provider_name,
    record_usage,
    render_reply,
)
from gateway.tools import execute_tool

log = logging.getLogger("gateway.agent")


async def run_agent(conn, conv: dict, user_text: str) -> str:
    name = provider_name()
    if name == "mock" or not await budget_allows(conn, 0.01):
        reply = await deterministic(conn, conv, user_text)
        outcome = "mock" if name == "mock" else "fallback"
        await _record_mock(conn, conv, user_text, reply, outcome)
        return reply
    try:
        text, usage = await live_turn(conn, conv, user_text)
        cost = cost_inr(usage["input"], usage["output"])
        charged = await commit_spend(conn, cost)
        if text:
            await record_usage(
                conn,
                conv["id"],
                settings_model(),
                usage["input"],
                usage["output"],
                cost,
                "ok" if charged else "fallback",
            )
            return text
    except Exception:
        log.exception("live model failed; using the deterministic policy")
    reply = await deterministic(conn, conv, user_text)
    await _record_mock(conn, conv, user_text, reply, "fallback")
    return reply


def settings_model() -> str:
    from gateway.settings import settings

    return settings.llm_model


async def deterministic(conn, conv: dict, user_text: str) -> str:
    if conv["state"] == "handed_off":
        return "A teammate already has this chat and will follow up."
    if wants_handoff(user_text):
        result = await execute_tool(conn, conv, user_text, "handoff_to_human", {"reason": "patient asked"})
        return render_reply(conv, result)
    if conv["state"] == "booked":
        ref = (conv.get("memory") or {}).get("booking_id") or "your booking"
        return f"You're already booked. Reference {ref}."

    memory = dict(conv.get("memory") or {})
    if conv["state"] == "offering":
        from gateway.catalog import selected_slot

        picked = selected_slot(user_text, memory.get("offered_slots") or [])
        if picked:
            result = await execute_tool(
                conn,
                conv,
                user_text,
                "book_test",
                {
                    "test_code": memory.get("test_code"),
                    "slot_id": picked,
                    "patient_name": memory.get("patient_name") or "Synthetic Patient",
                },
            )
            return render_reply(conv, result)
        if detect_test(user_text) is None:
            return "Reply with the number of the slot you want."

    test = detect_test(user_text) or memory.get("test_code")
    if not test:
        return "Which test should I book? CBC, LFT, Thyroid, HbA1c, or Vitamin D."
    day = parse_date(user_text) or memory.get("date")
    if not day:
        day = (datetime.now(ZoneInfo("Asia/Kolkata")).date() + timedelta(days=1)).isoformat()
    result = await execute_tool(
        conn,
        conv,
        user_text,
        "check_slots",
        {"test_code": test, "date": str(day)},
    )
    if result.get("error"):
        await execute_tool(conn, conv, user_text, "handoff_to_human", {"reason": "lab_unavailable"})
        return "The lab isn't answering right now. I've handed this to a teammate."
    lines = [f"{test} on {day}. Reply with a number:"]
    for index, slot in enumerate(result.get("slots") or [], start=1):
        lines.append(f"{index}. {slot['label']}")
    return "\n".join(lines)


async def _record_mock(conn, conv, user_text: str, reply: str, outcome: str) -> None:
    prompt = f"{SYSTEM}\n{conv.get('memory')}\n{user_text}"
    await record_usage(
        conn,
        conv["id"],
        "mock",
        estimate_tokens(prompt),
        estimate_tokens(reply),
        0,
        outcome,
    )
