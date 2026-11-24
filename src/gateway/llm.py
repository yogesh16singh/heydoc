import json
import logging

import httpx

from gateway.catalog import slot_label
from gateway.settings import settings
from gateway.tools import execute_tool

log = logging.getLogger("gateway.llm")

SYSTEM = (
    "You book diagnostic lab tests on WhatsApp. "
    "Allowed tests: CBC, LFT, THYROID, HBA1C, VITD. "
    "Call check_slots before you mention any time. "
    "Call book_test only for a slot the patient's latest message selected. "
    "Never invent slot ids or booking references. "
    "If the lab keeps failing, call handoff_to_human. "
    "Reply in two short sentences. Do not ask for clinical history."
)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "check_slots",
            "description": "List open slots for a test on a date.",
            "parameters": {
                "type": "object",
                "properties": {
                    "test_code": {"type": "string"},
                    "date": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["test_code", "date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "book_test",
            "description": "Book a slot the patient just selected.",
            "parameters": {
                "type": "object",
                "properties": {
                    "test_code": {"type": "string"},
                    "slot_id": {"type": "string"},
                    "patient_name": {"type": "string"},
                },
                "required": ["test_code", "slot_id", "patient_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "handoff_to_human",
            "description": "Hand the conversation to a person.",
            "parameters": {
                "type": "object",
                "properties": {"reason": {"type": "string"}},
                "required": ["reason"],
            },
        },
    },
]


def provider_name() -> str:
    if settings.llm_provider == "openrouter" and settings.openrouter_api_key:
        return "openrouter"
    return "mock"


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def cost_inr(input_tokens: int, output_tokens: int) -> float:
    usd = (
        input_tokens * settings.input_usd_per_mtok + output_tokens * settings.output_usd_per_mtok
    ) / 1_000_000
    return usd * settings.usd_inr


def render_reply(conv: dict, result: dict) -> str:
    memory = conv.get("memory") or {}
    status = result.get("status")
    if status == "confirmed":
        label = slot_label(memory, result.get("slot_id", ""))
        test = memory.get("test_code", "test")
        return f"Booked {test} at {label}. Reference {result.get('lab_booking_id')}."
    if status == "slot_taken":
        return "That slot was just taken. Reply with another number from the list."
    if status == "pending":
        return "I'm confirming that slot with the lab. I'll reply once it locks in."
    if status == "handed_off":
        return "I've handed this chat to a teammate. They will follow up here."
    return str(result.get("error") or "I couldn't complete that.")


async def live_turn(conn, conv: dict, user_text: str) -> tuple[str, dict]:
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM},
        {
            "role": "user",
            "content": json.dumps({"state": conv["state"], "memory": conv.get("memory") or {}}, default=str),
        },
        {"role": "user", "content": user_text},
    ]
    usage = {"input": 0, "output": 0}
    text = ""
    async with httpx.AsyncClient(timeout=30) as client:
        for _ in range(4):
            response = await client.post(
                f"{settings.llm_base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {settings.openrouter_api_key}"},
                json={"model": settings.llm_model, "messages": messages, "tools": TOOLS, "temperature": 0},
            )
            response.raise_for_status()
            data = response.json()
            prompt = int((data.get("usage") or {}).get("prompt_tokens") or 0)
            completion = int((data.get("usage") or {}).get("completion_tokens") or 0)
            usage["input"] += prompt
            usage["output"] += completion
            message = data["choices"][0]["message"]
            tool_calls = message.get("tool_calls") or []
            if not tool_calls:
                text = message.get("content") or ""
                break
            messages.append(message)
            for call in tool_calls:
                name = call["function"]["name"]
                raw_args = call["function"].get("arguments") or "{}"
                try:
                    args = json.loads(raw_args)
                except json.JSONDecodeError:
                    args = {}
                result = await execute_tool(conn, conv, user_text, name, args)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call["id"],
                        "content": json.dumps(result, default=str),
                    }
                )
            else:
                continue
            break
    return text, usage


async def record_usage(conn, conversation_id, model: str, input_tokens: int, output_tokens: int, cost: float, outcome: str) -> None:
    await conn.execute(
        """
        INSERT INTO llm_usage (conversation_id, model, input_tokens, output_tokens, cost_inr, outcome)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        conversation_id,
        model,
        input_tokens,
        output_tokens,
        cost,
        outcome,
    )
