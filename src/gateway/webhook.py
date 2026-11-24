import json

import asyncpg

from gateway.payload import extract_messages
from gateway.settings import settings


class MalformedPayload(Exception):
    pass


async def accept_messages(conn: asyncpg.Connection, payload: dict) -> list[dict]:
    messages = extract_messages(payload)
    if not messages:
        if "entry" not in payload:
            raise MalformedPayload("missing entry")
        return []
    prepared = []
    for message in messages:
        wa_id = message.get("id")
        phone = message.get("from")
        if not wa_id or not phone:
            raise MalformedPayload("message missing id or from")
        msg_type = message.get("type") or "text"
        body = None
        if msg_type == "text":
            body = ((message.get("text") or {}).get("body"))
        prepared.append((str(wa_id), str(phone), body, msg_type))

    results = []
    for wa_id, phone, body, msg_type in prepared:
        row = await conn.fetchrow(
            "SELECT * FROM accept_inbound($1, $2, $3, $4, $5, $6)",
            wa_id,
            phone,
            body,
            msg_type,
            payload,
            settings.rate_limit_per_minute,
        )
        results.append(
            {
                "wamid": wa_id,
                "phone": phone,
                "inserted": row["inserted"],
                "queued": row["queued"],
                "rate_limited": row["rate_limited"],
                "duplicate": row["duplicate"],
                "ignored": row["ignored"],
            }
        )
    return results


def loads_payload(raw: bytes) -> dict:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MalformedPayload("invalid json") from exc
    if not isinstance(payload, dict):
        raise MalformedPayload("payload must be an object")
    return payload
