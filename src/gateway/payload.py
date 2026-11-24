def cloud_payload(phone: str, text: str, wamid: str, msg_type: str = "text") -> dict:
    message: dict = {
        "from": phone,
        "id": wamid,
        "timestamp": "1700000000",
        "type": msg_type,
    }
    if msg_type == "text":
        message["text"] = {"body": text}
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {
                                "display_phone_number": "15550001111",
                                "phone_number_id": "100",
                            },
                            "contacts": [{"profile": {"name": "Synthetic Patient"}, "wa_id": phone}],
                            "messages": [message],
                        },
                    }
                ],
            }
        ],
    }


def extract_messages(payload: dict) -> list[dict]:
    found: list[dict] = []
    for entry in payload.get("entry") or []:
        for change in entry.get("changes") or []:
            value = change.get("value") or {}
            for message in value.get("messages") or []:
                found.append(message)
    return found
