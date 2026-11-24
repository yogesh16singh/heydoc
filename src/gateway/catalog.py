import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ALLOWED = ("CBC", "LFT", "THYROID", "HBA1C", "VITD")

_ALIASES = (
    ("VITAMIN D", "VITD"),
    ("VIT D", "VITD"),
    ("HBA1C", "HBA1C"),
    ("THYROID", "THYROID"),
    ("CBC", "CBC"),
    ("LFT", "LFT"),
    ("VITD", "VITD"),
)

_HANDOFF = re.compile(
    r"\b(human|person|doctor|receptionist)\b|speak to|talk to|hand ?off",
    re.IGNORECASE,
)


def detect_test(text: str) -> str | None:
    upper = text.upper()
    for alias, code in _ALIASES:
        if alias in upper:
            return code
    return None


def parse_date(text: str) -> str | None:
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    if "tomorrow" in text.lower():
        day = datetime.now(ZoneInfo("Asia/Kolkata")).date() + timedelta(days=1)
        return day.isoformat()
    return None


def wants_handoff(text: str) -> bool:
    return _HANDOFF.search(text) is not None


def selected_slot(user_text: str, offered: list[dict]) -> str | None:
    text = user_text.strip().lower()
    if text.isdigit():
        index = int(text) - 1
        if 0 <= index < len(offered):
            return offered[index]["id"]
        return None
    for slot in offered:
        label = str(slot.get("label", "")).lower()
        slot_id = str(slot.get("id", "")).lower()
        if slot_id and slot_id in text:
            return slot["id"]
        if label and label in text:
            return slot["id"]
    return None


def slot_label(memory: dict, slot_id: str) -> str:
    for slot in memory.get("offered_slots") or []:
        if slot.get("id") == slot_id:
            return str(slot.get("label") or slot_id)
    return slot_id
