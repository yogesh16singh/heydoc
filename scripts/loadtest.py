"""Send 1,000 signed WhatsApp webhooks and wait until the queue is idle."""

import asyncio
import json
import math
import os
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import httpx

SECRET = os.environ.get("APP_SECRET", "dev-app-secret")
BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
ROOT = Path(__file__).resolve().parents[1]


def sign(body: bytes) -> str:
    import hashlib
    import hmac

    digest = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def payload(phone: str, text: str, wamid: str) -> bytes:
    body = {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "messages": [
                                {
                                    "from": phone,
                                    "id": wamid,
                                    "timestamp": "1700000000",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }
    return json.dumps(body).encode()


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def script(day: str) -> list[str]:
    return [
        "Hi",
        f"I want to book a CBC test on {day}",
        "1",
        "thanks",
        "ok",
        "bye",
        "hello again",
        "got it",
    ]


def post(client: httpx.Client, raw: bytes) -> tuple[int, float]:
    started = time.perf_counter()
    response = client.post(
        "/webhook",
        content=raw,
        headers={"x-hub-signature-256": sign(raw), "content-type": "application/json"},
    )
    return response.status_code, (time.perf_counter() - started) * 1000


async def wait_until(client: httpx.AsyncClient, predicate, timeout_s: float):
    deadline = time.perf_counter() + timeout_s
    last = {}
    while time.perf_counter() < deadline:
        response = await client.get("/metrics/summary")
        response.raise_for_status()
        last = response.json()
        if predicate(last):
            return last
        await asyncio.sleep(0.5)
    raise TimeoutError(f"queue did not settle: {json.dumps(last)[:500]}")


async def main() -> None:
    phones = []
    base = date(2026, 12, 1)
    for index in range(100):
        phone = f"91981{index:06d}"
        day = (base + timedelta(days=index)).isoformat()
        messages = []
        for step, text in enumerate(script(day)):
            wamid = f"wamid.load.{index}.{step}.{uuid.uuid4().hex[:6]}"
            raw = payload(phone, text, wamid)
            messages.append(raw)
            if step in (0, 4):
                messages.append(raw)
        phones.append(messages)

    assert sum(len(items) for items in phones) == 1000

    async with httpx.AsyncClient(base_url=BASE, timeout=30) as client:
        ready = await client.get("/readyz")
        ready.raise_for_status()
        statuses: list[int] = []
        latencies: list[float] = []

        def send_phone(messages: list[bytes]) -> list[tuple[int, float]]:
            # One connection per in-flight phone, same shape as parallel webhook deliveries.
            with httpx.Client(base_url=BASE, timeout=30) as phone_client:
                return [post(phone_client, raw) for raw in messages]

        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=25) as pool:
            for batch in pool.map(send_phone, phones):
                for status, elapsed in batch:
                    statuses.append(status)
                    latencies.append(elapsed)
        send_seconds = time.perf_counter() - started

        def drained(summary: dict) -> bool:
            jobs = summary["jobs"]
            return jobs["pending"] == 0 and jobs["running"] == 0 and summary["bookings"]["pending"] == 0

        summary = await wait_until(client, drained, 240)
        drain_seconds = time.perf_counter() - started

        burst_phone = "919899990099"
        with httpx.Client(base_url=BASE, timeout=30) as burst_client:
            for index in range(25):
                raw = payload(burst_phone, f"hello {index}", f"wamid.burst.{index}")
                status, _ = post(burst_client, raw)
                statuses.append(status)
        summary = await wait_until(client, drained, 120)

    report = {
        "messages": 1000,
        "duplicates_sent": 200,
        "http_200": statuses[:1000].count(200),
        "http_200_including_burst": statuses.count(200),
        "ack_ms": {
            "p50": round(percentile(latencies, 0.50), 2),
            "p95": round(percentile(latencies, 0.95), 2),
            "p99": round(percentile(latencies, 0.99), 2),
            "max": round(max(latencies), 2),
            "mean": round(statistics.fmean(latencies), 2),
        },
        "send_seconds": round(send_seconds, 2),
        "drain_seconds": round(drain_seconds, 2),
        "accepted": summary["accepted"],
        "duplicates": summary["duplicates"],
        "rate_limited": summary["rate_limited"],
        "jobs": summary["jobs"],
        "bookings": summary["bookings"],
        "lab_calls": summary["lab_calls"],
        "handoffs": summary["handoffs"],
        "double_booked_slots": summary["double_booked_slots"],
        "lab_db_mismatches": summary["lab_db_mismatches"],
        "llm": summary["llm"],
        "seed": int(os.environ.get("LAB_SEED", "7")),
    }
    (ROOT / "load-report.json").write_text(json.dumps(report, indent=2) + "\n")
    retries = sum(
        row["count"] for row in summary["lab_calls"] if row["outcome"] in {"http_503", "timeout", "error"}
    )
    lines = [
        "# Load report",
        "",
        "1,000 signed posts: 800 unique messages (100 phones, 8 each, in order) and 200 replays of earlier ids. 25 phones in flight. Each phone books CBC on its own date. The lab is slow (500–1500 ms) and fails about 20% of calls before commit. A 25-message burst from one new phone follows, to show the rate limit.",
        "",
        "The script waits until pending and running jobs are zero and no booking is still `pending`.",
        "",
        "| Metric | Result |",
        "|---|---|",
        f"| HTTP 200 (first 1,000) | {report['http_200']} |",
        f"| Ack p50 / p95 / p99 / max | {report['ack_ms']['p50']} / {report['ack_ms']['p95']} / {report['ack_ms']['p99']} / {report['ack_ms']['max']} ms |",
        f"| Send phase | {report['send_seconds']} s |",
        f"| Queue idle | {report['drain_seconds']} s |",
        f"| Accepted | {report['accepted']} |",
        f"| Duplicates suppressed | {report['duplicates']} |",
        f"| Rate limited | {report['rate_limited']} |",
        f"| Jobs done / dead | {report['jobs']['done']} / {report['jobs']['dead']} (done includes the 20 burst messages under the cap) |",
        f"| Confirmed bookings | {report['bookings']['confirmed']} |",
        f"| Double-booked slots | {report['double_booked_slots']} |",
        f"| Lab vs database mismatches | {report['lab_db_mismatches']} |",
        f"| Lab retries recorded | {retries} |",
        f"| Handoffs | {report['handoffs']} |",
        f"| LLM spend | ₹{report['llm']['cost_inr']:.2f} |",
        f"| Lab RNG seed | {report['seed']} |",
        "",
        "Pass line: p99 under 200 ms, duplicates 200, dead jobs 0, double-booked slots 0, mismatches 0, rate limited greater than 0.",
        "",
    ]
    (ROOT / "LOAD_REPORT.md").write_text("\n".join(lines))
    print(json.dumps(report, indent=2))
    problems = []
    if report["http_200"] != 1000:
        problems.append("not all webhooks returned 200")
    if report["ack_ms"]["p99"] >= 200:
        problems.append("p99 ack is not under 200 ms")
    if report["duplicates"] != 200:
        problems.append(f"duplicates={report['duplicates']}")
    if report["jobs"]["dead"] != 0:
        problems.append("dead jobs")
    if report["double_booked_slots"] != 0:
        problems.append("double booking")
    if report["lab_db_mismatches"] != 0:
        problems.append("lab/database mismatch")
    if report["rate_limited"] <= 0:
        problems.append("rate limit did not fire")
    if problems:
        raise SystemExit("load failed: " + "; ".join(problems))


if __name__ == "__main__":
    asyncio.run(main())
