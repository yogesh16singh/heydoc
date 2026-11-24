import threading

_BOUNDS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2)


class LatencyHistogram:
    def __init__(self) -> None:
        self._counts = [0] * (len(_BOUNDS) + 1)
        self.total = 0.0
        self.count = 0
        self._lock = threading.Lock()

    def observe(self, seconds: float) -> None:
        with self._lock:
            self.count += 1
            self.total += seconds
            for index, bound in enumerate(_BOUNDS):
                if seconds <= bound:
                    self._counts[index] += 1
                    return
            self._counts[-1] += 1

    def render(self, name: str) -> str:
        with self._lock:
            counts = list(self._counts)
            total = self.total
            count = self.count
        lines = [
            f"# HELP {name} Webhook handler latency in seconds.",
            f"# TYPE {name} histogram",
        ]
        cumulative = 0
        for bound, raw in zip(_BOUNDS, counts):
            cumulative += raw
            lines.append(f'{name}_bucket{{le="{bound}"}} {cumulative}')
        cumulative += counts[-1]
        lines.append(f'{name}_bucket{{le="+Inf"}} {cumulative}')
        lines.append(f"{name}_sum {total:.6f}")
        lines.append(f"{name}_count {count}")
        return "\n".join(lines)


ack_latency = LatencyHistogram()


def mask_phone(phone: str) -> str:
    if len(phone) < 6:
        return "***"
    return f"{phone[:4]}***{phone[-2:]}"


async def snapshot(conn) -> dict:
    counters = {
        row["kind"]: int(row["n"])
        for row in await conn.fetch("SELECT kind, count(*) AS n FROM ingress_events GROUP BY kind")
    }
    jobs = {row["status"]: int(row["n"]) for row in await conn.fetch("SELECT status, count(*) AS n FROM jobs GROUP BY status")}
    bookings = {
        row["status"]: int(row["n"])
        for row in await conn.fetch("SELECT status, count(*) AS n FROM bookings GROUP BY status")
    }
    lab_rows = await conn.fetch("SELECT op, outcome, count(*) AS n FROM lab_calls GROUP BY op, outcome")
    usage = await conn.fetchrow(
        """
        SELECT COALESCE(sum(input_tokens), 0) AS input_tokens,
               COALESCE(sum(output_tokens), 0) AS output_tokens,
               COALESCE(sum(cost_inr), 0) AS cost_inr
        FROM llm_usage
        """
    )
    double_booked = int(
        await conn.fetchval(
            """
            SELECT count(*) FROM (
              SELECT slot_id FROM bookings
              WHERE status IN ('pending', 'confirmed')
              GROUP BY slot_id
              HAVING count(*) > 1
            ) doubled
            """
        )
    )
    mismatches = int(
        await conn.fetchval(
            """
            SELECT
              (SELECT count(*) FROM bookings b
                 LEFT JOIN lab.bookings l ON l.idempotency_key = b.attempt_id::text
                 WHERE b.status = 'confirmed'
                   AND (l.idempotency_key IS NULL OR l.slot_id <> b.slot_id))
              +
              (SELECT count(*) FROM lab.bookings l
                 LEFT JOIN bookings b
                   ON b.attempt_id::text = l.idempotency_key
                  AND b.status = 'confirmed'
                 WHERE b.id IS NULL)
            """
        )
    )
    handoffs = int(await conn.fetchval("SELECT count(*) FROM handoffs"))
    for status in ("pending", "running", "done", "dead"):
        jobs.setdefault(status, 0)
    for status in ("pending", "confirmed", "failed", "cancelled"):
        bookings.setdefault(status, 0)
    return {
        "accepted": counters.get("accepted", 0),
        "duplicates": counters.get("duplicates", 0),
        "rate_limited": counters.get("rate_limited", 0),
        "ignored": counters.get("ignored", 0),
        "jobs": jobs,
        "bookings": bookings,
        "lab_calls": [
            {"op": row["op"], "outcome": row["outcome"], "count": int(row["n"])} for row in lab_rows
        ],
        "llm": {
            "input_tokens": int(usage["input_tokens"]),
            "output_tokens": int(usage["output_tokens"]),
            "cost_inr": float(usage["cost_inr"]),
        },
        "handoffs": handoffs,
        "double_booked_slots": double_booked,
        "lab_db_mismatches": mismatches,
        "ack": {"count": ack_latency.count, "sum_seconds": ack_latency.total},
    }


def render_prometheus(data: dict) -> str:
    lines = [
        ack_latency.render("gateway_webhook_duration_seconds"),
        "# HELP gateway_webhook_accepted_total Messages that enqueued a job.",
        "# TYPE gateway_webhook_accepted_total counter",
        f"gateway_webhook_accepted_total {data['accepted']}",
        "# TYPE gateway_duplicate_messages_total counter",
        f"gateway_duplicate_messages_total {data['duplicates']}",
        "# TYPE gateway_rate_limited_total counter",
        f"gateway_rate_limited_total {data['rate_limited']}",
        "# TYPE gateway_ignored_messages_total counter",
        f"gateway_ignored_messages_total {data['ignored']}",
        "# TYPE gateway_queue_depth gauge",
    ]
    for status, count in data["jobs"].items():
        lines.append(f'gateway_queue_depth{{status="{status}"}} {count}')
    lines.append("# TYPE gateway_bookings gauge")
    for status, count in data["bookings"].items():
        lines.append(f'gateway_bookings{{status="{status}"}} {count}')
    lines.append("# TYPE gateway_lab_calls_total counter")
    for row in data["lab_calls"]:
        lines.append(f'gateway_lab_calls_total{{op="{row["op"]}",outcome="{row["outcome"]}"}} {row["count"]}')
    lines.extend(
        [
            "# TYPE gateway_llm_tokens_total counter",
            f'gateway_llm_tokens_total{{kind="input"}} {data["llm"]["input_tokens"]}',
            f'gateway_llm_tokens_total{{kind="output"}} {data["llm"]["output_tokens"]}',
            "# TYPE gateway_llm_cost_inr gauge",
            f"gateway_llm_cost_inr {data['llm']['cost_inr']:.6f}",
            "# TYPE gateway_handoffs_total counter",
            f"gateway_handoffs_total {data['handoffs']}",
            "# TYPE gateway_double_booked_slots gauge",
            f"gateway_double_booked_slots {data['double_booked_slots']}",
            "# TYPE gateway_lab_db_mismatches gauge",
            f"gateway_lab_db_mismatches {data['lab_db_mismatches']}",
        ]
    )
    return "\n".join(lines) + "\n"


async def conversation_page(conn, limit: int, offset: int) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT c.id, c.phone, c.state,
               COALESCE(sum(u.input_tokens), 0) AS input_tokens,
               COALESCE(sum(u.output_tokens), 0) AS output_tokens,
               COALESCE(sum(u.cost_inr), 0) AS cost_inr,
               (
                 SELECT b.lab_booking_id FROM bookings b
                 WHERE b.conversation_id = c.id AND b.status = 'confirmed'
                 ORDER BY b.created_at DESC
                 LIMIT 1
               ) AS booking_id
        FROM conversations c
        LEFT JOIN llm_usage u ON u.conversation_id = c.id
        GROUP BY c.id
        ORDER BY c.updated_at DESC
        LIMIT $1 OFFSET $2
        """,
        limit,
        offset,
    )
    return [
        {
            "id": str(row["id"]),
            "phone": mask_phone(row["phone"]),
            "state": row["state"],
            "input_tokens": int(row["input_tokens"]),
            "output_tokens": int(row["output_tokens"]),
            "cost_inr": float(row["cost_inr"]),
            "booking_id": row["booking_id"],
        }
        for row in rows
    ]
