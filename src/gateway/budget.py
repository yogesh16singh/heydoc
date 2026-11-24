import asyncpg


async def budget_allows(conn: asyncpg.Connection, delta_inr: float) -> bool:
    row = await conn.fetchrow(
        "SELECT spent_inr, calls, cap_inr, max_calls FROM llm_budget WHERE id = 1"
    )
    if row is None:
        return False
    return float(row["spent_inr"]) + delta_inr <= float(row["cap_inr"]) and row["calls"] + 1 <= row["max_calls"]


async def commit_spend(conn: asyncpg.Connection, delta_inr: float) -> bool:
    row = await conn.fetchrow(
        """
        UPDATE llm_budget
        SET spent_inr = spent_inr + $1,
            calls = calls + 1
        WHERE id = 1
          AND spent_inr + $1 <= cap_inr
          AND calls + 1 <= max_calls
        RETURNING spent_inr, calls
        """,
        delta_inr,
    )
    return row is not None
