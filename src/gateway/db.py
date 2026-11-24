import json
from pathlib import Path

import asyncpg

from gateway.settings import settings

_LOCK = 8675309


def migrations_dir() -> Path:
    here = Path(__file__).resolve()
    candidate = here.parents[2] / "migrations"
    if candidate.exists():
        return candidate
    return Path.cwd() / "migrations"


async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


async def migrate(dsn: str | None = None) -> None:
    conn = await asyncpg.connect(dsn or settings.database_url)
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version text PRIMARY KEY,
              applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute("SELECT pg_advisory_lock($1)", _LOCK)
        try:
            applied = {row["version"] for row in await conn.fetch("SELECT version FROM schema_migrations")}
            for path in sorted(migrations_dir().glob("*.sql")):
                if path.name in applied:
                    continue
                async with conn.transaction():
                    await conn.execute(path.read_text())
                    await conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES ($1)",
                        path.name,
                    )
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _LOCK)
    finally:
        await conn.close()


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=settings.db_pool_max,
        init=_init_conn,
        command_timeout=30,
    )


async def reset_database(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
              outbox,
              handoffs,
              llm_usage,
              lab_calls,
              jobs,
              bookings,
              inbound_messages,
              rate_limits,
              conversations
            RESTART IDENTITY CASCADE
            """
        )
        await conn.execute("TRUNCATE lab.bookings, lab.call_log, lab.slots, ingress_events")
        await conn.execute(
            """
            UPDATE llm_budget
            SET spent_inr = 0,
                calls = 0,
                cap_inr = $1,
                max_calls = $2
            WHERE id = 1
            """,
            settings.max_spend_inr,
            settings.max_llm_calls,
        )
