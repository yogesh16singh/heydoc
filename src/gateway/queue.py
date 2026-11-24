import uuid

import asyncpg

from gateway.settings import settings


async def claim_job(pool: asyncpg.Pool) -> dict | None:
    """Claim the oldest pending job of a conversation that is not already running.

    Two loops can observe the same conversation. The partial unique index on
    running jobs rejects the second update, and that transaction rolls back.
    """
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        locked_until = NULL,
                        claim_token = NULL,
                        updated_at = now(),
                        last_error = 'lease expired'
                    WHERE status = 'running'
                      AND locked_until IS NOT NULL
                      AND locked_until < now()
                    """
                )
                row = await conn.fetchrow(
                    """
                    SELECT j.id, j.message_id, j.conversation_id, j.attempts
                    FROM jobs j
                    WHERE j.status = 'pending'
                      AND j.next_run_at <= now()
                      AND NOT EXISTS (
                        SELECT 1 FROM jobs r
                        WHERE r.conversation_id = j.conversation_id
                          AND r.status = 'running'
                          AND r.locked_until > now()
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM jobs earlier
                        WHERE earlier.conversation_id = j.conversation_id
                          AND earlier.status = 'pending'
                          AND earlier.next_run_at <= now()
                          AND earlier.id < j.id
                      )
                    ORDER BY j.id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                if row is None:
                    return None
                token = uuid.uuid4()
                claimed = await conn.fetchrow(
                    """
                    UPDATE jobs
                    SET status = 'running',
                        locked_until = now() + make_interval(secs => $2),
                        attempts = attempts + 1,
                        claim_token = $3,
                        updated_at = now()
                    WHERE id = $1
                    RETURNING id, message_id, conversation_id, attempts, claim_token
                    """,
                    row["id"],
                    settings.lease_seconds,
                    token,
                )
                return dict(claimed)
        except asyncpg.UniqueViolationError:
            return None


async def heartbeat(pool: asyncpg.Pool, job_id: int, token: uuid.UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
            SET locked_until = now() + make_interval(secs => $3)
            WHERE id = $1 AND claim_token = $2 AND status = 'running'
            """,
            job_id,
            token,
            settings.lease_seconds,
        )


async def complete_job(conn: asyncpg.Connection, job_id: int, token: uuid.UUID) -> bool:
    row = await conn.fetchval(
        """
        UPDATE jobs
        SET status = 'done', locked_until = NULL, updated_at = now()
        WHERE id = $1 AND claim_token = $2 AND status = 'running'
        RETURNING id
        """,
        job_id,
        token,
    )
    return row is not None


async def release_claim(pool: asyncpg.Pool, job_id: int, token: uuid.UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
            SET status = 'pending',
                locked_until = NULL,
                claim_token = NULL,
                updated_at = now()
            WHERE id = $1 AND claim_token = $2 AND status = 'running'
            """,
            job_id,
            token,
        )


async def fail_job(pool: asyncpg.Pool, job: dict, error: str) -> str | None:
    delay = min(30, 2 ** int(job["attempts"]))
    async with pool.acquire() as conn:
        status = await conn.fetchval(
            """
            UPDATE jobs
            SET status = CASE WHEN attempts >= $3 THEN 'dead' ELSE 'pending' END,
                next_run_at = now() + make_interval(secs => $4),
                locked_until = NULL,
                claim_token = NULL,
                last_error = $5,
                updated_at = now()
            WHERE id = $1 AND claim_token = $2 AND status = 'running'
            RETURNING status
            """,
            job["id"],
            job["claim_token"],
            settings.max_job_attempts,
            delay,
            error[:500],
        )
        return status
