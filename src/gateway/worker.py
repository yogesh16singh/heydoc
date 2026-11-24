import asyncio
import contextlib
import logging
import signal

from gateway.agent import run_agent
from gateway.db import create_pool, migrate
from gateway.queue import claim_job, complete_job, fail_job, heartbeat, release_claim
from gateway.reconcile import reconcile_once
from gateway.settings import settings

log = logging.getLogger("gateway.worker")


async def process_job(pool, job: dict) -> None:
    beat_stop = asyncio.Event()

    async def beats() -> None:
        while not beat_stop.is_set():
            try:
                await asyncio.wait_for(beat_stop.wait(), timeout=15)
            except TimeoutError:
                await heartbeat(pool, job["id"], job["claim_token"])

    beater = asyncio.create_task(beats())
    try:
        async with pool.acquire() as conn:
            message = await conn.fetchrow("SELECT * FROM inbound_messages WHERE id = $1", job["message_id"])
            row = await conn.fetchrow("SELECT * FROM conversations WHERE id = $1", job["conversation_id"])
            conv = {
                "id": row["id"],
                "phone": row["phone"],
                "state": row["state"],
                "memory": row["memory"] or {},
            }
            reply = await run_agent(conn, conv, message["body"] or "")
            await conn.execute(
                """
                INSERT INTO outbox (conversation_id, phone, body, message_id, status)
                VALUES ($1, $2, $3, $4, 'sent')
                ON CONFLICT (message_id) DO NOTHING
                """,
                conv["id"],
                conv["phone"],
                reply,
                message["id"],
            )
            done = await complete_job(conn, job["id"], job["claim_token"])
            if not done:
                log.warning("job %s lost its claim before completion", job["id"])
            else:
                log.info("done job %s phone %s", job["id"], conv["phone"])
    except asyncio.CancelledError:
        await release_claim(pool, job["id"], job["claim_token"])
        raise
    except Exception as exc:
        log.exception("job %s failed", job["id"])
        status = await fail_job(pool, job, str(exc))
        if status == "dead":
            await _dead_letter(pool, job, str(exc))
    finally:
        beat_stop.set()
        beater.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beater


async def _dead_letter(pool, job: dict, error: str) -> None:
    async with pool.acquire() as conn:
        conv = await conn.fetchrow("SELECT * FROM conversations WHERE id = $1", job["conversation_id"])
        if conv["state"] != "handed_off":
            await conn.execute(
                "INSERT INTO handoffs (conversation_id, reason, summary) VALUES ($1, $2, $3)",
                conv["id"],
                "job_failed",
                error[:500],
            )
            await conn.execute(
                """
                UPDATE conversations
                SET state = 'handed_off', updated_at = now()
                WHERE id = $1
                """,
                conv["id"],
            )
        await conn.execute(
            """
            INSERT INTO outbox (conversation_id, phone, body, status)
            VALUES ($1, $2, $3, 'sent')
            """,
            conv["id"],
            conv["phone"],
            "Something went wrong on our side. A teammate will follow up.",
        )


async def worker_loop(pool, wake: asyncio.Event, stop: asyncio.Event, index: int) -> None:
    while not stop.is_set():
        wake.clear()
        try:
            job = await claim_job(pool)
        except Exception:
            log.exception("claim failed")
            await asyncio.sleep(0.5)
            continue
        if job is None:
            try:
                await asyncio.wait_for(wake.wait(), timeout=settings.poll_ms / 1000)
            except TimeoutError:
                pass
            continue
        await process_job(pool, job)
        log.debug("loop %s finished job %s", index, job["id"])


async def listener(pool, wake: asyncio.Event, stop: asyncio.Event) -> None:
    async with pool.acquire() as conn:
        def _notify(*_args) -> None:
            wake.set()

        await conn.add_listener("jobs", _notify)
        try:
            while not stop.is_set():
                await asyncio.sleep(0.5)
        finally:
            await conn.remove_listener("jobs", _notify)


async def reconcile_loop(pool, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            changed = await reconcile_once(pool)
            if changed:
                log.info("reconciled %s booking(s)", changed)
        except Exception:
            log.exception("reconciler failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.reconcile_interval_s)
        except TimeoutError:
            pass


async def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    await migrate()
    pool = await create_pool()
    wake = asyncio.Event()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    tasks = [
        asyncio.create_task(worker_loop(pool, wake, stop, index))
        for index in range(settings.worker_concurrency)
    ]
    tasks.append(asyncio.create_task(listener(pool, wake, stop)))
    tasks.append(asyncio.create_task(reconcile_loop(pool, stop)))
    log.info("worker up concurrency=%s", settings.worker_concurrency)
    await stop.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE jobs
            SET status = 'pending', locked_until = NULL, claim_token = NULL, updated_at = now()
            WHERE status = 'running'
            """
        )
    await pool.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
