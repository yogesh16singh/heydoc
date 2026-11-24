import os

os.environ["DATABASE_URL"] = "postgresql://heydoc:heydoc@localhost:5432/heydoc"
os.environ["LAB_BASE_URL"] = "http://127.0.0.1:8001"
os.environ["LAB_CLIENT_CHAOS"] = "none"
os.environ["APP_SECRET"] = "test-secret"
os.environ["VERIFY_TOKEN"] = "test-verify"
os.environ["DEV_MODE"] = "true"
os.environ["LLM_PROVIDER"] = "mock"
os.environ["MAX_SPEND_INR"] = "500"

import pytest

from gateway.db import create_pool, migrate, reset_database


@pytest.fixture
async def pool():
    try:
        await migrate()
        created = await create_pool()
    except Exception as exc:
        pytest.fail(f"Postgres is not reachable at localhost:5432 ({exc})")
    await reset_database(created)
    yield created
    await created.close()


@pytest.fixture
def http():
    from fastapi.testclient import TestClient

    from gateway.api import app

    with TestClient(app) as client:
        response = client.post("/dev/reset")
        assert response.status_code == 200
        yield client
