import httpx
import pytest
from httpx import ASGITransport
from prefect.server.api.server import create_app
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    """
    Sets up test harness for temporary DB during test runs.
    """
    try:
        with prefect_test_harness():
            yield
    except OSError as e:
        if "Directory not empty" in str(e):
            pass
        else:
            raise e


@pytest.fixture()
def app():
    return create_app(ephemeral=True)


@pytest.fixture
async def client(app):
    """
    Yield a test client for testing the api
    """
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="https://test/api"
    ) as async_client:
        yield async_client
