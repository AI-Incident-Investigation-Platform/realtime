import httpx
import pytest

from src.main import create_app


@pytest.mark.asyncio
@pytest.mark.case(id="RT-012")
async def test_liveness_endpoint_needs_no_external_services():
    app = create_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        response = await http.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
