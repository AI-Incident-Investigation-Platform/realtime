from uuid import uuid4

import httpx
import pytest

from src.history import HttpHistory


@pytest.mark.asyncio
@pytest.mark.case(id="RT-006")
async def test_history_uses_user_token_for_lookup_and_all_replay_pages():
    investigation_id = uuid4()
    project_id = uuid4()
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["authorization"] == "Bearer user-token"
        if request.url.path.endswith("/events"):
            after = int(request.url.params["after_sequence"])
            items = [{"sequence": after + 1, "type": "investigation.progress"}]
            return httpx.Response(
                200, json={"items": items, "next_cursor": "next" if after == 0 else None}
            )
        return httpx.Response(200, json={"project_id": str(project_id)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        history = HttpHistory(http, "http://incidents")
        assert await history.project(investigation_id, "user-token") == project_id
        assert [
            item["sequence"]
            for item in await history.events_after(investigation_id, 0, "user-token")
        ] == [1, 2]
    assert len(calls) == 3
