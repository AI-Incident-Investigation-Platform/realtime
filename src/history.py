"""Read-only catch-up from the incidents owner using the user's access token."""

from typing import Any
from uuid import UUID

import httpx


class HttpHistory:
    def __init__(self, http: httpx.AsyncClient, base_url: str, timeout: float = 3.0) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    async def project(self, investigation_id: UUID, token: str) -> UUID:
        response = await self._http.get(
            f"{self._base_url}/api/v1/investigations/{investigation_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=self._timeout,
        )
        response.raise_for_status()
        return UUID(response.json()["project_id"])

    async def events_after(
        self, investigation_id: UUID, after_sequence: int, token: str
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        sequence = after_sequence
        while True:
            response = await self._http.get(
                f"{self._base_url}/api/v1/investigations/{investigation_id}/events",
                params={"after_sequence": sequence, "limit": 200},
                headers={"Authorization": f"Bearer {token}"},
                timeout=self._timeout,
            )
            response.raise_for_status()
            body = response.json()
            page = body["items"]
            events.extend(page)
            if not body.get("next_cursor"):
                return events
            next_sequence = max((int(item["sequence"]) for item in page), default=sequence)
            if next_sequence <= sequence:
                raise ValueError("history page made no progress")
            sequence = next_sequence
