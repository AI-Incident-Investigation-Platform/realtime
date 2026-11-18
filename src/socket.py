"""WebSocket frame boundary and connection lifecycle."""

import asyncio
import contextlib
import json
import logging
import random
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from src.hub import Hub
from src.security import ConnectionLeases, JwtAuthenticator

log = logging.getLogger(__name__)
MAX_FRAME_BYTES = 4096


def parse_command(frame: bytes) -> dict[str, Any]:
    if len(frame) > MAX_FRAME_BYTES:
        raise ValueError("frame too large")
    command = json.loads(frame)
    if not isinstance(command, dict):
        raise ValueError("command must be an object")
    action = command.get("action")
    topic = command.get("topic")
    required: set[str]
    if action == "unsubscribe":
        required = {"action", "subscription_id"}
    elif action == "subscribe" and topic == "investigation":
        required = {"action", "topic", "subscription_id", "investigation_id", "after_sequence"}
        UUID(command.get("investigation_id", ""))
        if type(command.get("after_sequence")) is not int or command["after_sequence"] < 0:
            raise ValueError("invalid after_sequence")
    elif action == "subscribe" and topic == "project":
        required = {"action", "topic", "subscription_id", "project_id"}
        UUID(command.get("project_id", ""))
    elif action == "subscribe" and topic == "inbox":
        required = {"action", "topic", "subscription_id"}
    else:
        raise ValueError("unknown action or topic")
    if set(command) != required:
        raise ValueError("unknown or missing fields")
    subscription_id = command["subscription_id"]
    if not isinstance(subscription_id, str) or not 1 <= len(subscription_id) <= 64:
        raise ValueError("invalid subscription_id")
    return command


async def serve_socket(
    websocket: WebSocket,
    hub: Hub,
    auth: JwtAuthenticator,
    leases: ConnectionLeases,
    allowed_origins: set[str],
) -> None:
    if websocket.headers.get("origin") not in allowed_origins:
        await websocket.close(code=4403)
        return
    try:
        principal = await auth.authenticate(websocket.cookies.get("access_token"))
        connection_id = await leases.acquire(principal.session_id)
    except PermissionError:
        await websocket.close(code=4401)
        return
    if connection_id is None:
        await websocket.close(code=4403)
        return
    client = hub.add(principal)
    await websocket.accept()

    async def sender() -> None:
        while client.close_code is None:
            try:
                event = await asyncio.wait_for(client.outgoing.get(), timeout=1)
            except TimeoutError:
                continue
            await websocket.send_json(event)
        while not client.outgoing.empty():
            await websocket.send_json(client.outgoing.get_nowait())

    async def receiver() -> None:
        while client.close_code is None:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return
            body = message.get("bytes")
            if body is None:
                body = (message.get("text") or "").encode()
            command: dict[str, Any] = {}
            try:
                command = parse_command(body)
                if command["action"] == "subscribe":
                    await hub.subscribe(client, command)
                else:
                    await hub.unsubscribe(client, command["subscription_id"])
            except (
                ValueError,
                KeyError,
                TypeError,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as exc:
                if "frame too large" in str(exc):
                    client.close_code = 1009
                    return
                await hub._offer(
                    client,
                    {
                        "type": "subscription.error",
                        "subscription_id": command.get("subscription_id", ""),
                        "error": {
                            "code": "invalid_request",
                            "message": "Invalid subscription command",
                        },
                    },
                )
            except PermissionError:
                await hub._offer(
                    client,
                    {
                        "type": "subscription.error",
                        "subscription_id": command["subscription_id"],
                        "error": {"code": "forbidden", "message": "Access denied"},
                    },
                )
            except httpx.HTTPStatusError as exc:
                await hub._offer(
                    client,
                    {
                        "type": "subscription.error",
                        "subscription_id": command["subscription_id"],
                        "error": {
                            "code": "not_found"
                            if exc.response.status_code == 404
                            else "unavailable",
                            "message": "Subscription unavailable",
                        },
                    },
                )
            except httpx.RequestError:
                await hub._offer(
                    client,
                    {
                        "type": "subscription.error",
                        "subscription_id": command["subscription_id"],
                        "error": {"code": "unavailable", "message": "Subscription unavailable"},
                    },
                )

    async def maintenance() -> None:
        max_age = random.uniform(1800, 3600)
        started = asyncio.get_running_loop().time()
        checks = 0
        while client.close_code is None:
            await asyncio.sleep(5)
            if client.close_code is not None:
                return
            if asyncio.get_running_loop().time() - started >= max_age:
                client.close_code = 1012
            elif not await leases.renew(principal.session_id, connection_id):
                client.close_code = 4403
            else:
                await hub.check_permissions(client)
                checks += 1
                if checks % 3 == 0:
                    await hub._offer(
                        client, {"type": "heartbeat", "occurred_at": datetime.now(UTC).isoformat()}
                    )

    tasks = [asyncio.create_task(coro()) for coro in (sender, receiver, maintenance)]
    try:
        while client.close_code is None and not any(task.done() for task in tasks):
            await asyncio.sleep(0.2)
        for task in tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                log.exception("WebSocket task failed", exc_info=task.exception())
                client.close_code = 1012
        if client.close_code is not None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(tasks[0], timeout=2)
            with contextlib.suppress(WebSocketDisconnect, RuntimeError):
                await websocket.close(code=client.close_code)
    except WebSocketDisconnect:
        pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await hub.remove(client)
        await leases.release(principal.session_id, connection_id)
