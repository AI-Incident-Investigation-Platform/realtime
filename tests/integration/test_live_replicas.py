"""Exercise replica-owned queues and connection leases against real infrastructure."""

import asyncio
import json
import time
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import httpx
import jwt
import pytest
from aiip_kit.access.checker import AclCacheWriter, DragonflyAccessChecker
from aiip_kit.access.model import AclSnapshot
from aiormq.exceptions import ChannelNotFoundEntity
from cryptography.hazmat.primitives.asymmetric import rsa
from redis.asyncio import Redis
from testcontainers.core.container import DockerContainer

from src.broker import LiveBroker
from src.hub import Hub, Principal
from src.main import create_app
from src.security import ConnectionLeases, JwtAuthenticator, ProjectAccess


class NoHistory:
    async def project(self, investigation_id: UUID, token: str) -> UUID:
        raise AssertionError("investigation history was not requested")

    async def events_after(
        self, investigation_id: UUID, after_sequence: int, token: str
    ) -> list[dict[str, Any]]:
        raise AssertionError("investigation history was not requested")


class NoAclFallback:
    async def fetch(self, project_id: UUID, user_id: UUID) -> Any:
        raise AssertionError("fresh Dragonfly ACL should satisfy this check")


async def wait_for(predicate: Any, timeout: float = 5) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("expected broker delivery did not happen")
        await asyncio.sleep(0.05)


async def rabbit_connection(url: str) -> aio_pika.abc.AbstractConnection:
    deadline = asyncio.get_running_loop().time() + 30
    while True:
        try:
            return await aio_pika.connect(url, timeout=2)
        except OSError, aio_pika.AMQPException:
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.25)


async def queue_gone(publisher: aio_pika.abc.AbstractConnection, name: str) -> bool:
    probe = await publisher.channel()
    try:
        await probe.declare_queue(name, passive=True)
    except ChannelNotFoundEntity:
        return True
    finally:
        if not probe.is_closed:
            await probe.close()
    return False


class AsgiSocket:
    def __init__(self, app: Any, *, origin: str, token: str | None) -> None:
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        headers = [(b"origin", origin.encode())]
        if token is not None:
            headers.append((b"cookie", f"access_token={token}".encode()))
        scope = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "scheme": "ws",
            "path": "/ws/v1/events",
            "raw_path": b"/ws/v1/events",
            "root_path": "",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "subprotocols": [],
        }
        self.task = asyncio.create_task(app(scope, self.incoming.get, self.outgoing.put))

    async def next(self) -> dict[str, Any]:
        return await asyncio.wait_for(self.outgoing.get(), 5)

    async def connect(self) -> dict[str, Any]:
        await self.incoming.put({"type": "websocket.connect"})
        return await self.next()

    async def send_json(self, payload: dict[str, Any]) -> None:
        await self.incoming.put({"type": "websocket.receive", "text": json.dumps(payload)})

    async def disconnect(self) -> None:
        await self.incoming.put({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(self.task, 5)


async def exercise_asgi_socket(
    redis: Redis,
    broker: LiveBroker,
    hub: Hub,
    exchange: aio_pika.abc.AbstractExchange,
    user_id: UUID,
    project_id: UUID,
) -> None:
    session_id = uuid4()
    await redis.set(f"auth:session:{session_id}", "active", ex=60)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwk["kid"] = "integration"
    token = jwt.encode(
        {
            "iss": "auth",
            "aud": "aiip",
            "sub": str(user_id),
            "sid": str(session_id),
            "iat": int(time.time()),
            "exp": int(time.time()) + 60,
        },
        key,
        algorithm="RS256",
        headers={"kid": "integration"},
    )

    def jwks(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/jwks"
        return httpx.Response(200, json={"keys": [jwk]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(jwks)) as http:
        app = create_app()
        app.state.runtime = (
            type("Settings", (), {"allowed_origins": ("http://testserver",)})(),
            redis,
            http,
            broker,
            hub,
            JwtAuthenticator(http, redis, "http://auth/jwks", "auth", "aiip"),
            ConnectionLeases(redis),
        )
        denied_origin = AsgiSocket(app, origin="http://evil.test", token=token)
        assert await denied_origin.connect() == {
            "type": "websocket.close",
            "code": 4403,
            "reason": "",
        }
        await denied_origin.task
        denied_cookie = AsgiSocket(app, origin="http://testserver", token=None)
        assert await denied_cookie.connect() == {
            "type": "websocket.close",
            "code": 4401,
            "reason": "",
        }
        await denied_cookie.task

        for sequence in (1, 2):
            socket = AsgiSocket(app, origin="http://testserver", token=token)
            assert (await socket.connect())["type"] == "websocket.accept"
            await socket.send_json(
                {
                    "action": "subscribe",
                    "topic": "project",
                    "subscription_id": "p",
                    "project_id": str(project_id),
                }
            )
            ack = await socket.next()
            assert ack["type"] == "websocket.send"
            assert json.loads(ack["text"]) == {"type": "subscription.ack", "subscription_id": "p"}
            event = {"type": "incident.updated", "project_id": str(project_id), "version": sequence}
            await exchange.publish(
                aio_pika.Message(json.dumps({"data": event}).encode()),
                routing_key=f"project.{project_id}",
            )
            message = await socket.next()
            assert message["type"] == "websocket.send"
            assert json.loads(message["text"]) == event
            await socket.disconnect()
        assert await redis.zcard(f"realtime:session:{session_id}") == 0


@pytest.mark.asyncio
@pytest.mark.case(id="RT-015")
async def test_two_replicas_route_only_local_subscriptions_and_dragonfly_leases():
    with (
        DockerContainer("rabbitmq:4-management-alpine")
        .with_env("RABBITMQ_DEFAULT_USER", "realtime_test")
        .with_env("RABBITMQ_DEFAULT_PASS", "realtime_test")
        .with_exposed_ports(5672) as rabbit,
        DockerContainer("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2")
        .with_command(["dragonfly", "--logtostderr", "--proactor_threads=2", "--maxmemory=768mb"])
        .with_exposed_ports(6379) as dragonfly,
    ):
        rabbit_url = (
            f"amqp://realtime_test:realtime_test@{rabbit.get_container_host_ip()}:"
            f"{rabbit.get_exposed_port(5672)}/"
        )
        redis_url = (
            f"redis://{dragonfly.get_container_host_ip()}:{dragonfly.get_exposed_port(6379)}/0"
        )
        redis = Redis.from_url(redis_url, decode_responses=True)
        publisher = await rabbit_connection(rabbit_url)
        broker_a, broker_b = LiveBroker(rabbit_url), LiveBroker(rabbit_url)
        queue_names: list[str] = []
        try:
            channel = await publisher.channel()
            exchanges = {
                name: await channel.declare_exchange(
                    name, aio_pika.ExchangeType.TOPIC, durable=True
                )
                for name in LiveBroker.exchanges
            }
            drop_queue = await channel.declare_queue(
                "",
                exclusive=True,
                auto_delete=True,
                arguments={"x-max-length": 1, "x-overflow": "drop-head"},
            )
            await drop_queue.bind(exchanges["incidents.live"], routing_key="drop-head-proof")
            for body in (b"old", b"new"):
                await exchanges["incidents.live"].publish(
                    aio_pika.Message(body), routing_key="drop-head-proof"
                )
            surviving = await drop_queue.get(timeout=2)
            assert surviving.body == b"new"
            assert not surviving.headers
            await surviving.ack()
            assert await drop_queue.get(fail=False, timeout=2) is None
            deadline = asyncio.get_running_loop().time() + 20
            while True:
                try:
                    if await redis.ping():
                        break
                except ConnectionError:
                    pass
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("Dragonfly did not become ready")
                await asyncio.sleep(0.25)

            user_id, session_id, project_a, project_b = (uuid4() for _ in range(4))
            await redis.set(f"auth:session:{session_id}", "active", ex=60)
            now_ms = time.time_ns() // 1_000_000
            writer = AclCacheWriter(redis)
            for project_id in (project_a, project_b):
                await writer.store(
                    AclSnapshot(
                        project_id=project_id,
                        user_id=user_id,
                        project_state="active",
                        lifecycle_version=1,
                        acl_version=1,
                        member_role_key="viewer",
                        member_version=1,
                        role_key="viewer",
                        role_version=1,
                        permissions=frozenset({"project.read"}),
                        observed_at_ms=now_ms,
                        fresh_until_ms=now_ms + 30_000,
                    )
                )
            access = ProjectAccess(DragonflyAccessChecker(redis, NoAclFallback()), redis)
            principal = Principal(user_id, session_id, "unused", int(time.time()) + 60)
            hub_a = Hub(broker_a, access, NoHistory(), unbind_delay=0)
            hub_b = Hub(broker_b, access, NoHistory(), unbind_delay=0)
            await broker_a.start(hub_a.deliver, hub_a.broker_lost, hub_a.rebind)
            await broker_b.start(hub_b.deliver, hub_b.broker_lost, hub_b.rebind)
            assert broker_a._queue is not None and broker_b._queue is not None
            assert broker_a._queue.name != broker_b._queue.name
            queue_names = [broker_a._queue.name, broker_b._queue.name]

            client_a = hub_a.add(principal)
            client_b1, client_b2 = hub_b.add(principal), hub_b.add(principal)
            for client, hub, project_id in (
                (client_a, hub_a, project_a),
                (client_b1, hub_b, project_a),
                (client_b2, hub_b, project_b),
            ):
                await hub.subscribe(
                    client,
                    {
                        "topic": "project",
                        "action": "subscribe",
                        "subscription_id": "p",
                        "project_id": str(project_id),
                    },
                )
                assert (await client.outgoing.get())["type"] == "subscription.ack"

            event_a = {"type": "incident.created", "project_id": str(project_a)}
            await exchanges["incidents.live"].publish(
                aio_pika.Message(json.dumps({"data": event_a}).encode()),
                routing_key=f"project.{project_a}",
            )
            assert await asyncio.wait_for(client_a.outgoing.get(), 5) == event_a
            assert await asyncio.wait_for(client_b1.outgoing.get(), 5) == event_a
            assert client_b2.outgoing.empty()

            event_b = {"type": "incident.updated", "project_id": str(project_b)}
            await exchanges["incidents.live"].publish(
                aio_pika.Message(json.dumps({"data": event_b}).encode()),
                routing_key=f"project.{project_b}",
            )
            assert await asyncio.wait_for(client_b2.outgoing.get(), 5) == event_b
            assert client_a.outgoing.empty() and client_b1.outgoing.empty()

            leases = ConnectionLeases(redis, ttl_seconds=45, limit=2)
            first, second = await leases.acquire(session_id), await leases.acquire(session_id)
            assert first is not None and second is not None
            assert await leases.acquire(session_id) is None
            await redis.zadd(f"realtime:session:{session_id}", {first: time.time() - 1})
            assert not await leases.renew(session_id, first)
            assert await leases.renew(session_id, second)
            assert await leases.acquire(session_id) is not None

            await exchanges["auth.live"].publish(
                aio_pika.Message(json.dumps({"data": {"session_id": str(session_id)}}).encode()),
                routing_key="control.session.revoked",
            )
            await wait_for(
                lambda: all(c.close_code == 4403 for c in (client_a, client_b1, client_b2))
            )
            await exercise_asgi_socket(
                redis, broker_a, hub_a, exchanges["incidents.live"], user_id, project_a
            )
        finally:
            await broker_a.close()
            await broker_b.close()
            for name in queue_names:
                deadline = asyncio.get_running_loop().time() + 5
                while not await queue_gone(publisher, name):
                    if asyncio.get_running_loop().time() >= deadline:
                        raise AssertionError(f"exclusive queue {name} survived owner shutdown")
                    await asyncio.sleep(0.05)
            await publisher.close()
            await redis.aclose()
