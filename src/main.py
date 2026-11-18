"""FastAPI entrypoint for the realtime replica."""

import logging
from contextlib import asynccontextmanager

import httpx
from aiip_kit.access.checker import DragonflyAccessChecker
from aiip_kit.access.source import HttpAclSource
from aiip_kit.service_auth.provider import AuthServiceTokenProvider
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from src.broker import LiveBroker
from src.config import Settings
from src.history import HttpHistory
from src.hub import Hub
from src.security import ConnectionLeases, JwtAuthenticator, ProjectAccess
from src.socket import serve_socket

log = logging.getLogger(__name__)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = Settings.load()
        redis = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=2)
        http = httpx.AsyncClient(timeout=3)
        broker = LiveBroker(settings.rabbitmq_url)
        token_provider = AuthServiceTokenProvider(
            http, settings.auth_url, settings.client_id, settings.client_secret
        )
        acl_source = HttpAclSource(
            http,
            settings.projects_url,
            lambda: token_provider.token("projects", ["projects.acl.read"]),
        )
        checker = DragonflyAccessChecker(redis, acl_source)
        access = ProjectAccess(checker, redis)
        hub = Hub(broker, access, HttpHistory(http, settings.incidents_url))
        auth = JwtAuthenticator(http, redis, settings.jwks_url, settings.issuer, settings.audience)
        leases = ConnectionLeases(redis)
        app.state.runtime = (settings, redis, http, broker, hub, auth, leases)
        try:
            await broker.start(hub.deliver, hub.broker_lost, hub.rebind)
            yield
        finally:
            for client in tuple(hub.clients):
                client.close_code = 1012
            await broker.close()
            await http.aclose()
            await redis.aclose()

    app = FastAPI(title="AIIP realtime", lifespan=lifespan)

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> Response:
        runtime = getattr(app.state, "runtime", None)
        if runtime is None:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        _, redis, _, broker, _, _, _ = runtime
        try:
            if broker.ready and await redis.ping():
                return JSONResponse({"status": "ok"})
        except ConnectionError:
            pass
        return JSONResponse({"status": "unavailable"}, status_code=503)

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/ws/v1/events")
    async def events(websocket: WebSocket) -> None:
        settings, _, _, _, hub, auth, leases = app.state.runtime
        await serve_socket(websocket, hub, auth, leases, set(settings.allowed_origins))

    return app


app = create_app()
