"""A fresh exclusive RabbitMQ queue for each running replica."""

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, cast

import aio_pika
from aio_pika.abc import (
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
)
from aiormq.abc import FieldTable

log = logging.getLogger(__name__)


class LiveBroker:
    exchanges = (
        "incidents.live",
        "content.live",
        "notifications.live",
        "auth.live",
        "projects.live",
    )
    queue_arguments = {
        "x-max-length": 1000,
        "x-max-length-bytes": 4 * 1024 * 1024,
        "x-message-ttl": 30_000,
        "x-overflow": "drop-head",
    }

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: AbstractConnection | None = None
        self._queue: AbstractQueue | None = None
        self._exchanges: dict[str, AbstractExchange] = {}
        self._ready = asyncio.Event()
        self._watch: asyncio.Task[None] | None = None
        self._deliver: Callable[[str, str, dict[str, Any]], Awaitable[None]] | None = None
        self._on_loss: Callable[[], Awaitable[None]] | None = None
        self._on_restore: Callable[[], Awaitable[None]] | None = None

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    async def start(
        self,
        deliver: Callable[[str, str, dict[str, Any]], Awaitable[None]],
        on_loss: Callable[[], Awaitable[None]],
        on_restore: Callable[[], Awaitable[None]],
    ) -> None:
        self._deliver, self._on_loss, self._on_restore = deliver, on_loss, on_restore
        await self._connect()
        self._watch = asyncio.create_task(self._watch_connection())

    async def _connect(self) -> None:
        self._connection = await aio_pika.connect(self._url, timeout=3)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=8)
        self._exchanges = {
            name: await channel.declare_exchange(name, aio_pika.ExchangeType.TOPIC, passive=True)
            for name in self.exchanges
        }
        self._queue = await channel.declare_queue(
            "",
            durable=False,
            exclusive=True,
            auto_delete=True,
            arguments=cast(FieldTable, self.queue_arguments),
        )
        await self.bind_controls()
        await self._queue.consume(self._on_message, no_ack=False)
        self._ready.set()

    async def bind_controls(self) -> None:
        assert self._queue is not None
        for name in ("auth.live", "projects.live"):
            await self._queue.bind(self._exchanges[name], routing_key="control.#")

    async def bind(self, exchange: str, key: str) -> None:
        await self._ready.wait()
        assert self._queue is not None
        await self._queue.bind(self._exchanges[exchange], routing_key=key)

    async def unbind(self, exchange: str, key: str) -> None:
        await self._ready.wait()
        assert self._queue is not None
        await self._queue.unbind(self._exchanges[exchange], routing_key=key)

    async def _watch_connection(self) -> None:
        while self._connection is not None:
            await self._connection.closed()
            self._ready.clear()
            if self._on_loss:
                await self._on_loss()
            while True:
                try:
                    await self._connect()
                    if self._on_restore:
                        await self._on_restore()
                    break
                except OSError, aio_pika.AMQPException:
                    log.warning("Соединение с RabbitMQ потеряно")
                    await asyncio.sleep(2)

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=False):
            try:
                event = self.payload(message.body)
            except ValueError, TypeError, UnicodeDecodeError:
                log.warning("Получено некорректное live-событие")
                return
            if self._deliver and message.exchange and message.routing_key:
                await self._deliver(message.exchange, message.routing_key, event)

    @staticmethod
    def payload(body: bytes) -> dict[str, Any]:
        payload = json.loads(body)
        event = payload.get("data", payload)
        if not isinstance(event, dict):
            raise ValueError("live payload must be an object")
        return event

    async def close(self) -> None:
        if self._watch:
            self._watch.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watch
        if self._connection:
            await self._connection.close()
