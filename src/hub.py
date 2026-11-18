"""Per-replica socket subscriptions and live event routing."""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True)
class Principal:
    user_id: UUID
    session_id: UUID
    token: str
    expires_at: int


class Broker(Protocol):
    async def bind(self, exchange: str, key: str) -> None: ...
    async def unbind(self, exchange: str, key: str) -> None: ...


class Access(Protocol):
    async def require(self, principal: Principal, project_id: UUID, permission: str) -> None: ...
    async def live(self, principal: Principal) -> bool: ...


class History(Protocol):
    async def project(self, investigation_id: UUID, token: str) -> UUID: ...
    async def events_after(
        self, investigation_id: UUID, after_sequence: int, token: str
    ) -> list[dict[str, Any]]: ...


@dataclass(eq=False)
class Client:
    principal: Principal
    outgoing: asyncio.Queue[dict[str, Any]] = field(default_factory=lambda: asyncio.Queue(64))
    subscriptions: dict[str, Subscription] = field(default_factory=dict)
    close_code: int | None = None


@dataclass(eq=False)
class Subscription:
    client: Client
    subscription_id: str
    topic: str
    project_id: UUID | None
    investigation_id: UUID | None
    bindings: tuple[tuple[str, str], ...]
    catching_up: bool = False
    buffered: list[dict[str, Any]] = field(default_factory=list)
    last_sequence: int = 0


class Hub:
    """One Hub per replica; broker bindings exist while local clients need them."""

    def __init__(
        self, broker: Broker, access: Access, history: History, *, unbind_delay: float = 20.0
    ) -> None:
        self.broker = broker
        self.access = access
        self.history = history
        self.unbind_delay = unbind_delay
        self.clients: set[Client] = set()
        self._bindings: dict[tuple[str, str], int] = {}
        self._pending_unbind: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._binding_lock = asyncio.Lock()

    def add(self, principal: Principal) -> Client:
        client = Client(principal)
        self.clients.add(client)
        return client

    async def remove(self, client: Client) -> None:
        for subscription_id in list(client.subscriptions):
            await self.unsubscribe(client, subscription_id)
        self.clients.discard(client)

    async def subscribe(self, client: Client, command: dict[str, Any]) -> None:
        subscription_id = command.get("subscription_id")
        if not isinstance(subscription_id, str) or not 1 <= len(subscription_id) <= 64:
            raise ValueError("invalid subscription_id")
        if subscription_id in client.subscriptions or len(client.subscriptions) >= 10:
            raise ValueError("subscription limit or duplicate id")
        topic = command.get("topic")
        bindings: tuple[tuple[str, str], ...]
        investigation_id: UUID | None = None
        project_id: UUID | None = None
        if topic == "investigation":
            investigation_id = UUID(command["investigation_id"])
            after = command["after_sequence"]
            if type(after) is not int or after < 0:
                raise ValueError("invalid after_sequence")
            project_id = await self.history.project(investigation_id, client.principal.token)
            await self.access.require(client.principal, project_id, "project.read")
            bindings = (
                ("incidents.live", f"project.{project_id}.investigation.{investigation_id}"),
            )
        elif topic == "project":
            project_id = UUID(command["project_id"])
            await self.access.require(client.principal, project_id, "project.read")
            bindings = tuple(
                (exchange, key)
                for exchange in ("incidents.live", "content.live")
                for key in (f"project.{project_id}", f"project.{project_id}.#")
            )
            after = 0
        elif topic == "inbox":
            bindings = tuple(
                (exchange, f"user.{client.principal.user_id}")
                for exchange in ("notifications.live", "content.live")
            )
            after = 0
        else:
            raise ValueError("invalid topic")
        sub = Subscription(
            client,
            subscription_id,
            topic,
            project_id,
            investigation_id,
            bindings,
            catching_up=topic == "investigation",
            last_sequence=after,
        )
        client.subscriptions[subscription_id] = sub
        bound: list[tuple[str, str]] = []
        try:
            for binding in bindings:
                await self._bind(binding)
                bound.append(binding)
            if investigation_id is not None:
                assert project_id is not None
                events = await self.history.events_after(
                    investigation_id, after, client.principal.token
                )
                if client.close_code is not None or not await self.access.live(client.principal):
                    raise PermissionError("access revoked during replay")
                await self.access.require(client.principal, project_id, "project.read")
                # A live event may arrive during the REST request; sequence resolves overlap.
                merged = sorted((*events, *sub.buffered), key=lambda event: event["sequence"])
                seen: set[str] = set()
                for event in merged:
                    sequence = event["sequence"]
                    if sequence <= sub.last_sequence or str(event.get("event_id")) in seen:
                        continue
                    if sequence != sub.last_sequence + 1:
                        raise ValueError("replay gap")
                    await self._offer(client, event)
                    seen.add(str(event.get("event_id")))
                    sub.last_sequence = sequence
                sub.buffered.clear()
                sub.catching_up = False
            ack: dict[str, Any] = {"type": "subscription.ack", "subscription_id": subscription_id}
            if investigation_id is not None:
                ack.update(investigation_id=str(investigation_id), last_sequence=sub.last_sequence)
            await self._offer(client, ack)
        except BaseException:
            sub.bindings = tuple(bound)
            await self.unsubscribe(client, subscription_id)
            raise

    async def unsubscribe(self, client: Client, subscription_id: str) -> None:
        sub = client.subscriptions.pop(subscription_id, None)
        if sub is None:
            return
        for binding in sub.bindings:
            await self._release(binding)

    async def _bind(self, binding: tuple[str, str]) -> None:
        async with self._binding_lock:
            pending = self._pending_unbind.pop(binding, None)
            if pending:
                pending.cancel()
            count = self._bindings.get(binding, 0)
            if count == 0 and pending is None:
                await self.broker.bind(*binding)
            self._bindings[binding] = count + 1

    async def _release(self, binding: tuple[str, str]) -> None:
        async with self._binding_lock:
            count = self._bindings.get(binding, 0)
            if count <= 1:
                self._bindings.pop(binding, None)
                self._pending_unbind[binding] = asyncio.create_task(self._delayed_unbind(binding))
            else:
                self._bindings[binding] = count - 1

    async def _delayed_unbind(self, binding: tuple[str, str]) -> None:
        try:
            await asyncio.sleep(self.unbind_delay)
            async with self._binding_lock:
                if binding not in self._bindings:
                    await self.broker.unbind(*binding)
                self._pending_unbind.pop(binding, None)
        except asyncio.CancelledError:
            pass

    async def deliver(self, exchange: str, key: str, event: dict[str, Any]) -> None:
        if key.startswith("control."):
            await self._control(key, event)
            return
        for client in tuple(self.clients):
            if client.close_code is not None:
                continue
            for sub in tuple(client.subscriptions.values()):
                if not any(
                    name == exchange and self._matches(pattern, key)
                    for name, pattern in sub.bindings
                ):
                    continue
                if not await self.access.live(client.principal):
                    client.close_code = 4403
                    break
                if sub.project_id is not None:
                    try:
                        await self.access.require(client.principal, sub.project_id, "project.read")
                    except PermissionError:
                        client.close_code = 4403
                        break
                if sub.catching_up:
                    if len(sub.buffered) >= 64:
                        await self._offer(client, {"type": "resync_required", "reason": "overflow"})
                        client.close_code = 1012
                    else:
                        sub.buffered.append(event)
                elif sub.investigation_id is not None:
                    seq = event.get("sequence")
                    if type(seq) is not int or seq <= sub.last_sequence:
                        continue
                    if seq != sub.last_sequence + 1:
                        await self._offer(client, {"type": "resync_required", "reason": "overflow"})
                        client.close_code = 1012
                    else:
                        sub.last_sequence = seq
                        await self._offer(client, event)
                else:
                    await self._offer(client, event)

    async def _control(self, key: str, event: dict[str, Any]) -> None:
        data = event.get("data", event)
        for client in tuple(self.clients):
            if key == "control.session.revoked" and str(client.principal.session_id) == str(
                data.get("session_id")
            ):
                client.close_code = 4403
            elif key in ("control.member.removed", "control.role.changed"):
                project_id = str(data.get("project_id"))
                user_id = str(data.get("user_id"))
                if any(
                    str(sub.project_id) == project_id for sub in client.subscriptions.values()
                ) and user_id == str(client.principal.user_id):
                    client.close_code = 4403
            elif key == "control.project.deleting":
                project_id = str(data.get("project_id"))
                if any(str(sub.project_id) == project_id for sub in client.subscriptions.values()):
                    client.close_code = 4403

    async def broker_lost(self) -> None:
        for client in tuple(self.clients):
            await self._offer(client, {"type": "resync_required", "reason": "broker_lost"})

    async def rebind(self) -> None:
        for binding in tuple(self._bindings):
            await self.broker.bind(*binding)

    async def check_permissions(self, target: Client | None = None) -> None:
        for client in (target,) if target is not None else tuple(self.clients):
            if client.close_code is not None:
                continue
            if client.principal.expires_at <= time.time():
                client.close_code = 4401
            elif not await self.access.live(client.principal):
                client.close_code = 4403
            else:
                for sub in client.subscriptions.values():
                    if sub.project_id is not None:
                        try:
                            await self.access.require(
                                client.principal, sub.project_id, "project.read"
                            )
                        except PermissionError:
                            client.close_code = 4403
                            break

    @staticmethod
    def _matches(pattern: str, key: str) -> bool:
        return key == pattern or (pattern.endswith(".#") and key.startswith(pattern[:-1]))

    @staticmethod
    async def _offer(client: Client, event: dict[str, Any]) -> None:
        try:
            client.outgoing.put_nowait(event)
        except asyncio.QueueFull:
            client.close_code = 1012
