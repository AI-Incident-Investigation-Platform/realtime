import asyncio
from uuid import uuid4

import pytest

from src.hub import Hub, Principal


class Broker:
    def __init__(self):
        self.binds = []
        self.unbinds = []
        self.on_bind = None

    async def bind(self, exchange, key):
        self.binds.append((exchange, key))
        if self.on_bind:
            await self.on_bind()

    async def unbind(self, exchange, key):
        self.unbinds.append((exchange, key))


class Access:
    def __init__(self):
        self.allowed = True

    async def require(self, principal, project_id, permission):
        if not self.allowed:
            raise PermissionError

    async def live(self, principal):
        return self.allowed


class History:
    def __init__(self, project_id):
        self.project_id = project_id
        self.events = []

    async def project(self, investigation_id, token):
        return self.project_id

    async def events_after(self, investigation_id, after_sequence, token):
        await asyncio.sleep(0)
        return [event for event in self.events if event["sequence"] > after_sequence]


@pytest.fixture
def setup():
    project_id = uuid4()
    broker = Broker()
    access = Access()
    history = History(project_id)
    hub = Hub(broker, access, history, unbind_delay=0)
    principal = Principal(uuid4(), uuid4(), "token", 9999999999)
    return hub, broker, access, history, principal


@pytest.mark.asyncio
@pytest.mark.case(id="RT-001")
async def test_bind_precedes_replay_and_ack_and_merges_live_event(setup):
    hub, broker, _, history, principal = setup
    investigation_id = uuid4()
    history.events = [{"type": "investigation.started", "sequence": 1, "event_id": str(uuid4())}]
    client = hub.add(principal)

    async def publish_during_bind():
        await hub.deliver(
            "incidents.live",
            f"project.{history.project_id}.investigation.{investigation_id}",
            {"type": "investigation.progress", "sequence": 2, "event_id": str(uuid4())},
        )

    broker.on_bind = publish_during_bind
    await hub.subscribe(
        client,
        {
            "action": "subscribe",
            "topic": "investigation",
            "subscription_id": "x",
            "investigation_id": str(investigation_id),
            "after_sequence": 0,
        },
    )
    received = [client.outgoing.get_nowait() for _ in range(3)]
    assert [item["type"] for item in received] == [
        "investigation.started",
        "investigation.progress",
        "subscription.ack",
    ]
    assert received[-1]["last_sequence"] == 2
    assert broker.binds == [
        ("incidents.live", f"project.{history.project_id}.investigation.{investigation_id}")
    ]


@pytest.mark.asyncio
@pytest.mark.case(id="RT-002")
async def test_one_binding_per_replica_and_revocation_stops_delivery(setup):
    hub, broker, access, history, principal = setup
    clients = [hub.add(principal), hub.add(principal)]
    for index, client in enumerate(clients):
        await hub.subscribe(
            client,
            {
                "action": "subscribe",
                "topic": "project",
                "subscription_id": str(index),
                "project_id": str(history.project_id),
            },
        )
        client.outgoing.get_nowait()
    assert len(broker.binds) == 4  # exact and descendants in both source exchanges
    await hub.deliver(
        "incidents.live",
        f"project.{history.project_id}",
        {"type": "incident.created", "project_id": str(history.project_id)},
    )
    assert all(client.outgoing.get_nowait()["type"] == "incident.created" for client in clients)
    access.allowed = False
    await hub.deliver(
        "incidents.live",
        f"project.{history.project_id}",
        {"type": "incident.updated", "project_id": str(history.project_id)},
    )
    assert all(client.close_code == 4403 for client in clients)


@pytest.mark.asyncio
@pytest.mark.case(id="RT-003")
async def test_broker_loss_requests_resync(setup):
    hub, _, _, _, principal = setup
    client = hub.add(principal)
    await hub.broker_lost()
    assert client.outgoing.get_nowait() == {"type": "resync_required", "reason": "broker_lost"}


@pytest.mark.asyncio
@pytest.mark.case(id="RT-004")
async def test_periodic_check_closes_access_revoked_without_new_events(setup):
    hub, _, access, history, principal = setup
    client = hub.add(principal)
    await hub.subscribe(
        client,
        {
            "action": "subscribe",
            "topic": "project",
            "subscription_id": "p",
            "project_id": str(history.project_id),
        },
    )
    access.allowed = False
    await hub.check_permissions(client)
    assert client.close_code == 4403


@pytest.mark.asyncio
@pytest.mark.case(id="RT-005")
async def test_limits_and_subscription_ids(setup):
    hub, _, _, history, principal = setup
    client = hub.add(principal)
    with pytest.raises(ValueError):
        await hub.subscribe(
            client,
            {"action": "subscribe", "topic": "project", "subscription_id": "*", "project_id": "*"},
        )
    for index in range(10):
        await hub.subscribe(
            client,
            {
                "action": "subscribe",
                "topic": "project",
                "subscription_id": str(index),
                "project_id": str(history.project_id),
            },
        )
        client.outgoing.get_nowait()
    with pytest.raises(ValueError):
        await hub.subscribe(
            client, {"action": "subscribe", "topic": "inbox", "subscription_id": "extra"}
        )


@pytest.mark.asyncio
@pytest.mark.case(id="RT-013")
async def test_project_deletion_closes_every_member_but_removal_targets_one(setup):
    hub, _, _, history, principal = setup
    other = Principal(uuid4(), uuid4(), "other-token", 9999999999)
    clients = [hub.add(principal), hub.add(other)]
    for client in clients:
        await hub.subscribe(
            client,
            {
                "action": "subscribe",
                "topic": "project",
                "subscription_id": "p",
                "project_id": str(history.project_id),
            },
        )
    await hub.deliver(
        "projects.live",
        "control.member.removed",
        {"project_id": str(history.project_id), "user_id": str(principal.user_id)},
    )
    assert [client.close_code for client in clients] == [4403, None]
    await hub.deliver(
        "projects.live",
        "control.project.deleting",
        {"project_id": str(history.project_id)},
    )
    assert clients[1].close_code == 4403


@pytest.mark.asyncio
@pytest.mark.case(id="RT-014")
async def test_replay_rechecks_access_before_sending_history(setup):
    hub, broker, access, history, principal = setup
    investigation_id = uuid4()
    history.events = [{"type": "investigation.started", "sequence": 1}]
    client = hub.add(principal)

    async def revoke_after_bind():
        access.allowed = False

    broker.on_bind = revoke_after_bind
    with pytest.raises(PermissionError):
        await hub.subscribe(
            client,
            {
                "action": "subscribe",
                "topic": "investigation",
                "subscription_id": "x",
                "investigation_id": str(investigation_id),
                "after_sequence": 0,
            },
        )
    assert client.outgoing.empty()
    assert not client.subscriptions
