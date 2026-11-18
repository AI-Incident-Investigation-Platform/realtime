import json
from typing import Any, cast
from uuid import uuid4

import pytest

from src.broker import LiveBroker


class Exchange:
    def __init__(self, name):
        self.name = name


class Queue:
    def __init__(self):
        self.bindings = []
        self.unbindings = []

    async def bind(self, exchange, routing_key):
        self.bindings.append((exchange.name, routing_key))

    async def unbind(self, exchange, routing_key):
        self.unbindings.append((exchange.name, routing_key))


@pytest.mark.asyncio
@pytest.mark.case(id="RT-008")
async def test_replica_queue_is_exclusive_bounded_and_control_bound():
    queue = Queue()
    broker = LiveBroker("amqp://guest:guest@localhost/")
    broker._queue = cast(Any, queue)
    broker._exchanges = {name: cast(Any, Exchange(name)) for name in broker.exchanges}
    broker._ready.set()
    await broker.bind_controls()
    assert ("auth.live", "control.#") in queue.bindings
    assert ("projects.live", "control.#") in queue.bindings
    await broker.bind("incidents.live", f"project.{uuid4()}")
    assert queue.bindings[-1][0] == "incidents.live"
    assert broker.queue_arguments == {
        "x-max-length": 1000,
        "x-max-length-bytes": 4194304,
        "x-message-ttl": 30000,
        "x-overflow": "drop-head",
    }


@pytest.mark.case(id="RT-009")
def test_broker_unwraps_live_envelope():
    payload = {"message_id": str(uuid4()), "data": {"type": "inbox.changed", "unread_count": 2}}
    assert LiveBroker.payload(json.dumps(payload).encode()) == payload["data"]
