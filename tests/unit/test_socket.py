import json
from uuid import uuid4

import pytest

from src.socket import parse_command


@pytest.mark.case(id="RT-010")
def test_frame_limit_and_exact_command_shape():
    project_id = str(uuid4())
    command = {
        "action": "subscribe",
        "topic": "project",
        "subscription_id": "p",
        "project_id": project_id,
    }
    assert parse_command(json.dumps(command).encode()) == command
    with pytest.raises(ValueError):
        parse_command(b"{" + b" " * 4096 + b"}")
    with pytest.raises(ValueError):
        parse_command(json.dumps({**command, "routing_key": "project.#"}).encode())
    with pytest.raises(ValueError):
        parse_command(json.dumps({**command, "project_id": "*"}).encode())
