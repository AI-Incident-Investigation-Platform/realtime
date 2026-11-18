"""Small explicit runtime configuration; config.toml is mounted from a Secret."""

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Settings:
    rabbitmq_url: str
    redis_url: str
    jwks_url: str
    auth_url: str
    projects_url: str
    incidents_url: str
    client_id: str
    client_secret: str
    allowed_origins: tuple[str, ...] = ("http://localhost", "https://localhost")
    issuer: str = "auth"
    audience: str = "aiip"
    host: str = "0.0.0.0"
    port: int = 8009

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> Settings:
        required = (
            "rabbitmq_url",
            "redis_url",
            "jwks_url",
            "auth_url",
            "projects_url",
            "incidents_url",
            "client_id",
            "client_secret",
        )
        missing = [key for key in required if not values.get(key)]
        if missing:
            raise ValueError(f"missing settings: {', '.join(missing)}")
        if "allowed_origins" in values:
            values = {**values, "allowed_origins": tuple(values["allowed_origins"])}
        return cls(**values)

    @classmethod
    def load(cls) -> Settings:
        path = Path(os.getenv("REALTIME_CONFIG", "config.toml"))
        values = tomllib.loads(path.read_text()) if path.exists() else {}
        for name in cls.__dataclass_fields__:
            env = os.getenv(f"REALTIME_{name.upper()}")
            if env is not None:
                values[name] = env.split(",") if name == "allowed_origins" else env
        return cls.from_mapping(values)
