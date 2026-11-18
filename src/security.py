"""Cookie authentication, project ACL and per-session connection leases."""

import asyncio
import time
from typing import cast
from uuid import UUID, uuid4

import httpx
import jwt
from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal as AclPrincipal
from aiip_kit.errors import NotAuthenticatedError, PermissionDeniedError, ProjectNotFoundError
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from redis.asyncio import Redis

from src.hub import Principal


class JwtAuthenticator:
    def __init__(
        self,
        http: httpx.AsyncClient,
        redis: Redis,
        jwks_url: str,
        issuer: str,
        audience: str,
    ) -> None:
        self._http = http
        self._redis = redis
        self._jwks_url = jwks_url
        self._issuer = issuer
        self._audience = audience
        self._keys: dict[str, RSAPublicKey] = {}
        self._lock = asyncio.Lock()

    async def authenticate(self, token: str | None) -> Principal:
        if not token:
            raise PermissionError("missing access token")
        try:
            kid = jwt.get_unverified_header(token)["kid"]
            key = self._keys.get(kid)
            if key is None:
                async with self._lock:
                    if kid not in self._keys:
                        response = await self._http.get(self._jwks_url, timeout=2)
                        response.raise_for_status()
                        self._keys = {
                            item["kid"]: cast(
                                RSAPublicKey, jwt.algorithms.RSAAlgorithm.from_jwk(item)
                            )
                            for item in response.json()["keys"]
                            if item.get("kty") == "RSA"
                        }
                    key = self._keys.get(kid)
            if key is None:
                raise PermissionError("unknown signing key")
            payload = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer=self._issuer,
                audience=self._audience,
                options={"require": ["iss", "aud", "sub", "sid", "iat", "exp"]},
            )
            principal = Principal(
                UUID(payload["sub"]), UUID(payload["sid"]), token, int(payload["exp"])
            )
            if not await self._redis.exists(f"auth:session:{principal.session_id}"):
                raise PermissionError("session revoked")
            return principal
        except (jwt.PyJWTError, KeyError, TypeError, ValueError, httpx.HTTPError) as exc:
            raise PermissionError("invalid access token") from exc


class ProjectAccess:
    def __init__(self, checker: AccessChecker, redis: Redis) -> None:
        self._checker = checker
        self._redis = redis

    async def require(self, principal: Principal, project_id: UUID, permission: str) -> None:
        try:
            await self._checker.require(
                AclPrincipal(principal.user_id, principal.session_id), project_id, permission
            )
        except (NotAuthenticatedError, PermissionDeniedError, ProjectNotFoundError) as exc:
            raise PermissionError("access revoked") from exc

    async def live(self, principal: Principal) -> bool:
        return bool(await self._redis.exists(f"auth:session:{principal.session_id}"))


_ACQUIRE = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) >= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', key, tonumber(ARGV[2]), ARGV[4])
redis.call('EXPIRE', key, tonumber(ARGV[5]))
return 1
"""

_RENEW = """
local expiry = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not expiry or tonumber(expiry) <= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return 1
"""


class ConnectionLeases:
    def __init__(self, redis: Redis, ttl_seconds: int = 45, limit: int = 2) -> None:
        self._redis = redis
        self.ttl_seconds = ttl_seconds
        self.limit = limit

    async def acquire(self, session_id: UUID) -> str | None:
        connection_id = str(uuid4())
        now = int(time.time())
        acquired = await self._redis.eval(
            _ACQUIRE,
            1,
            f"realtime:session:{session_id}",
            now,
            now + self.ttl_seconds,
            self.limit,
            connection_id,
            self.ttl_seconds * 2,
        )
        return connection_id if acquired == 1 else None

    async def renew(self, session_id: UUID, connection_id: str) -> bool:
        now = int(time.time())
        renewed = await self._redis.eval(
            _RENEW,
            1,
            f"realtime:session:{session_id}",
            connection_id,
            now,
            now + self.ttl_seconds,
            self.ttl_seconds * 2,
        )
        return renewed == 1

    async def release(self, session_id: UUID, connection_id: str) -> None:
        await self._redis.zrem(f"realtime:session:{session_id}", connection_id)
