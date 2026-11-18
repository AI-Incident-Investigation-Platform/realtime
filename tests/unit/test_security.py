import time
from typing import Any, cast
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from src.security import JwtAuthenticator


@pytest.mark.asyncio
@pytest.mark.case(id="RT-007")
async def test_jwt_validates_signature_claims_and_live_session():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwk["kid"] = "test"
    user_id, session_id = uuid4(), uuid4()

    def handler(request):
        return httpx.Response(200, json={"keys": [jwk]})

    class Sessions:
        alive = True

        async def exists(self, key):
            assert key == f"auth:session:{session_id}"
            return self.alive

    sessions = Sessions()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        auth = JwtAuthenticator(http, cast(Any, sessions), "http://auth/jwks", "auth", "aiip")
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
            headers={"kid": "test"},
        )
        assert (await auth.authenticate(token)).user_id == user_id
        sessions.alive = False
        with pytest.raises(PermissionError):
            await auth.authenticate(token)
