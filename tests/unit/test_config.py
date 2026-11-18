import pytest

from src.config import Settings


@pytest.mark.case(id="RT-011")
def test_settings_require_external_credentials():
    settings = Settings.from_mapping(
        {
            "rabbitmq_url": "amqp://guest:guest@localhost/",
            "redis_url": "redis://localhost/0",
            "jwks_url": "http://auth/jwks",
            "auth_url": "http://auth",
            "projects_url": "http://projects",
            "incidents_url": "http://incidents",
            "client_id": "realtime",
            "client_secret": "fake",
        }
    )
    assert settings.port == 8009
    assert "http://localhost" in settings.allowed_origins
