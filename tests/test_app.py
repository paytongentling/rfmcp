from __future__ import annotations

from http import HTTPStatus

import httpx
import pytest

from buybox_mcp.app import create_app
from buybox_mcp.config import Settings
from buybox_mcp.server import create_mcp_server


def build_settings() -> Settings:
    return Settings(
        bearer_token="test-token",
        mongo_uri=None,
        mongo_database=None,
    )


@pytest.mark.asyncio
async def test_healthz_is_public() -> None:
    app = create_app(build_settings())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/healthz")

    assert response.status_code == HTTPStatus.OK
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_mcp_requires_bearer_token() -> None:
    app = create_app(build_settings())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/mcp")

    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.headers["www-authenticate"] == 'Bearer realm="buybox-mcp"'


@pytest.mark.asyncio
async def test_root_exposes_connection_info() -> None:
    app = create_app(build_settings())
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="https://example.com") as client:
        response = await client.get("/")

    assert response.status_code == HTTPStatus.OK
    body = response.json()
    assert body["mcp_url"] == "https://example.com/mcp"
    assert body["auth"]["type"] == "bearer"


def test_streamable_http_host_tracks_settings() -> None:
    server = create_mcp_server(
        Settings(
            bearer_token="test-token",
            mongo_uri=None,
            mongo_database=None,
            host="0.0.0.0",
            port=10000,
        )
    )

    assert server.settings.host == "0.0.0.0"
    assert server.settings.port == 10000
    assert server.settings.transport_security is None
