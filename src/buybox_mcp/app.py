from __future__ import annotations

from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.types import ASGIApp, Receive, Scope, Send

from buybox_mcp.auth import apply_bearer_auth
from buybox_mcp.config import Settings, get_settings
from buybox_mcp.runtime import ApplicationRuntime, set_runtime
from buybox_mcp.server import create_mcp_server


def create_app(settings: Settings | None = None) -> ASGIApp:
    resolved = settings or get_settings()
    runtime = ApplicationRuntime(resolved)
    mcp = create_mcp_server(resolved)

    @asynccontextmanager
    async def lifespan(app: Starlette):
        await runtime.start()
        set_runtime(runtime)
        async with mcp.session_manager.run():
            yield
        set_runtime(None)
        await runtime.stop()

    async def index(request: Request) -> JSONResponse:
        base_url = str(request.base_url).rstrip("/")
        return JSONResponse(
            {
                "name": resolved.app_name,
                "status": "ok",
                "transport": "streamable-http",
                "mcp_url": f"{base_url}{resolved.mcp_mount_path}",
                "auth": {
                    "type": "bearer",
                    "header": "Authorization: Bearer <token>",
                },
            }
        )

    async def healthz(request: Request) -> JSONResponse:
        return JSONResponse(await runtime.health_snapshot())

    app = Starlette(
        debug=resolved.env == "development",
        routes=[
            Route("/", endpoint=index),
            Route(resolved.health_path, endpoint=healthz),
            Mount(resolved.mcp_mount_path, app=mcp.streamable_http_app()),
        ],
        lifespan=lifespan,
    )

    if resolved.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(resolved.cors_allowed_origins),
            allow_methods=["GET", "POST", "DELETE"],
            allow_headers=["Authorization", "Content-Type", "Mcp-Session-Id"],
            expose_headers=["Mcp-Session-Id"],
        )

    return apply_bearer_auth(
        app,
        token=resolved.bearer_token.get_secret_value(),
        protected_prefixes=(resolved.mcp_mount_path,),
        exempt_paths=("/", resolved.health_path),
    )


class LazyApp:
    def __init__(self) -> None:
        self._app: ASGIApp | None = None

    def _resolve(self) -> ASGIApp:
        if self._app is None:
            self._app = create_app()
        return self._app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._resolve()(scope, receive, send)


app = LazyApp()
