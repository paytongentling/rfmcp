from __future__ import annotations

import secrets
from collections.abc import Iterable
from typing import Any

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class BearerTokenMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        token: str,
        protected_prefixes: Iterable[str],
        exempt_paths: Iterable[str],
    ) -> None:
        self.app = app
        self.token = token
        self.protected_prefixes = tuple(protected_prefixes)
        self.exempt_paths = tuple(exempt_paths)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if scope.get("method") == "OPTIONS":
            await self.app(scope, receive, send)
            return

        if self._is_exempt(path) or not self._is_protected(path):
            await self.app(scope, receive, send)
            return

        auth_header = Headers(scope=scope).get("authorization")
        if not self._is_authorized(auth_header):
            response = JSONResponse(
                {"error": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer realm="buybox-mcp"'},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)

    def _is_exempt(self, path: str) -> bool:
        return path in self.exempt_paths

    def _is_protected(self, path: str) -> bool:
        for prefix in self.protected_prefixes:
            if path == prefix or path.startswith(f"{prefix}/"):
                return True
        return False

    def _is_authorized(self, auth_header: str | None) -> bool:
        if not auth_header:
            return False
        scheme, _, token = auth_header.partition(" ")
        return scheme.lower() == "bearer" and secrets.compare_digest(token, self.token)


def apply_bearer_auth(
    app: ASGIApp,
    *,
    token: str,
    protected_prefixes: Iterable[str],
    exempt_paths: Iterable[str],
) -> ASGIApp:
    return BearerTokenMiddleware(
        app,
        token=token,
        protected_prefixes=protected_prefixes,
        exempt_paths=exempt_paths,
    )
