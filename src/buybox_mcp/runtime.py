from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pymongo import AsyncMongoClient

from buybox_mcp.config import Settings


@dataclass
class ApplicationRuntime:
    settings: Settings
    mongo_client: AsyncMongoClient[dict[str, Any]] | None = None
    mongo_database: Any | None = None
    startup_errors: list[str] = field(default_factory=list)

    async def start(self) -> None:
        if not self.settings.mongo_uri:
            return

        self.mongo_client = AsyncMongoClient(self.settings.mongo_uri.get_secret_value())
        self.mongo_database = self.mongo_client.get_database(self.settings.mongo_database)

        try:
            await self.mongo_client.admin.command("ping")
        except Exception as exc:  # pragma: no cover - exercised through health checks at runtime
            self.startup_errors.append(f"Mongo ping failed: {exc}")

    async def stop(self) -> None:
        if self.mongo_client is not None:
            await self.mongo_client.close()

    async def health_snapshot(self) -> dict[str, Any]:
        mongo = {
            "configured": self.mongo_client is not None,
            "database": self.settings.mongo_database,
            "ok": None,
        }
        if self.mongo_client is not None:
            try:
                await self.mongo_client.admin.command("ping")
                mongo["ok"] = True
            except Exception as exc:
                mongo["ok"] = False
                mongo["error"] = str(exc)

        return {
            "status": "ok",
            "environment": self.settings.env,
            "mongo": mongo,
            "startup_errors": self.startup_errors,
        }


_runtime: ApplicationRuntime | None = None


def set_runtime(runtime: ApplicationRuntime | None) -> None:
    global _runtime
    _runtime = runtime


def get_runtime() -> ApplicationRuntime:
    if _runtime is None:
        raise RuntimeError("Application runtime is not initialized")
    return _runtime
