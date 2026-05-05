from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILES = (
    ".env.local",
    ".env",
    str(PACKAGE_ROOT / ".env.local"),
    str(PACKAGE_ROOT / ".env"),
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="BUYBOX_MCP_",
        env_file=DEFAULT_ENV_FILES,
        env_file_encoding="utf-8",
        case_sensitive=False,
        populate_by_name=True,
    )

    env: str = "development"
    app_name: str = "Buy Box Analytics MCP"
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    bearer_token: SecretStr
    mongo_uri: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("BUYBOX_MCP_MONGO_URI", "MONGODB_URI"),
    )
    mongo_database: str | None = Field(
        default="bb2",
        validation_alias=AliasChoices(
            "BUYBOX_MCP_MONGO_DATABASE",
            "MONGODB_DATABASE",
            "MONGO_DATABASE",
        ),
    )
    inventory_mongo_database: str | None = Field(
        default="INV-Tracker",
        validation_alias=AliasChoices(
            "BUYBOX_MCP_INVENTORY_MONGO_DATABASE",
            "INVENTORY_MONGODB_DATABASE",
            "INVENTORY_MONGO_DATABASE",
        ),
    )
    mcp_mount_path: str = "/mcp"
    health_path: str = "/healthz"
    cors_allowed_origins: tuple[str, ...] = ()

    fedex_api_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "BUYBOX_MCP_FEDEX_API_KEY",
            "FEDEX_API_KEY",
        ),
    )
    fedex_api_secret: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "BUYBOX_MCP_FEDEX_API_SECRET",
            "FEDEX_API_SECRET",
        ),
    )
    fedex_account_number: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "BUYBOX_MCP_FEDEX_ACCOUNT_NUMBER",
            "FEDEX_ACCOUNT_NUMBER",
        ),
    )
    fedex_api_base: str = Field(
        default="https://apis-sandbox.fedex.com",
        validation_alias=AliasChoices(
            "BUYBOX_MCP_FEDEX_API_BASE",
            "FEDEX_API_BASE",
        ),
    )

    @field_validator("cors_allowed_origins", mode="before")
    @classmethod
    def parse_cors_allowed_origins(cls, value: object) -> tuple[str, ...]:
        if value in (None, ""):
            return ()
        if isinstance(value, str):
            return tuple(item.strip() for item in value.split(",") if item.strip())
        if isinstance(value, (list, tuple, set)):
            return tuple(str(item).strip() for item in value if str(item).strip())
        raise TypeError("cors_allowed_origins must be a comma-separated string or list")

    @model_validator(mode="after")
    def validate_mongo_settings(self) -> Settings:
        if self.mongo_uri and not self.mongo_database:
            raise ValueError("BUYBOX_MCP_MONGO_DATABASE is required when BUYBOX_MCP_MONGO_URI is set")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
