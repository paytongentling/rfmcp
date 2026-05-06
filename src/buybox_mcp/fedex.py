from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

from buybox_mcp.config import Settings


class FedexNotConfiguredError(RuntimeError):
    """Raised when a FedEx call is attempted but credentials are not set."""


class FedexApiError(RuntimeError):
    """Raised when the FedEx API returns an error envelope."""

    def __init__(self, code: str, message: str, http_status: int | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.http_status = http_status


@dataclass
class _CachedToken:
    value: str
    expires_at: float


class FedexClient:
    """Thin async client for the FedEx REST APIs we use.

    Caches OAuth tokens until ~30s before expiry. Safe for concurrent use.
    """

    def __init__(self, settings: Settings, *, http: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._http = http or httpx.AsyncClient(timeout=30.0)
        self._owns_http = http is None
        self._token: _CachedToken | None = None
        self._token_lock = asyncio.Lock()

    @property
    def configured(self) -> bool:
        return bool(
            self._settings.fedex_api_key
            and self._settings.fedex_api_secret
            and self._settings.fedex_account_number
        )

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _get_token(self) -> str:
        if not self.configured:
            raise FedexNotConfiguredError(
                "FedEx credentials are not configured. Set FEDEX_API_KEY, "
                "FEDEX_API_SECRET, and FEDEX_ACCOUNT_NUMBER."
            )
        async with self._token_lock:
            now = time.monotonic()
            if self._token and self._token.expires_at - 30 > now:
                return self._token.value
            assert self._settings.fedex_api_key is not None
            assert self._settings.fedex_api_secret is not None
            resp = await self._http.post(
                f"{self._settings.fedex_api_base}/oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._settings.fedex_api_key.get_secret_value(),
                    "client_secret": self._settings.fedex_api_secret.get_secret_value(),
                },
            )
            payload = resp.json()
            if resp.status_code >= 400 or "access_token" not in payload:
                errors = payload.get("errors") or [{}]
                err = errors[0]
                raise FedexApiError(
                    err.get("code", "OAUTH.ERROR"),
                    err.get("message", "OAuth token request failed"),
                    http_status=resp.status_code,
                )
            self._token = _CachedToken(
                value=payload["access_token"],
                expires_at=now + float(payload.get("expires_in", 3300)),
            )
            return self._token.value

    async def quote_rates(
        self,
        *,
        origin_zip: str,
        dest_zip: str,
        weight_lb: float,
        ship_date: str | None = None,
        length_in: float | None = None,
        width_in: float | None = None,
        height_in: float | None = None,
        service_type: str | None = None,
        saturday_delivery: bool = False,
        origin_country: str = "US",
        dest_country: str = "US",
    ) -> dict[str, Any]:
        """Call /rate/v1/rates/quotes and return the parsed JSON response.

        Uses the standing assumptions: pickupType=USE_SCHEDULED_PICKUP (3PL has
        a daily pickup), packagingType=YOUR_PACKAGING, no signature required,
        and returnTransitTimes=true so the response includes commit/promise data.
        """

        token = await self._get_token()
        package: dict[str, Any] = {"weight": {"units": "LB", "value": weight_lb}}
        if length_in is not None and width_in is not None and height_in is not None:
            package["dimensions"] = {
                "length": length_in,
                "width": width_in,
                "height": height_in,
                "units": "IN",
            }

        requested_shipment: dict[str, Any] = {
            "shipper": {"address": {"postalCode": origin_zip, "countryCode": origin_country}},
            "recipient": {"address": {"postalCode": dest_zip, "countryCode": dest_country}},
            "pickupType": "USE_SCHEDULED_PICKUP",
            "packagingType": "YOUR_PACKAGING",
            "rateRequestType": ["ACCOUNT", "LIST"],
            "requestedPackageLineItems": [package],
        }
        if ship_date:
            requested_shipment["shipDateStamp"] = ship_date
        if service_type:
            requested_shipment["serviceType"] = service_type
        if saturday_delivery:
            requested_shipment["specialServicesRequested"] = {
                "specialServiceTypes": ["SATURDAY_DELIVERY"],
            }

        body = {
            "accountNumber": {"value": self._settings.fedex_account_number},
            "rateRequestControlParameters": {
                "returnTransitTimes": True,
                "servicesNeededOnRateFailure": True,
            },
            "requestedShipment": requested_shipment,
        }

        resp = await self._http.post(
            f"{self._settings.fedex_api_base}/rate/v1/rates/quotes",
            headers={
                "Content-Type": "application/json",
                "X-locale": "en_US",
                "Authorization": f"Bearer {token}",
            },
            json=body,
        )
        payload = resp.json()
        if resp.status_code >= 400 or "errors" in payload:
            errors = payload.get("errors") or [{"code": "HTTP.ERROR", "message": resp.text[:200]}]
            err = errors[0]
            raise FedexApiError(
                err.get("code", "RATE.ERROR"),
                err.get("message", "Rate request failed"),
                http_status=resp.status_code,
            )
        return payload

    async def validate_address(
        self,
        *,
        street_lines: list[str],
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        country: str = "US",
    ) -> dict[str, Any]:
        """Call /address/v1/addresses/resolve and return the parsed JSON response.

        Use to standardize a buyer-supplied address, classify it as residential
        vs business (a Ground rate input), and detect undeliverable / interpolated
        / PO Box situations before paying for a Rate call or printing a label.
        """

        token = await self._get_token()
        address: dict[str, Any] = {"countryCode": country}
        if street_lines:
            address["streetLines"] = street_lines
        if city is not None:
            address["city"] = city
        if state is not None:
            address["stateOrProvinceCode"] = state
        if postal_code is not None:
            address["postalCode"] = postal_code

        body = {"addressesToValidate": [{"address": address}]}

        resp = await self._http.post(
            f"{self._settings.fedex_api_base}/address/v1/addresses/resolve",
            headers={
                "Content-Type": "application/json",
                "X-locale": "en_US",
                "Authorization": f"Bearer {token}",
            },
            json=body,
        )
        payload = resp.json()
        if resp.status_code >= 400 or "errors" in payload:
            errors = payload.get("errors") or [{"code": "HTTP.ERROR", "message": resp.text[:200]}]
            err = errors[0]
            raise FedexApiError(
                err.get("code", "ADDRESS.ERROR"),
                err.get("message", "Address validation request failed"),
                http_status=resp.status_code,
            )
        return payload
