from __future__ import annotations

from http import HTTPStatus

import httpx
import pytest

from buybox_mcp.app import create_app
from buybox_mcp.config import Settings
from buybox_mcp.server import (
    _build_inventory_component_availability,
    _normalize_as_of_selector,
    _score_keyword_source_document,
    _score_inventory_location_document,
    _score_inventory_sku_document,
    _score_source_document,
    _score_tracked_keyword_document,
    create_mcp_server,
)


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


def test_server_instructions_include_buybox_winner_guidance() -> None:
    server = create_mcp_server(build_settings())

    assert "our_seller_name" in server.instructions
    assert "lowest offer_index" in server.instructions
    assert "tracking_key" in server.instructions
    assert "buybox_featured_offer_percent_resolve_source" in server.instructions
    assert "buybox_featured_offer_percent_summarize_status" in server.instructions
    assert "Do not assume every collection has rows for every date" in server.instructions
    assert "inventory_by_location_resolve_sku" in server.instructions
    assert "inventory_by_location_find_documents" in server.instructions
    assert "inventory_by_location_aggregate_documents" in server.instructions
    assert "inventory_by_location_quantity" in server.instructions
    assert "recorded_quantity and buildable_quantity separately" in server.instructions
    assert "keyword_rank_tracking_list_sources" in server.instructions
    assert "keyword_rank_tracking_find_documents" in server.instructions
    assert "keyword_rank_tracking_aggregate_documents" in server.instructions
    assert "keyword_rank_tracking_rank_history" in server.instructions
    assert "search_results[].position" in server.instructions


def test_server_registers_keyword_rank_tracking_tools() -> None:
    server = create_mcp_server(build_settings())
    tool_names = set(server._tool_manager._tools)

    assert "keyword_rank_tracking_list_collections" in tool_names
    assert "keyword_rank_tracking_inspect_collection" in tool_names
    assert "keyword_rank_tracking_find_documents" in tool_names
    assert "keyword_rank_tracking_distinct_values" in tool_names
    assert "keyword_rank_tracking_aggregate_documents" in tool_names
    assert "keyword_rank_tracking_list_sources" in tool_names
    assert "keyword_rank_tracking_resolve_source" in tool_names
    assert "keyword_rank_tracking_resolve_keyword" in tool_names
    assert "keyword_rank_tracking_resolve_asin" in tool_names
    assert "keyword_rank_tracking_latest_search_results" in tool_names
    assert "keyword_rank_tracking_rank_history" in tool_names
    assert "keyword_rank_tracking_search_query_volume" in tool_names
    assert "inventory_by_location_list_collections" in tool_names
    assert "inventory_by_location_inspect_collection" in tool_names
    assert "inventory_by_location_find_documents" in tool_names
    assert "inventory_by_location_distinct_values" in tool_names
    assert "inventory_by_location_aggregate_documents" in tool_names


def test_score_source_document_supports_exact_slug_and_fuzzy_name() -> None:
    document = {
        "name": "All Treasure Garden Offers",
        "slug": "all-treasure-garden-offers",
        "slug_lower": "all-treasure-garden-offers",
        "webhook_slug": "all-treasure-garden-offers",
        "webhook_slug_lower": "all-treasure-garden-offers",
    }

    assert _score_source_document(
        document,
        query_text="all treasure garden offers",
        query_slug="all-treasure-garden-offers",
    ) == (100, "slug_exact")
    assert _score_source_document(
        document,
        query_text="treasure garden",
        query_slug="treasure-garden",
    ) is not None


def test_score_keyword_source_document_supports_exact_slug_and_collection() -> None:
    document = {
        "name": "Treasure Garden Branded",
        "slug": "treasure-garden-branded",
        "slug_lower": "treasure-garden-branded",
        "webhook_slug": "treasure-garden-branded",
        "webhook_slug_lower": "treasure-garden-branded",
        "search_terms_collection": "source-treasure-garden-branded-search_terms",
    }

    assert _score_keyword_source_document(
        document,
        query_text="treasure garden branded",
        query_slug="treasure-garden-branded",
    ) == (100, "slug_exact")
    assert _score_keyword_source_document(
        document,
        query_text="source-treasure-garden-branded-search_terms",
        query_slug="source-treasure-garden-branded-search-terms",
    ) is not None


def test_score_tracked_keyword_document_supports_exact_and_partial_match() -> None:
    document = {
        "keyword": "gas fireplace logs",
        "keyword_lower": "gas fireplace logs",
    }

    assert _score_tracked_keyword_document(
        document,
        query_text="gas fireplace logs",
    ) == (120, "keyword_exact")
    assert _score_tracked_keyword_document(
        document,
        query_text="gas fireplace",
    ) == (110, "keyword_prefix")


def test_normalize_as_of_selector_supports_date_and_timestamp() -> None:
    assert _normalize_as_of_selector("2026-04-30") == "2026-04-30T23:59:59.999999Z"
    assert _normalize_as_of_selector("2026-04-30T14:09:29Z") == "2026-04-30T14:09:29Z"


def test_score_inventory_sku_document_supports_exact_sku_alias_and_asin() -> None:
    document = {
        "sku": "CHD-24-G45",
        "name": "Peterson Real Fyre 24-Inch Charred Oak Gas Log Set with Vented G45 Burner",
        "aliases": ["CHD-24-G45-MF"],
        "childAsins": ["B000E86AKC"],
    }

    assert _score_inventory_sku_document(
        document,
        query_text="chd-24-g45",
        query_ident="chd24g45",
    ) == (120, "sku_exact")
    assert _score_inventory_sku_document(
        document,
        query_text="chd-24-g45-mf",
        query_ident="chd24g45mf",
    ) == (116, "alias_exact")
    assert _score_inventory_sku_document(
        document,
        query_text="b000e86akc",
        query_ident="b000e86akc",
    ) == (112, "asin_exact")


def test_score_inventory_location_document_supports_name_and_suffix() -> None:
    document = {
        "_id": "68ef25be565252f65328bd0a",
        "name": "Ontario, CA",
        "amazonSkuSuffix": "-MF",
        "shippingTemplateName": "Migrated Template",
    }

    assert _score_inventory_location_document(
        document,
        query_text="ontario, ca",
        query_ident="ontarioca",
    ) == (100, "name_exact")
    assert _score_inventory_location_document(
        document,
        query_text="-mf",
        query_ident="mf",
    ) == (97, "suffix_exact")


def test_build_inventory_component_availability_returns_limiting_component() -> None:
    sku_document = {
        "kitComponents": [
            {"componentSku": "LOG-24", "quantity": 1},
            {"componentSku": "BURNER-24", "quantity": 2},
        ]
    }
    sku_catalog = {
        "LOG-24": {"name": "24in Log Set"},
        "BURNER-24": {"name": "24in Burner"},
    }
    inventory_by_pair = {
        ("loc-1", "LOG-24"): 5,
        ("loc-1", "BURNER-24"): 3,
    }

    rows, buildable_quantity, limiting_component = _build_inventory_component_availability(
        sku_document,
        location_id="loc-1",
        inventory_by_pair=inventory_by_pair,
        sku_catalog=sku_catalog,
    )

    assert len(rows) == 2
    assert buildable_quantity == 1
    assert limiting_component is not None
    assert limiting_component.component_sku == "BURNER-24"
