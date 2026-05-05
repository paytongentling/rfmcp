from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from bson import json_util
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from buybox_mcp.config import Settings, get_settings
from buybox_mcp.fedex import FedexApiError, FedexNotConfiguredError
from buybox_mcp.runtime import ApplicationRuntime, get_runtime

MAX_FIND_LIMIT = 200
MAX_AGGREGATE_LIMIT = 200
MAX_DISTINCT_LIMIT = 200
MAX_SAMPLE_SIZE = 5
MAX_FIELD_DEPTH = 3
MAX_KEYWORD_SOURCE_COUNT = 50
MAX_KEYWORD_RESULT_LIMIT = 100
MAX_KEYWORD_HISTORY_LIMIT = 60
MAX_KEYWORD_VOLUME_LIMIT = 100
FORBIDDEN_AGGREGATE_STAGES = {"$out", "$merge"}
DEFAULT_KEYWORD_SOURCE_SLUG = "search-terms"
DEFAULT_KEYWORD_SOURCE_NAME = "search_terms (Default)"
DEFAULT_KEYWORD_SEARCH_TERMS_COLLECTION = "search_terms"
DEFAULT_KEYWORD_RUNS_COLLECTION = "searchterms_runs"


class CollectionHint(BaseModel):
    category: str
    description: str
    recommended_for_buybox_analysis: bool


class ServerStatus(BaseModel):
    name: str
    environment: str
    auth: str
    mongo_configured: bool
    mongo_database: str | None
    inventory_mongo_database: str | None
    startup_errors: list[str]


class CollectionSummary(BaseModel):
    name: str
    estimated_count: int
    category: str
    description: str
    recommended_for_buybox_analysis: bool


class IndexSummary(BaseModel):
    name: str
    keys: dict[str, Any]


class CollectionProfile(BaseModel):
    name: str
    estimated_count: int
    category: str
    description: str
    recommended_for_buybox_analysis: bool
    sample_top_level_fields: list[str]
    sample_nested_fields: list[str]
    indexes: list[IndexSummary]
    sample_documents: list[dict[str, Any]]


class ScopedCollectionSummary(BaseModel):
    name: str
    estimated_count: int
    category: str
    description: str
    recommended_for_scope_analysis: bool


class ScopedCollectionProfile(BaseModel):
    name: str
    estimated_count: int
    category: str
    description: str
    recommended_for_scope_analysis: bool
    sample_top_level_fields: list[str]
    sample_nested_fields: list[str]
    indexes: list[IndexSummary]
    sample_documents: list[dict[str, Any]]


class SortField(BaseModel):
    field: str = Field(
        description=(
            "Field path to sort on, for example received_at or offer.seller.name. "
            "Use this inside a sort object, not as a raw string."
        )
    )
    direction: Literal[1, -1] = Field(
        default=1,
        description="Use 1 for ascending order and -1 for descending order.",
    )


class QueryResult(BaseModel):
    collection: str
    returned_count: int
    limit_applied: int
    documents: list[dict[str, Any]]


class DistinctResult(BaseModel):
    collection: str
    field: str
    returned_count: int
    limit_applied: int
    values: list[Any]


class AggregateResult(BaseModel):
    collection: str
    returned_count: int
    limit_applied: int
    pipeline_executed: list[dict[str, Any]]
    documents: list[dict[str, Any]]


class ResolvedSource(BaseModel):
    query: str
    source_slug: str
    source_name: str
    offers_collection: str | None
    runs_collection: str | None
    matched_on: str
    alternatives_considered: list[str]


class KeywordTrackingSourceSummary(BaseModel):
    source_slug: str
    source_name: str
    search_terms_collection: str
    runs_collection: str | None = None
    tracked_keyword_count: int
    latest_snapshot_received_at: str | None = None


class ResolvedKeywordSource(BaseModel):
    query: str
    source_slug: str
    source_name: str
    search_terms_collection: str
    runs_collection: str | None = None
    matched_on: str
    alternatives_considered: list[str]


class ResolvedTrackedKeyword(BaseModel):
    query: str
    keyword: str
    source_slug: str
    source_name: str
    search_terms_collection: str
    is_active: bool
    refreshed_at: str | None = None
    matched_on: str
    alternatives_considered: list[str]


class ResolvedTrackedAsin(BaseModel):
    query: str
    asin: str
    matched_on: str
    alternatives_considered: list[str]


class KeywordSearchResultRow(BaseModel):
    asin: str
    position: int | None = None
    price: Any | None = None
    rating: Any | None = None
    ratings_total: int | None = None
    sponsored: bool | None = None
    tracked_asin: bool


class KeywordSnapshotSummary(BaseModel):
    source: ResolvedKeywordSource
    resolved_keyword: ResolvedTrackedKeyword | None = None
    search_term: str
    received_at: str
    result_count: int
    tracked_result_count: int
    top_results: list[KeywordSearchResultRow]
    notes: list[str]
    collections_used: list[str]
    key_fields_used: list[str]


class KeywordRankHistoryPoint(BaseModel):
    received_at: str
    present: bool
    position: int | None = None
    price: Any | None = None
    rating: Any | None = None
    ratings_total: int | None = None
    sponsored: bool | None = None


class KeywordRankHistorySummary(BaseModel):
    source: ResolvedKeywordSource
    resolved_keyword: ResolvedTrackedKeyword | None = None
    resolved_asin: ResolvedTrackedAsin | None = None
    search_term: str
    asin: str
    latest_position: int | None = None
    best_position: int | None = None
    worst_position: int | None = None
    appearance_count: int
    snapshot_count: int
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    history: list[KeywordRankHistoryPoint]
    notes: list[str]
    collections_used: list[str]
    key_fields_used: list[str]


class KeywordSearchQueryVolumeRow(BaseModel):
    keyword: str
    source_slug: str
    source_name: str | None = None
    search_query_volume: int
    uploaded_at: str | None = None


class KeywordSearchQueryVolumeSummary(BaseModel):
    source: ResolvedKeywordSource | None = None
    resolved_keyword: ResolvedTrackedKeyword | None = None
    records: list[KeywordSearchQueryVolumeRow]
    notes: list[str]
    collections_used: list[str]
    key_fields_used: list[str]


class BuyBoxRunSnapshot(BaseModel):
    received_at: str
    result_set_id: int | None = None
    observed_cell_count: int | None = None
    missing_cell_count: int | None = None
    total_cells: int
    our_wins: int
    our_share: float


class BuyBoxZipcodeSummary(BaseModel):
    zipcode: str
    city: str | None = None
    state: str | None = None
    gains: int = 0
    losses: int = 0
    net: int = 0
    previous_total: int | None = None
    latest_total: int | None = None
    previous_wins: int | None = None
    latest_wins: int | None = None
    previous_share: float | None = None
    latest_share: float | None = None
    share_change_pp: float | None = None


class BuyBoxCompetitorChange(BaseModel):
    competitor: str
    gains_from_competitor: int
    losses_to_competitor: int
    net: int


class BuyBoxStatusSummary(BaseModel):
    source: ResolvedSource
    canonical_seller_name: str
    status_direction: Literal["gaining", "losing", "mixed", "flat"]
    latest_run: BuyBoxRunSnapshot
    previous_run: BuyBoxRunSnapshot
    recent_trend: list[BuyBoxRunSnapshot]
    raw_win_delta: int
    share_delta_points: float
    overlap_cells_compared: int
    overlap_gains: int
    overlap_losses: int
    overlap_net_change: int
    top_gaining_zipcodes: list[BuyBoxZipcodeSummary]
    top_losing_zipcodes: list[BuyBoxZipcodeSummary]
    weakest_current_zipcodes: list[BuyBoxZipcodeSummary]
    competitor_changes: list[BuyBoxCompetitorChange]
    notes: list[str]
    collections_used: list[str]
    key_fields_used: list[str]


class ResolvedInventoryLocation(BaseModel):
    query: str
    location_id: str
    location_name: str
    amazon_sku_suffix: str | None = None
    handling_time: int | None = None
    last_successful_ingestion_at: str | None = None
    matched_on: str
    alternatives_considered: list[str]


class ResolvedInventorySku(BaseModel):
    query: str
    sku: str
    name: str
    sku_type: str
    aliases: list[str]
    child_asins: list[str]
    is_indirect_component: bool | None = None
    component_count: int = 0
    matched_on: str
    alternatives_considered: list[str]


class InventorySkuComponent(BaseModel):
    component_sku: str
    component_name: str | None = None
    required_quantity: int
    is_indirect_component: bool | None = None


class InventoryComponentAvailability(BaseModel):
    component_sku: str
    component_name: str | None = None
    required_quantity: int
    on_hand_quantity: int
    build_limit: int
    is_indirect_component: bool | None = None


class InventoryLocationQuantity(BaseModel):
    location_id: str
    location_name: str
    amazon_sku_suffix: str | None = None
    handling_time: int | None = None
    last_successful_ingestion_at: str | None = None
    has_recorded_row: bool
    recorded_quantity: int
    buildable_quantity: int | None = None
    limiting_component_sku: str | None = None
    limiting_component_name: str | None = None
    component_breakdown: list[InventoryComponentAvailability] = Field(default_factory=list)


class InventorySkuExplanation(BaseModel):
    resolved_sku: ResolvedInventorySku
    selected_location: ResolvedInventoryLocation | None = None
    components: list[InventorySkuComponent]
    parent_skus: list[ResolvedInventorySku]
    quantities: list[InventoryLocationQuantity]
    location_count_with_inventory_rows: int
    positive_location_count: int
    notes: list[str]


class InventoryQuantitySummary(BaseModel):
    resolved_sku: ResolvedInventorySku
    selected_location: ResolvedInventoryLocation | None = None
    quantities: list[InventoryLocationQuantity]
    total_recorded_quantity: int
    total_buildable_quantity: int | None = None
    locations_with_recorded_stock: int
    locations_with_buildable_stock: int | None = None
    notes: list[str]


class InventoryParentSkuAvailability(BaseModel):
    parent_sku: str
    parent_name: str
    parent_sku_type: str
    required_quantity_of_component: int
    child_asins: list[str]
    total_recorded_quantity: int
    total_buildable_quantity: int | None = None
    locations_with_recorded_stock: int
    locations_with_buildable_stock: int | None = None


class InventoryParentSkuSummary(BaseModel):
    resolved_component: ResolvedInventorySku
    selected_location: ResolvedInventoryLocation | None = None
    parent_skus: list[InventoryParentSkuAvailability]
    notes: list[str]


class InventoryAsinAvailabilityRow(BaseModel):
    asin: str
    location_id: str
    location_name: str
    amazon_sku_suffix: str | None = None
    handling_time: int | None = None
    last_successful_ingestion_at: str | None = None
    amazon_sku_alias: str | None = None
    resolved_sku: str | None = None
    resolved_sku_name: str | None = None
    resolved_sku_type: str | None = None
    recorded_quantity: int | None = None
    buildable_quantity: int | None = None
    alias_match: bool = False
    child_asin_match: bool = False


class InventoryAsinAvailabilitySummary(BaseModel):
    asin: str
    selected_location: ResolvedInventoryLocation | None = None
    matches: list[InventoryAsinAvailabilityRow]
    notes: list[str]


class InventoryUncoveredSkuGap(BaseModel):
    location_id: str
    location_name: str
    amazon_sku_suffix: str | None = None
    last_successful_ingestion_at: str | None = None
    observed_sku: str
    latest_quantity: int
    latest_observed_at: str
    source_method: str | None = None


class InventoryUncoveredGapSummary(BaseModel):
    selected_location: ResolvedInventoryLocation | None = None
    gaps: list[InventoryUncoveredSkuGap]
    notes: list[str]


class InventoryLocationFreshness(BaseModel):
    location_id: str
    location_name: str
    amazon_sku_suffix: str | None = None
    handling_time: int | None = None
    last_successful_ingestion_at: str | None = None
    inventory_row_count: int
    positive_inventory_row_count: int
    total_recorded_quantity: int
    distinct_uncovered_skus: int
    positive_uncovered_skus: int


class InventoryFreshnessSummary(BaseModel):
    selected_location: ResolvedInventoryLocation | None = None
    locations: list[InventoryLocationFreshness]
    notes: list[str]


class FedexRateOffer(BaseModel):
    service_type: str
    service_name: str
    account_rate_usd: float | None = None
    list_rate_usd: float | None = None
    currency: str | None = None
    committed_delivery_at: str | None = None
    committed_delivery_dow: str | None = None
    saturday_delivery: bool | None = None
    destination_airport_id: str | None = None
    money_back_guarantee_eligible: bool | None = None


class FedexRateQuoteSummary(BaseModel):
    api_base: str
    account_number: str
    origin_postal_code: str
    destination_postal_code: str
    ship_date: str | None
    weight_lb: float
    dimensions_in: dict[str, float] | None = None
    pickup_type: str
    packaging_type: str
    services_returned: int
    offers: list[FedexRateOffer]
    notes: list[str]


KNOWN_COLLECTION_HINTS: dict[str, CollectionHint] = {
    "bb2_offers": CollectionHint(
        category="buybox",
        description=(
            "Main production buy-box fact table. Each document is one offer observation for an ASIN, "
            "source_slug, zipcode, and received_at timestamp. For a single cell, defined by "
            "tracking_key plus received_at, the buy-box winner is the row with the lowest offer_index. "
            "Use this collection for winner changes, seller shifts, price comparisons, Prime/FBA status, "
            "and delivery analysis."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "bb2_offer_runs": CollectionHint(
        category="buybox",
        description=(
            "Run-level metadata for buy-box scrapes. Use this to find the latest completed runs, "
            "source coverage, batch timing, and stored counts. For run-over-run analysis, compare "
            "bb2_offer_runs coverage first, then analyze winner changes in bb2_offers. Do not assume "
            "runs exist for every calendar date; cadence can vary by source and may be daily, weekly, "
            "or triggered on demand."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "bb2_offer_missing_cells": CollectionHint(
        category="buybox",
        description=(
            "Diagnostics for offer rows that could not be fully normalized. Useful for coverage gaps, "
            "parsing failures, and explaining why expected offer fields are missing."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "bb2_offer_sources": CollectionHint(
        category="buybox",
        description=(
            "Registry of configured buy-box sources. Use this to discover valid source_slug values "
            "instead of guessing brand or collection slugs."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "bb2_offer_settings": CollectionHint(
        category="config",
        description=(
            "Application-level settings for the buy-box scraper, including the canonical internal seller "
            "name in our_seller_name. Read this before answering questions like 'are we winning the buy box?'."
        ),
        recommended_for_buybox_analysis=False,
    ),
    "bb2_zipcode_locations": CollectionHint(
        category="reference",
        description=(
            "Reference table mapping tracked zipcodes to location metadata such as city, state, and coordinates. "
            "Use this to translate zipcode-level changes into readable geography."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "search_terms": CollectionHint(
        category="search_terms",
        description="Legacy search-term result collection with search results stored by term and receive time.",
        recommended_for_buybox_analysis=False,
    ),
    "searchterms_runs": CollectionHint(
        category="search_terms",
        description=(
            "Run-level metadata for legacy search-term collection loads. Use this to confirm which dates "
            "actually exist before assuming a daily history."
        ),
        recommended_for_buybox_analysis=False,
    ),
    "kw_sources": CollectionHint(
        category="keywords",
        description="Registry of keyword and search-term sources.",
        recommended_for_buybox_analysis=False,
    ),
    "kw_groups": CollectionHint(
        category="keywords",
        description="Keyword grouping metadata.",
        recommended_for_buybox_analysis=False,
    ),
    "kw_group_assignments": CollectionHint(
        category="keywords",
        description="Assignments from search terms into keyword groups.",
        recommended_for_buybox_analysis=False,
    ),
    "kw_tracked_keywords": CollectionHint(
        category="keywords",
        description="Tracked keyword list with source metadata and freshness flags.",
        recommended_for_buybox_analysis=False,
    ),
    "kw_tracked_asins": CollectionHint(
        category="keywords",
        description="Tracked ASIN list used in keyword/search-term workflows.",
        recommended_for_buybox_analysis=False,
    ),
    "kw_search_query_volumes": CollectionHint(
        category="keywords",
        description="Uploaded search query volume data keyed by keyword and source.",
        recommended_for_buybox_analysis=False,
    ),
    "rainforest_runs": CollectionHint(
        category="ingest",
        description="Run metadata for Rainforest-sourced imports.",
        recommended_for_buybox_analysis=False,
    ),
    "healthchecks": CollectionHint(
        category="ops",
        description="Operational health-check collection.",
        recommended_for_buybox_analysis=False,
    ),
}

KEYWORD_QUERY_CATEGORIES = {"keywords", "search_terms"}

INVENTORY_COLLECTION_HINTS: dict[str, CollectionHint] = {
    "locations": CollectionHint(
        category="inventory_reference",
        description=(
            "Warehouse and fulfillment-location metadata, including location name, Amazon SKU suffix, "
            "handling time, and ingestion freshness markers."
        ),
        recommended_for_buybox_analysis=False,
    ),
    "skus": CollectionHint(
        category="inventory_catalog",
        description=(
            "Canonical SKU catalog with aliases, child ASINs, SKU type, and kitComponents bill-of-materials metadata."
        ),
        recommended_for_buybox_analysis=False,
    ),
    "inventorylevels": CollectionHint(
        category="inventory_levels",
        description=(
            "Recorded inventory quantities by locationId and sku. This is the main fact table for direct on-hand stock."
        ),
        recommended_for_buybox_analysis=False,
    ),
    "amazonskualiases": CollectionHint(
        category="inventory_reference",
        description=(
            "Location-specific mappings between Amazon-facing SKU aliases and internal SKU identities."
        ),
        recommended_for_buybox_analysis=False,
    ),
    "uncoveredskuobservations": CollectionHint(
        category="inventory_diagnostics",
        description=(
            "Gap and ingestion-diagnostic rows for observed SKUs that do not map cleanly into the modeled SKU catalog."
        ),
        recommended_for_buybox_analysis=False,
    ),
}


def _jsonable(value: Any) -> Any:
    return json_util.loads(json_util.dumps(value))


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _slugify_text(value: str) -> str:
    normalized = _normalize_text(value)
    return re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")


def _normalize_identifier(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def _coerce_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(value)


def _coerce_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _coerce_optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    return bool(value)


def _normalize_as_of_selector(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError("Date selector must not be empty.")

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        return f"{candidate}T23:59:59.999999Z"

    if " " in candidate and "T" not in candidate:
        candidate = candidate.replace(" ", "T")

    if candidate.endswith("Z"):
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _score_source_document(
    document: dict[str, Any],
    *,
    query_text: str,
    query_slug: str,
) -> tuple[int, str] | None:
    name = _normalize_text(str(document.get("name", "")))
    slug = _normalize_text(str(document.get("slug", "")))
    slug_lower = _normalize_text(str(document.get("slug_lower", slug)))
    webhook_slug = _normalize_text(str(document.get("webhook_slug", "")))
    webhook_slug_lower = _normalize_text(str(document.get("webhook_slug_lower", webhook_slug)))

    if query_slug and slug_lower == query_slug:
        return 100, "slug_exact"
    if query_slug and webhook_slug_lower == query_slug:
        return 95, "webhook_slug_exact"
    if query_text and name == query_text:
        return 90, "name_exact"
    if query_slug and slug_lower.startswith(query_slug):
        return 80, "slug_prefix"
    if query_slug and webhook_slug_lower.startswith(query_slug):
        return 78, "webhook_slug_prefix"
    if query_text and name.startswith(query_text):
        return 76, "name_prefix"
    if query_slug and query_slug in slug_lower:
        return 70, "slug_contains"
    if query_text and query_text in name:
        return 68, "name_contains"
    return None


def _winner_per_cell_pipeline(match_filter: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"$match": match_filter},
        {"$sort": {"tracking_key": 1, "received_at": 1, "offer_index": 1}},
        {
            "$group": {
                "_id": {"tracking_key": "$tracking_key", "received_at": "$received_at"},
                "tracking_key": {"$first": "$tracking_key"},
                "received_at": {"$first": "$received_at"},
                "zipcode": {"$first": "$zipcode"},
                "asin": {"$first": "$asin"},
                "winner_seller_name": {"$first": "$seller_name"},
                "winner_offer_index": {"$first": "$offer_index"},
            }
        },
    ]


def _flatten_keys(document: dict[str, Any], prefix: str = "", depth: int = MAX_FIELD_DEPTH) -> set[str]:
    keys: set[str] = set()
    if depth < 0:
        return keys

    for key, value in document.items():
        path = f"{prefix}.{key}" if prefix else key
        keys.add(path)
        if isinstance(value, dict):
            keys.update(_flatten_keys(value, prefix=path, depth=depth - 1))
    return keys


def _collection_hint(name: str) -> CollectionHint:
    if name in KNOWN_COLLECTION_HINTS:
        return KNOWN_COLLECTION_HINTS[name]
    if name.startswith("source-") and name.endswith("-search_terms"):
        return CollectionHint(
            category="search_terms",
            description=(
                "Source-specific search-term result collection. Documents usually contain a search term, "
                "search results payload, and receive time."
            ),
            recommended_for_buybox_analysis=False,
        )
    if name.startswith("source-") and name.endswith("-searchterms_runs"):
        return CollectionHint(
            category="search_terms",
            description="Source-specific run metadata for search-term ingestion batches.",
            recommended_for_buybox_analysis=False,
        )
    if "offers" in name.lower() or "asin" in name.lower():
        return CollectionHint(
            category="legacy_buybox",
            description=(
                "Legacy or experimental product/offer snapshot collection. Do not use this for current "
                "production buy-box status reporting unless the question explicitly targets the legacy dataset. "
                "Prefer bb2_offers for current analysis."
            ),
            recommended_for_buybox_analysis=False,
        )
    return CollectionHint(
        category="unclassified",
        description="Unclassified collection. Use buybox_featured_offer_percent_inspect_collection to understand its structure before querying it.",
        recommended_for_buybox_analysis=False,
    )


def _inventory_collection_hint(name: str) -> CollectionHint:
    if name in INVENTORY_COLLECTION_HINTS:
        return INVENTORY_COLLECTION_HINTS[name]
    return CollectionHint(
        category="unclassified",
        description=(
            "Unclassified inventory collection. Use inventory_by_location_inspect_collection to understand its structure "
            "before querying it."
        ),
        recommended_for_buybox_analysis=False,
    )


def _is_keyword_tracking_collection_name(name: str) -> bool:
    return _collection_hint(name).category in KEYWORD_QUERY_CATEGORIES


def _is_inventory_query_collection_name(name: str) -> bool:
    return name in INVENTORY_COLLECTION_HINTS


def _normalize_limit(limit: int, *, max_limit: int) -> int:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    return min(limit, max_limit)


def _normalize_sort(sort: list[SortField] | None) -> list[tuple[str, int]] | None:
    if not sort:
        return None
    return [(item.field, item.direction) for item in sort]


def _validate_pipeline(pipeline: list[dict[str, Any]], *, tool_name: str) -> None:
    if not pipeline:
        raise ValueError("pipeline must contain at least one stage")
    for stage in pipeline:
        if not isinstance(stage, dict) or len(stage) != 1:
            raise ValueError("each pipeline stage must be an object with exactly one operator")
        operator = next(iter(stage))
        if operator in FORBIDDEN_AGGREGATE_STAGES:
            raise ValueError(f"{operator} is not allowed in {tool_name}")


def _ensure_limit_stage(pipeline: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    normalized = [dict(stage) for stage in pipeline]
    last_stage = normalized[-1]
    if "$limit" in last_stage:
        try:
            last_stage["$limit"] = _normalize_limit(int(last_stage["$limit"]), max_limit=MAX_AGGREGATE_LIMIT)
        except Exception as exc:
            raise ValueError("existing $limit stage must contain an integer value") from exc
        return normalized
    normalized.append({"$limit": limit})
    return normalized


def _preferred_sample_sort(name: str, sample_doc: dict[str, Any] | None) -> list[tuple[str, int]] | None:
    if sample_doc and "received_at" in sample_doc:
        return [("received_at", -1)]
    if sample_doc and "updated_at" in sample_doc:
        return [("updated_at", -1)]
    if name.endswith("_runs") or name == "bb2_offer_runs":
        return [("_id", -1)]
    return None


async def _build_collection_profile_details(
    name: str,
    collection: Any,
    *,
    hint: CollectionHint,
    sample_size: int = 2,
) -> dict[str, Any]:
    normalized_sample_size = _normalize_limit(sample_size, max_limit=MAX_SAMPLE_SIZE)

    estimated_count = await collection.estimated_document_count()
    first_doc = await collection.find_one()
    sample_sort = _preferred_sample_sort(name, first_doc)
    cursor = collection.find({}, limit=normalized_sample_size, sort=sample_sort)
    sample_documents = await cursor.to_list(length=normalized_sample_size)

    top_level_fields: set[str] = set()
    nested_fields: set[str] = set()
    for document in sample_documents or ([] if first_doc is None else [first_doc]):
        top_level_fields.update(document.keys())
        nested_fields.update(_flatten_keys(document))

    index_cursor = await collection.list_indexes()
    indexes = await index_cursor.to_list(length=None)

    return {
        "name": name,
        "estimated_count": estimated_count,
        "category": hint.category,
        "description": hint.description,
        "recommended_for_scope_analysis": hint.recommended_for_buybox_analysis,
        "sample_top_level_fields": sorted(top_level_fields),
        "sample_nested_fields": sorted(nested_fields),
        "indexes": [
            IndexSummary(
                name=str(index.get("name")),
                keys=_jsonable(index.get("key", {})),
            )
            for index in indexes
        ],
        "sample_documents": _jsonable(sample_documents),
    }


async def _execute_find_query(
    collection: Any,
    *,
    filter: dict[str, Any],
    projection: dict[str, Any] | None,
    sort: list[SortField] | None,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_limit = _normalize_limit(limit, max_limit=MAX_FIND_LIMIT)
    cursor = collection.find(
        filter or {},
        projection=projection,
        sort=_normalize_sort(sort),
        limit=normalized_limit,
        max_time_ms=15_000,
    )
    return _jsonable(await cursor.to_list(length=normalized_limit))


async def _execute_distinct_query(
    collection: Any,
    *,
    field_name: str,
    filter: dict[str, Any],
    limit: int,
) -> list[Any]:
    normalized_limit = _normalize_limit(limit, max_limit=MAX_DISTINCT_LIMIT)
    values = await collection.distinct(field_name, filter=filter or {}, maxTimeMS=15_000)
    return _jsonable(values)[:normalized_limit]


async def _execute_aggregate_query(
    collection: Any,
    *,
    pipeline: list[dict[str, Any]],
    limit: int,
    tool_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    normalized_limit = _normalize_limit(limit, max_limit=MAX_AGGREGATE_LIMIT)
    _validate_pipeline(pipeline, tool_name=tool_name)
    pipeline_to_run = _ensure_limit_stage(pipeline, normalized_limit)
    cursor = await collection.aggregate(
        pipeline_to_run,
        allowDiskUse=True,
        maxTimeMS=20_000,
    )
    documents = _jsonable(await cursor.to_list(length=normalized_limit))
    return pipeline_to_run, documents, normalized_limit


def _require_runtime() -> ApplicationRuntime:
    runtime = get_runtime()
    if runtime.mongo_database is None:
        raise RuntimeError(
            "MongoDB is not configured. Set MONGODB_URI or BUYBOX_MCP_MONGO_URI and a database name."
        )
    return runtime


async def _require_collection(name: str) -> tuple[ApplicationRuntime, Any]:
    runtime = _require_runtime()
    available = await runtime.mongo_database.list_collection_names()
    if name not in available:
        raise ValueError(f"Unknown collection: {name}. Use buybox_featured_offer_percent_list_collections first.")
    return runtime, runtime.mongo_database[name]


async def _build_collection_profile(name: str, sample_size: int = 2) -> CollectionProfile:
    _, collection = await _require_collection(name)
    hint = _collection_hint(name)
    details = await _build_collection_profile_details(
        name,
        collection,
        hint=hint,
        sample_size=sample_size,
    )
    return CollectionProfile(
        name=details["name"],
        estimated_count=details["estimated_count"],
        category=details["category"],
        description=details["description"],
        recommended_for_buybox_analysis=hint.recommended_for_buybox_analysis,
        sample_top_level_fields=details["sample_top_level_fields"],
        sample_nested_fields=details["sample_nested_fields"],
        indexes=details["indexes"],
        sample_documents=details["sample_documents"],
    )


async def _run_aggregate_query(
    collection_name: str,
    pipeline: list[dict[str, Any]],
    *,
    limit: int = MAX_AGGREGATE_LIMIT,
) -> list[dict[str, Any]]:
    _, collection = await _require_collection(collection_name)
    _, documents, _ = await _execute_aggregate_query(
        collection,
        pipeline=pipeline,
        limit=limit,
        tool_name="buybox_featured_offer_percent_aggregate_documents",
    )
    return documents


async def _fetch_our_seller_name() -> str:
    _, collection = await _require_collection("bb2_offer_settings")
    document = await collection.find_one({"_id": "global"}, projection={"_id": 0, "our_seller_name": 1})
    if not document or not document.get("our_seller_name"):
        raise ValueError("Could not resolve our canonical seller identity from bb2_offer_settings.our_seller_name.")
    return str(document["our_seller_name"])


async def _fetch_completed_runs(source_slug: str, *, limit: int) -> list[dict[str, Any]]:
    _, collection = await _require_collection("bb2_offer_runs")
    filter_doc: dict[str, Any] = {"source_slug": source_slug, "status": "completed"}
    cursor = collection.find(
        filter_doc,
        projection={
            "_id": 0,
            "source_slug": 1,
            "source_name": 1,
            "received_at": 1,
            "result_set.id": 1,
            "observed_cell_count": 1,
            "missing_cell_count": 1,
        },
        sort=[("received_at", -1)],
        limit=_normalize_limit(limit, max_limit=25),
        max_time_ms=15_000,
    )
    return _jsonable(await cursor.to_list(length=limit))


async def _fetch_completed_runs_up_to(
    source_slug: str,
    *,
    latest_received_at_or_before: str,
    limit: int,
) -> list[dict[str, Any]]:
    _, collection = await _require_collection("bb2_offer_runs")
    cursor = collection.find(
        {
            "source_slug": source_slug,
            "status": "completed",
            "received_at": {"$lte": latest_received_at_or_before},
        },
        projection={
            "_id": 0,
            "source_slug": 1,
            "source_name": 1,
            "received_at": 1,
            "result_set.id": 1,
            "observed_cell_count": 1,
            "missing_cell_count": 1,
        },
        sort=[("received_at", -1)],
        limit=_normalize_limit(limit, max_limit=25),
        max_time_ms=15_000,
    )
    return _jsonable(await cursor.to_list(length=limit))


async def _fetch_completed_run_at_or_before(source_slug: str, *, selector: str) -> dict[str, Any] | None:
    runs = await _fetch_completed_runs_up_to(
        source_slug,
        latest_received_at_or_before=selector,
        limit=1,
    )
    return runs[0] if runs else None


async def _fetch_completed_run_before(source_slug: str, *, received_at: str) -> dict[str, Any] | None:
    _, collection = await _require_collection("bb2_offer_runs")
    document = await collection.find_one(
        {
            "source_slug": source_slug,
            "status": "completed",
            "received_at": {"$lt": received_at},
        },
        projection={
            "_id": 0,
            "source_slug": 1,
            "source_name": 1,
            "received_at": 1,
            "result_set.id": 1,
            "observed_cell_count": 1,
            "missing_cell_count": 1,
        },
        sort=[("received_at", -1)],
        max_time_ms=15_000,
    )
    return _jsonable(document) if document else None


async def _resolve_source_query(source_query: str) -> ResolvedSource:
    query_text = _normalize_text(source_query)
    query_slug = _slugify_text(source_query)
    if not query_text and not query_slug:
        raise ValueError("source_query must not be empty")

    _, collection = await _require_collection("bb2_offer_sources")
    documents = _jsonable(
        await collection.find(
            {},
            projection={
                "_id": 0,
                "name": 1,
                "slug": 1,
                "slug_lower": 1,
                "webhook_slug": 1,
                "webhook_slug_lower": 1,
                "offers_collection": 1,
                "runs_collection": 1,
            },
            max_time_ms=15_000,
        ).to_list(length=100)
    )

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for document in documents:
        score = _score_source_document(document, query_text=query_text, query_slug=query_slug)
        if score is not None:
            ranked.append((score[0], score[1], document))

    if not ranked:
        available = sorted(str(document.get("slug", "")) for document in documents if document.get("slug"))
        raise ValueError(
            "No buy-box source matched source_query. Use buybox_featured_offer_percent_resolve_source or bb2_offer_sources to inspect valid sources. "
            f"Available slugs: {available}"
        )

    ranked.sort(key=lambda item: (-item[0], str(item[2].get("slug", ""))))
    top_score = ranked[0][0]
    if top_score >= 90:
        candidate_pool = [item for item in ranked if item[0] == top_score]
    else:
        candidate_pool = [item for item in ranked if item[0] >= top_score - 12]

    if len(candidate_pool) > 1:
        candidates_with_runs: list[tuple[str, str, dict[str, Any], dict[str, Any] | None]] = []
        for _, matched_on, document in candidate_pool:
            source_slug = str(document.get("slug", ""))
            latest_runs = await _fetch_completed_runs(source_slug, limit=1)
            latest_run = latest_runs[0] if latest_runs else None
            candidates_with_runs.append((matched_on, source_slug, document, latest_run))

        candidates_with_runs.sort(
            key=lambda item: (
                item[3].get("received_at", "") if item[3] else "",
                item[3].get("observed_cell_count", -1) if item[3] else -1,
                item[1],
            ),
            reverse=True,
        )
        matched_on, _, chosen_document, _ = candidates_with_runs[0]
        alternatives = [str(item[2].get("slug", "")) for item in candidates_with_runs]
    else:
        matched_on, chosen_document = candidate_pool[0][1], candidate_pool[0][2]
        alternatives = [str(item[2].get("slug", "")) for item in ranked[:5]]

    return ResolvedSource(
        query=source_query,
        source_slug=str(chosen_document.get("slug", "")),
        source_name=str(chosen_document.get("name", "")),
        offers_collection=chosen_document.get("offers_collection"),
        runs_collection=chosen_document.get("runs_collection"),
        matched_on=matched_on,
        alternatives_considered=alternatives,
    )


def _build_default_keyword_source_document(available_collections: set[str]) -> dict[str, Any] | None:
    if DEFAULT_KEYWORD_SEARCH_TERMS_COLLECTION not in available_collections:
        return None
    return {
        "name": DEFAULT_KEYWORD_SOURCE_NAME,
        "slug": DEFAULT_KEYWORD_SOURCE_SLUG,
        "slug_lower": DEFAULT_KEYWORD_SOURCE_SLUG,
        "webhook_slug": DEFAULT_KEYWORD_SOURCE_SLUG,
        "webhook_slug_lower": DEFAULT_KEYWORD_SOURCE_SLUG,
        "search_terms_collection": DEFAULT_KEYWORD_SEARCH_TERMS_COLLECTION,
        "runs_collection": (
            DEFAULT_KEYWORD_RUNS_COLLECTION if DEFAULT_KEYWORD_RUNS_COLLECTION in available_collections else None
        ),
    }


def _build_resolved_keyword_source(
    query: str,
    document: dict[str, Any],
    *,
    matched_on: str,
    alternatives: list[str],
) -> ResolvedKeywordSource:
    return ResolvedKeywordSource(
        query=query,
        source_slug=str(document.get("slug", "")),
        source_name=str(document.get("name", "")),
        search_terms_collection=str(document.get("search_terms_collection", "")),
        runs_collection=_stringify_optional(document.get("runs_collection")),
        matched_on=matched_on,
        alternatives_considered=alternatives,
    )


def _build_resolved_tracked_keyword(
    query: str,
    document: dict[str, Any],
    *,
    matched_on: str,
    alternatives: list[str],
) -> ResolvedTrackedKeyword:
    return ResolvedTrackedKeyword(
        query=query,
        keyword=str(document.get("keyword", "")),
        source_slug=str(document.get("source_slug", "")),
        source_name=str(document.get("source_name", "")),
        search_terms_collection=str(document.get("search_terms_collection", "")),
        is_active=bool(document.get("is_active")),
        refreshed_at=_stringify_optional(document.get("refreshed_at")),
        matched_on=matched_on,
        alternatives_considered=alternatives,
    )


def _keyword_source_sort_key(document: dict[str, Any]) -> tuple[str, str]:
    return (str(document.get("slug", "")), str(document.get("name", "")))


def _score_keyword_source_document(
    document: dict[str, Any],
    *,
    query_text: str,
    query_slug: str,
) -> tuple[int, str] | None:
    name = _normalize_text(str(document.get("name", "")))
    slug = _normalize_text(str(document.get("slug", "")))
    slug_lower = _normalize_text(str(document.get("slug_lower", slug)))
    webhook_slug = _normalize_text(str(document.get("webhook_slug", "")))
    webhook_slug_lower = _normalize_text(str(document.get("webhook_slug_lower", webhook_slug)))
    search_terms_collection = _normalize_text(str(document.get("search_terms_collection", "")))

    if query_slug and slug_lower == query_slug:
        return 100, "slug_exact"
    if query_slug and webhook_slug_lower == query_slug:
        return 97, "webhook_slug_exact"
    if query_text and name == query_text:
        return 95, "name_exact"
    if query_text and search_terms_collection == query_text:
        return 92, "collection_exact"
    if query_slug and slug_lower.startswith(query_slug):
        return 86, "slug_prefix"
    if query_text and name.startswith(query_text):
        return 84, "name_prefix"
    if query_text and query_text in name:
        return 76, "name_contains"
    if query_slug and query_slug in slug_lower:
        return 74, "slug_contains"
    if query_text and query_text in search_terms_collection:
        return 72, "collection_contains"
    return None


def _score_tracked_keyword_document(
    document: dict[str, Any],
    *,
    query_text: str,
) -> tuple[int, str] | None:
    keyword = _normalize_text(str(document.get("keyword_lower", document.get("keyword", ""))))

    if query_text and keyword == query_text:
        return 120, "keyword_exact"
    if query_text and keyword.startswith(query_text):
        return 110, "keyword_prefix"
    if query_text and query_text in keyword:
        return 100, "keyword_contains"
    return None


def _score_tracked_asin_document(
    document: dict[str, Any],
    *,
    query_text: str,
) -> tuple[int, str] | None:
    asin = str(document.get("asin", "")).strip().upper()

    if query_text and asin == query_text:
        return 120, "asin_exact"
    if query_text and asin.startswith(query_text):
        return 110, "asin_prefix"
    if query_text and query_text in asin:
        return 100, "asin_contains"
    return None


def _search_term_exact_regex(search_term_query: str) -> re.Pattern[str]:
    candidate = search_term_query.strip()
    if not candidate:
        raise ValueError("search_term_query must not be empty")
    return re.compile(rf"^{re.escape(candidate)}$", re.IGNORECASE)


async def _fetch_keyword_source_documents() -> list[dict[str, Any]]:
    runtime = _require_runtime()
    available_collections = set(await runtime.mongo_database.list_collection_names())
    _, collection = await _require_collection("kw_sources")
    documents = _jsonable(
        await collection.find(
            {},
            projection={
                "_id": 0,
                "name": 1,
                "slug": 1,
                "slug_lower": 1,
                "webhook_slug": 1,
                "webhook_slug_lower": 1,
                "search_terms_collection": 1,
                "runs_collection": 1,
            },
            max_time_ms=15_000,
        ).to_list(length=MAX_KEYWORD_SOURCE_COUNT)
    )
    if not any(str(document.get("slug", "")) == DEFAULT_KEYWORD_SOURCE_SLUG for document in documents):
        default_document = _build_default_keyword_source_document(available_collections)
        if default_document is not None:
            documents.append(default_document)
    return documents


async def _fetch_keyword_tracked_keyword_counts_by_source() -> dict[str, int]:
    _, collection = await _require_collection("kw_tracked_keywords")
    cursor = await collection.aggregate(
        [
            {"$group": {"_id": "$source_slug", "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "source_slug": "$_id", "count": 1}},
        ],
        allowDiskUse=True,
        maxTimeMS=15_000,
    )
    documents = _jsonable(await cursor.to_list(length=MAX_KEYWORD_SOURCE_COUNT))
    return {
        str(document.get("source_slug", "")): _coerce_int(document.get("count", 0))
        for document in documents
        if document.get("source_slug")
    }


async def _fetch_latest_snapshot_received_at(collection_name: str) -> str | None:
    try:
        _, collection = await _require_collection(collection_name)
    except ValueError:
        return None
    document = await collection.find_one(
        {},
        projection={"_id": 0, "received_at": 1},
        sort=[("received_at", -1)],
        max_time_ms=15_000,
    )
    if not document or not document.get("received_at"):
        return None
    return str(document["received_at"])


async def _list_keyword_tracking_sources() -> list[KeywordTrackingSourceSummary]:
    source_documents = await _fetch_keyword_source_documents()
    tracked_keyword_counts = await _fetch_keyword_tracked_keyword_counts_by_source()

    results: list[KeywordTrackingSourceSummary] = []
    for document in sorted(source_documents, key=_keyword_source_sort_key):
        search_terms_collection = str(document.get("search_terms_collection", ""))
        results.append(
            KeywordTrackingSourceSummary(
                source_slug=str(document.get("slug", "")),
                source_name=str(document.get("name", "")),
                search_terms_collection=search_terms_collection,
                runs_collection=_stringify_optional(document.get("runs_collection")),
                tracked_keyword_count=tracked_keyword_counts.get(str(document.get("slug", "")), 0),
                latest_snapshot_received_at=(
                    await _fetch_latest_snapshot_received_at(search_terms_collection)
                    if search_terms_collection
                    else None
                ),
            )
        )
    return results


async def _resolve_keyword_source_query(source_query: str) -> ResolvedKeywordSource:
    query_text = _normalize_text(source_query)
    query_slug = _slugify_text(source_query)
    if not query_text and not query_slug:
        raise ValueError("source_query must not be empty")

    documents = await _fetch_keyword_source_documents()
    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for document in documents:
        score = _score_keyword_source_document(document, query_text=query_text, query_slug=query_slug)
        if score is not None:
            ranked.append((score[0], score[1], document))

    if not ranked:
        available = sorted(str(document.get("slug", "")) for document in documents if document.get("slug"))
        raise ValueError(
            "No keyword-tracking source matched source_query. "
            f"Available source slugs: {available}"
        )

    ranked.sort(key=lambda item: (-item[0], _keyword_source_sort_key(item[2])))
    top_score = ranked[0][0]
    candidate_pool = [item for item in ranked if item[0] == top_score] if top_score >= 92 else [
        item for item in ranked if item[0] >= top_score - 10
    ]

    if len(candidate_pool) > 1:
        candidates_with_latest: list[tuple[str, str, str, dict[str, Any]]] = []
        for _, matched_on, document in candidate_pool:
            latest_snapshot = await _fetch_latest_snapshot_received_at(str(document.get("search_terms_collection", "")))
            candidates_with_latest.append(
                (
                    latest_snapshot or "",
                    matched_on,
                    str(document.get("slug", "")),
                    document,
                )
            )
        candidates_with_latest.sort(reverse=True)
        _, matched_on, _, chosen_document = candidates_with_latest[0]
        alternatives = [item[2] for item in candidates_with_latest]
    else:
        matched_on, chosen_document = candidate_pool[0][1], candidate_pool[0][2]
        alternatives = [str(item[2].get("slug", "")) for item in ranked[:5]]

    return _build_resolved_keyword_source(
        source_query,
        chosen_document,
        matched_on=matched_on,
        alternatives=alternatives,
    )


async def _resolve_keyword_source_from_tracked_keyword(
    document: dict[str, Any],
    *,
    query: str,
) -> ResolvedKeywordSource:
    source_slug = str(document.get("source_slug", ""))
    source_documents = await _fetch_keyword_source_documents()
    for source_document in source_documents:
        if str(source_document.get("slug", "")) == source_slug:
            return _build_resolved_keyword_source(
                query,
                source_document,
                matched_on="tracked_keyword_source",
                alternatives=[source_slug],
            )

    fallback_document = {
        "slug": source_slug,
        "name": str(document.get("source_name", "")),
        "search_terms_collection": str(document.get("search_terms_collection", "")),
        "runs_collection": (
            DEFAULT_KEYWORD_RUNS_COLLECTION
            if str(document.get("search_terms_collection", "")) == DEFAULT_KEYWORD_SEARCH_TERMS_COLLECTION
            else None
        ),
    }
    return _build_resolved_keyword_source(
        query,
        fallback_document,
        matched_on="tracked_keyword_fallback",
        alternatives=[source_slug],
    )


async def _resolve_tracked_keyword_query(
    keyword_query: str,
    *,
    source_slug: str | None = None,
    active_only: bool = True,
) -> ResolvedTrackedKeyword:
    query_text = _normalize_text(keyword_query)
    if not query_text:
        raise ValueError("keyword_query must not be empty")

    _, collection = await _require_collection("kw_tracked_keywords")
    filter_doc: dict[str, Any] = {}
    if source_slug:
        filter_doc["source_slug"] = source_slug
    if active_only:
        filter_doc["is_active"] = True
    documents = _jsonable(
        await collection.find(
            filter_doc,
            projection={
                "_id": 0,
                "keyword": 1,
                "keyword_lower": 1,
                "source_slug": 1,
                "source_name": 1,
                "search_terms_collection": 1,
                "is_active": 1,
                "refreshed_at": 1,
            },
            max_time_ms=15_000,
        ).to_list(length=1_000)
    )

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for document in documents:
        score = _score_tracked_keyword_document(document, query_text=query_text)
        if score is not None:
            ranked.append((score[0], score[1], document))

    if not ranked:
        scope_note = f" for source_slug={source_slug}" if source_slug else ""
        raise ValueError(f"No tracked keyword matched keyword_query{scope_note}.")

    ranked.sort(key=lambda item: (str(item[2].get("keyword", "")), str(item[2].get("source_slug", ""))))
    ranked.sort(key=lambda item: str(item[2].get("refreshed_at", "")), reverse=True)
    ranked.sort(key=lambda item: 1 if item[2].get("is_active") else 0, reverse=True)
    ranked.sort(key=lambda item: item[0], reverse=True)
    top_score = ranked[0][0]
    candidate_pool = [item for item in ranked if item[0] == top_score] if top_score >= 120 else [
        item for item in ranked if item[0] >= top_score - 10
    ]
    chosen_score, matched_on, chosen_document = candidate_pool[0]
    alternatives = [
        f"{str(item[2].get('keyword', ''))} [{str(item[2].get('source_slug', ''))}]"
        for item in ranked[:5]
    ]
    if chosen_score >= 120:
        alternatives = [
            f"{str(item[2].get('keyword', ''))} [{str(item[2].get('source_slug', ''))}]"
            for item in candidate_pool
        ]

    return _build_resolved_tracked_keyword(
        keyword_query,
        chosen_document,
        matched_on=matched_on,
        alternatives=alternatives,
    )


async def _resolve_tracked_asin_query(asin_query: str) -> ResolvedTrackedAsin:
    query_text = asin_query.strip().upper()
    if not query_text:
        raise ValueError("asin_query must not be empty")

    _, collection = await _require_collection("kw_tracked_asins")
    documents = _jsonable(
        await collection.find(
            {},
            projection={"_id": 0, "asin": 1},
            max_time_ms=15_000,
        ).to_list(length=5_000)
    )

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for document in documents:
        score = _score_tracked_asin_document(document, query_text=query_text)
        if score is not None:
            ranked.append((score[0], score[1], document))

    if not ranked:
        raise ValueError("No tracked ASIN matched asin_query.")

    ranked.sort(key=lambda item: (-item[0], str(item[2].get("asin", ""))))
    chosen_document = ranked[0][2]
    alternatives = [str(item[2].get("asin", "")) for item in ranked[:5]]
    return ResolvedTrackedAsin(
        query=asin_query,
        asin=str(chosen_document.get("asin", "")),
        matched_on=ranked[0][1],
        alternatives_considered=alternatives,
    )


async def _maybe_resolve_tracked_asin_query(asin_query: str) -> ResolvedTrackedAsin | None:
    try:
        return await _resolve_tracked_asin_query(asin_query)
    except ValueError:
        return None


async def _find_latest_search_term_document(
    collection_name: str,
    search_term_query: str,
) -> dict[str, Any] | None:
    _, collection = await _require_collection(collection_name)
    document = await collection.find_one(
        {"search_term": _search_term_exact_regex(search_term_query)},
        projection={"_id": 0, "search_term": 1, "received_at": 1, "search_results": 1, "meta": 1},
        sort=[("received_at", -1)],
        max_time_ms=15_000,
    )
    return _jsonable(document) if document else None


async def _find_search_term_documents(
    collection_name: str,
    search_term_query: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    _, collection = await _require_collection(collection_name)
    normalized_limit = _normalize_limit(limit, max_limit=MAX_KEYWORD_HISTORY_LIMIT)
    cursor = collection.find(
        {"search_term": _search_term_exact_regex(search_term_query)},
        projection={"_id": 0, "search_term": 1, "received_at": 1, "search_results": 1, "meta": 1},
        sort=[("received_at", -1)],
        limit=normalized_limit,
        max_time_ms=15_000,
    )
    return _jsonable(await cursor.to_list(length=normalized_limit))


async def _discover_search_term_source(search_term_query: str) -> tuple[ResolvedKeywordSource, str]:
    source_documents = await _fetch_keyword_source_documents()
    matches: list[tuple[str, str, dict[str, Any]]] = []
    for source_document in source_documents:
        collection_name = str(source_document.get("search_terms_collection", ""))
        if not collection_name:
            continue
        try:
            latest_document = await _find_latest_search_term_document(collection_name, search_term_query)
        except ValueError:
            continue
        if latest_document and latest_document.get("received_at"):
            matches.append(
                (
                    str(latest_document.get("received_at", "")),
                    str(latest_document.get("search_term", search_term_query.strip())),
                    source_document,
                )
            )

    if not matches:
        raise ValueError(
            "No keyword-tracking search-term snapshot matched search_term_query across the known source collections."
        )

    matches.sort(reverse=True)
    _, canonical_search_term, chosen_source_document = matches[0]
    alternatives = [str(item[2].get("slug", "")) for item in matches[:5]]
    return (
        _build_resolved_keyword_source(
            search_term_query,
            chosen_source_document,
            matched_on="search_term_present",
            alternatives=alternatives,
        ),
        canonical_search_term,
    )


async def _resolve_keyword_scope(
    search_term_query: str,
    *,
    source_query: str | None = None,
    active_only: bool = True,
) -> tuple[ResolvedKeywordSource, ResolvedTrackedKeyword | None, str]:
    if source_query:
        resolved_source = await _resolve_keyword_source_query(source_query)
        try:
            resolved_keyword = await _resolve_tracked_keyword_query(
                search_term_query,
                source_slug=resolved_source.source_slug,
                active_only=active_only,
            )
            return resolved_source, resolved_keyword, resolved_keyword.keyword
        except ValueError:
            latest_document = await _find_latest_search_term_document(
                resolved_source.search_terms_collection,
                search_term_query,
            )
            if not latest_document:
                raise ValueError(
                    f"No search-term snapshots matched search_term_query in source_slug={resolved_source.source_slug}."
                )
            return (
                resolved_source,
                None,
                str(latest_document.get("search_term", search_term_query.strip())),
            )

    try:
        resolved_keyword = await _resolve_tracked_keyword_query(
            search_term_query,
            active_only=active_only,
        )
    except ValueError:
        resolved_source, canonical_search_term = await _discover_search_term_source(search_term_query)
        return resolved_source, None, canonical_search_term

    resolved_source = await _resolve_keyword_source_from_tracked_keyword(
        {
            "source_slug": resolved_keyword.source_slug,
            "source_name": resolved_keyword.source_name,
            "search_terms_collection": resolved_keyword.search_terms_collection,
        },
        query=search_term_query,
    )
    return resolved_source, resolved_keyword, resolved_keyword.keyword


async def _fetch_tracked_asin_set() -> set[str]:
    _, collection = await _require_collection("kw_tracked_asins")
    values = await collection.distinct("asin", maxTimeMS=15_000)
    return {str(value).strip().upper() for value in values if value}


def _build_keyword_search_result_row(
    search_result: dict[str, Any],
    *,
    tracked_asins: set[str],
) -> KeywordSearchResultRow:
    asin = str(search_result.get("asin", "")).strip().upper()
    return KeywordSearchResultRow(
        asin=asin,
        position=_coerce_optional_int(search_result.get("position")),
        price=search_result.get("price"),
        rating=search_result.get("rating"),
        ratings_total=_coerce_optional_int(search_result.get("ratings_total")),
        sponsored=_coerce_optional_bool(search_result.get("sponsored")),
        tracked_asin=asin in tracked_asins,
    )


def _extract_keyword_search_result_for_asin(
    document: dict[str, Any],
    *,
    asin: str,
) -> dict[str, Any] | None:
    normalized_asin = asin.strip().upper()
    for row in document.get("search_results", []) or []:
        if str(row.get("asin", "")).strip().upper() == normalized_asin:
            return row
    return None


async def _fetch_zipcode_location_map() -> dict[str, dict[str, str | None]]:
    _, collection = await _require_collection("bb2_zipcode_locations")
    cursor = collection.find({}, projection={"_id": 0, "zipcode": 1, "city": 1, "state": 1}, max_time_ms=15_000)
    documents = _jsonable(await cursor.to_list(length=100))
    return {
        str(document.get("zipcode", "")): {
            "city": document.get("city"),
            "state": document.get("state"),
        }
        for document in documents
        if document.get("zipcode")
    }


def _stringify_optional(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _inventory_location_sort_key(document: dict[str, Any]) -> tuple[str, str]:
    return (str(document.get("name", "")), str(document.get("_id", "")))


def _inventory_sku_sort_key(document: dict[str, Any]) -> tuple[str, str]:
    return (str(document.get("sku", "")), str(document.get("name", "")))


def _build_resolved_inventory_location(
    query: str,
    document: dict[str, Any],
    *,
    matched_on: str,
    alternatives: list[str],
) -> ResolvedInventoryLocation:
    return ResolvedInventoryLocation(
        query=query,
        location_id=str(document.get("_id", "")),
        location_name=str(document.get("name", "")),
        amazon_sku_suffix=_stringify_optional(document.get("amazonSkuSuffix")),
        handling_time=document.get("handlingTime"),
        last_successful_ingestion_at=_stringify_optional(document.get("lastSuccessfulIngestionAt")),
        matched_on=matched_on,
        alternatives_considered=alternatives,
    )


def _build_resolved_inventory_sku(
    query: str,
    document: dict[str, Any],
    *,
    matched_on: str,
    alternatives: list[str],
) -> ResolvedInventorySku:
    aliases = [str(value) for value in document.get("aliases", []) if value]
    child_asins = [str(value) for value in document.get("childAsins", []) if value]
    kit_components = document.get("kitComponents", []) or []
    return ResolvedInventorySku(
        query=query,
        sku=str(document.get("sku", "")),
        name=str(document.get("name", "")),
        sku_type=str(document.get("type", "")),
        aliases=aliases,
        child_asins=child_asins,
        is_indirect_component=document.get("isIndirectComponent"),
        component_count=len(kit_components),
        matched_on=matched_on,
        alternatives_considered=alternatives,
    )


def _score_inventory_location_document(
    document: dict[str, Any],
    *,
    query_text: str,
    query_ident: str,
) -> tuple[int, str] | None:
    name = _normalize_text(str(document.get("name", "")))
    suffix = _normalize_identifier(str(document.get("amazonSkuSuffix", "")))
    shipping_template = _normalize_text(str(document.get("shippingTemplateName", "")))
    document_id = str(document.get("_id", ""))

    if query_text and name == query_text:
        return 100, "name_exact"
    if query_ident and suffix == query_ident:
        return 97, "suffix_exact"
    if query_text and shipping_template == query_text:
        return 95, "shipping_template_exact"
    if query_text and document_id.lower() == query_text:
        return 93, "id_exact"
    if query_text and name.startswith(query_text):
        return 88, "name_prefix"
    if query_text and shipping_template.startswith(query_text):
        return 86, "shipping_template_prefix"
    if query_text and query_text in name:
        return 80, "name_contains"
    if query_text and query_text in shipping_template:
        return 78, "shipping_template_contains"
    if query_ident and suffix and query_ident in suffix:
        return 76, "suffix_contains"
    return None


def _score_inventory_sku_document(
    document: dict[str, Any],
    *,
    query_text: str,
    query_ident: str,
) -> tuple[int, str] | None:
    sku = str(document.get("sku", ""))
    sku_ident = _normalize_identifier(sku)
    name = _normalize_text(str(document.get("name", "")))
    aliases = [str(value) for value in document.get("aliases", []) if value]
    alias_idents = [_normalize_identifier(value) for value in aliases]
    child_asins = [str(value) for value in document.get("childAsins", []) if value]
    child_asin_idents = [_normalize_identifier(value) for value in child_asins]

    if query_ident and sku_ident == query_ident:
        return 120, "sku_exact"
    if query_ident and query_ident in alias_idents:
        return 116, "alias_exact"
    if query_ident and query_ident in child_asin_idents:
        return 112, "asin_exact"
    if query_text and name == query_text:
        return 108, "name_exact"
    if query_ident and sku_ident.startswith(query_ident):
        return 98, "sku_prefix"
    if query_ident and any(alias.startswith(query_ident) for alias in alias_idents):
        return 95, "alias_prefix"
    if query_text and name.startswith(query_text):
        return 92, "name_prefix"
    if query_ident and query_ident in sku_ident:
        return 88, "sku_contains"
    if query_ident and any(query_ident in alias for alias in alias_idents):
        return 84, "alias_contains"
    if query_text and query_text in name:
        return 82, "name_contains"
    return None


def _inventory_sku_matches_alias(document: dict[str, Any], alias: str) -> bool:
    alias_ident = _normalize_identifier(alias)
    if not alias_ident:
        return False
    if _normalize_identifier(str(document.get("sku", ""))) == alias_ident:
        return True
    return alias_ident in {
        _normalize_identifier(str(value))
        for value in document.get("aliases", [])
        if value
    }


def _inventory_sku_matches_asin(document: dict[str, Any], asin: str) -> bool:
    asin_ident = _normalize_identifier(asin)
    if not asin_ident:
        return False
    return asin_ident in {
        _normalize_identifier(str(value))
        for value in document.get("childAsins", [])
        if value
    }


def _build_inventory_component_blueprint(
    sku_document: dict[str, Any],
    sku_catalog: dict[str, dict[str, Any]],
) -> list[InventorySkuComponent]:
    rows: list[InventorySkuComponent] = []
    for component in sku_document.get("kitComponents", []) or []:
        component_sku = str(component.get("componentSku", ""))
        component_document = sku_catalog.get(component_sku, {})
        rows.append(
            InventorySkuComponent(
                component_sku=component_sku,
                component_name=_stringify_optional(component_document.get("name")),
                required_quantity=max(_coerce_int(component.get("quantity", 1)), 1),
                is_indirect_component=component_document.get("isIndirectComponent"),
            )
        )
    return rows


def _build_inventory_component_availability(
    sku_document: dict[str, Any],
    *,
    location_id: str,
    inventory_by_pair: dict[tuple[str, str], int],
    sku_catalog: dict[str, dict[str, Any]],
) -> tuple[list[InventoryComponentAvailability], int | None, InventoryComponentAvailability | None]:
    rows: list[InventoryComponentAvailability] = []
    buildable_quantity: int | None = None
    limiting_component: InventoryComponentAvailability | None = None

    for component in sku_document.get("kitComponents", []) or []:
        component_sku = str(component.get("componentSku", ""))
        required_quantity = max(_coerce_int(component.get("quantity", 1)), 1)
        on_hand_quantity = _coerce_int(inventory_by_pair.get((location_id, component_sku), 0))
        build_limit = on_hand_quantity // required_quantity
        component_document = sku_catalog.get(component_sku, {})
        row = InventoryComponentAvailability(
            component_sku=component_sku,
            component_name=_stringify_optional(component_document.get("name")),
            required_quantity=required_quantity,
            on_hand_quantity=on_hand_quantity,
            build_limit=build_limit,
            is_indirect_component=component_document.get("isIndirectComponent"),
        )
        rows.append(row)
        if buildable_quantity is None or build_limit < buildable_quantity:
            buildable_quantity = build_limit
            limiting_component = row

    return rows, buildable_quantity, limiting_component


def _build_inventory_location_quantity_rows(
    sku_document: dict[str, Any],
    *,
    location_documents: list[dict[str, Any]],
    inventory_by_pair: dict[tuple[str, str], int],
    sku_catalog: dict[str, dict[str, Any]],
    include_component_breakdown: bool,
) -> list[InventoryLocationQuantity]:
    sku = str(sku_document.get("sku", ""))
    sku_type = str(sku_document.get("type", ""))
    rows: list[InventoryLocationQuantity] = []

    for location in sorted(location_documents, key=_inventory_location_sort_key):
        location_id = str(location.get("_id", ""))
        recorded_quantity = _coerce_int(inventory_by_pair.get((location_id, sku), 0))
        has_recorded_row = (location_id, sku) in inventory_by_pair
        component_breakdown: list[InventoryComponentAvailability] = []
        buildable_quantity: int | None = None
        limiting_component_sku: str | None = None
        limiting_component_name: str | None = None

        if sku_type in {"kit", "option"}:
            component_breakdown, buildable_quantity, limiting_component = _build_inventory_component_availability(
                sku_document,
                location_id=location_id,
                inventory_by_pair=inventory_by_pair,
                sku_catalog=sku_catalog,
            )
            if limiting_component is not None:
                limiting_component_sku = limiting_component.component_sku
                limiting_component_name = limiting_component.component_name
            if not include_component_breakdown:
                component_breakdown = []

        rows.append(
            InventoryLocationQuantity(
                location_id=location_id,
                location_name=str(location.get("name", "")),
                amazon_sku_suffix=_stringify_optional(location.get("amazonSkuSuffix")),
                handling_time=location.get("handlingTime"),
                last_successful_ingestion_at=_stringify_optional(location.get("lastSuccessfulIngestionAt")),
                has_recorded_row=has_recorded_row,
                recorded_quantity=recorded_quantity,
                buildable_quantity=buildable_quantity,
                limiting_component_sku=limiting_component_sku,
                limiting_component_name=limiting_component_name,
                component_breakdown=component_breakdown,
            )
        )

    return rows


def _summarize_inventory_location_rows(
    rows: list[InventoryLocationQuantity],
) -> tuple[int, int | None, int, int | None]:
    total_recorded = sum(row.recorded_quantity for row in rows)
    locations_with_recorded = sum(1 for row in rows if row.recorded_quantity > 0)
    buildable_values = [row.buildable_quantity for row in rows if row.buildable_quantity is not None]
    if buildable_values:
        total_buildable = sum(value or 0 for value in buildable_values)
        locations_with_buildable = sum(1 for value in buildable_values if value > 0)
    else:
        total_buildable = None
        locations_with_buildable = None
    return total_recorded, total_buildable, locations_with_recorded, locations_with_buildable


def _rebuild_inventory_quantity_summary(
    summary: InventoryQuantitySummary,
    rows: list[InventoryLocationQuantity],
    *,
    extra_note: str | None = None,
) -> InventoryQuantitySummary:
    total_recorded, total_buildable, locations_with_recorded, locations_with_buildable = _summarize_inventory_location_rows(rows)
    notes = list(summary.notes)
    if extra_note:
        notes.append(extra_note)
    return InventoryQuantitySummary(
        resolved_sku=summary.resolved_sku,
        selected_location=summary.selected_location,
        quantities=rows,
        total_recorded_quantity=total_recorded,
        total_buildable_quantity=total_buildable,
        locations_with_recorded_stock=locations_with_recorded,
        locations_with_buildable_stock=locations_with_buildable,
        notes=notes,
    )


async def _require_inventory_database() -> tuple[ApplicationRuntime, Any]:
    runtime = _require_runtime()
    if runtime.mongo_client is None or not runtime.settings.inventory_mongo_database:
        raise RuntimeError(
            "Inventory MongoDB is not configured. Set MONGODB_URI or BUYBOX_MCP_MONGO_URI and BUYBOX_MCP_INVENTORY_MONGO_DATABASE."
        )
    return runtime, runtime.mongo_client.get_database(runtime.settings.inventory_mongo_database)


async def _require_inventory_collection(name: str) -> tuple[ApplicationRuntime, Any]:
    runtime, database = await _require_inventory_database()
    available = await database.list_collection_names()
    if name not in available:
        raise ValueError(
            f"Unknown inventory collection: {name}. Expected one of {sorted(available)}."
        )
    return runtime, database[name]


async def _require_keyword_tracking_collection(name: str) -> tuple[ApplicationRuntime, Any]:
    runtime = _require_runtime()
    available = await runtime.mongo_database.list_collection_names()
    allowed = sorted(collection_name for collection_name in available if _is_keyword_tracking_collection_name(collection_name))
    if name not in allowed:
        raise ValueError(
            "Unknown keyword tracking collection: "
            f"{name}. Use keyword_rank_tracking_list_collections first. Allowed collections: {allowed}"
        )
    return runtime, runtime.mongo_database[name]


async def _require_inventory_query_collection(name: str) -> tuple[ApplicationRuntime, Any]:
    runtime, database = await _require_inventory_database()
    available = await database.list_collection_names()
    allowed = sorted(collection_name for collection_name in available if _is_inventory_query_collection_name(collection_name))
    if name not in allowed:
        raise ValueError(
            "Unknown scoped inventory collection: "
            f"{name}. Use inventory_by_location_list_collections first. Allowed collections: {allowed}"
        )
    return runtime, database[name]


async def _build_keyword_tracking_collection_profile(
    name: str,
    sample_size: int = 2,
) -> ScopedCollectionProfile:
    _, collection = await _require_keyword_tracking_collection(name)
    hint = _collection_hint(name)
    details = await _build_collection_profile_details(
        name,
        collection,
        hint=hint,
        sample_size=sample_size,
    )
    details["recommended_for_scope_analysis"] = True
    return ScopedCollectionProfile(**details)


async def _build_inventory_query_collection_profile(
    name: str,
    sample_size: int = 2,
) -> ScopedCollectionProfile:
    _, collection = await _require_inventory_query_collection(name)
    hint = _inventory_collection_hint(name)
    details = await _build_collection_profile_details(
        name,
        collection,
        hint=hint,
        sample_size=sample_size,
    )
    details["recommended_for_scope_analysis"] = True
    return ScopedCollectionProfile(**details)


async def _list_keyword_tracking_collection_summaries() -> list[ScopedCollectionSummary]:
    runtime = _require_runtime()
    names = sorted(
        name
        for name in await runtime.mongo_database.list_collection_names()
        if _is_keyword_tracking_collection_name(name)
    )
    results: list[ScopedCollectionSummary] = []
    for name in names:
        estimated_count = await runtime.mongo_database[name].estimated_document_count()
        hint = _collection_hint(name)
        results.append(
            ScopedCollectionSummary(
                name=name,
                estimated_count=estimated_count,
                category=hint.category,
                description=hint.description,
                recommended_for_scope_analysis=True,
            )
        )
    return results


async def _list_inventory_query_collection_summaries() -> list[ScopedCollectionSummary]:
    _, database = await _require_inventory_database()
    names = sorted(
        name
        for name in await database.list_collection_names()
        if _is_inventory_query_collection_name(name)
    )
    results: list[ScopedCollectionSummary] = []
    for name in names:
        estimated_count = await database[name].estimated_document_count()
        hint = _inventory_collection_hint(name)
        results.append(
            ScopedCollectionSummary(
                name=name,
                estimated_count=estimated_count,
                category=hint.category,
                description=hint.description,
                recommended_for_scope_analysis=True,
            )
        )
    return results


async def _fetch_inventory_location_documents() -> list[dict[str, Any]]:
    _, collection = await _require_inventory_collection("locations")
    cursor = collection.find(
        {},
        projection={
            "_id": 1,
            "name": 1,
            "amazonSkuSuffix": 1,
            "handlingTime": 1,
            "lastSuccessfulIngestionAt": 1,
            "shippingTemplateName": 1,
        },
        max_time_ms=15_000,
    )
    return _jsonable(await cursor.to_list(length=100))


async def _fetch_inventory_sku_documents(
    filter_doc: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    _, collection = await _require_inventory_collection("skus")
    cursor = collection.find(
        filter_doc or {},
        projection={
            "_id": 0,
            "sku": 1,
            "name": 1,
            "type": 1,
            "aliases": 1,
            "childAsins": 1,
            "kitComponents": 1,
            "isIndirectComponent": 1,
        },
        max_time_ms=20_000,
    )
    return _jsonable(await cursor.to_list(length=5_000))


async def _fetch_inventory_level_documents(
    *,
    skus: list[str] | None = None,
    location_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    _, collection = await _require_inventory_collection("inventorylevels")
    filter_doc: dict[str, Any] = {}
    if skus is not None:
        normalized_skus = sorted({str(value) for value in skus if value})
        if not normalized_skus:
            return []
        filter_doc["sku"] = {"$in": normalized_skus}
    if location_ids is not None:
        normalized_location_ids = sorted({str(value) for value in location_ids if value})
        if not normalized_location_ids:
            return []
        filter_doc["locationId"] = {"$in": normalized_location_ids}
    cursor = collection.find(
        filter_doc,
        projection={"_id": 0, "locationId": 1, "sku": 1, "quantity": 1},
        max_time_ms=20_000,
    )
    return _jsonable(await cursor.to_list(length=10_000))


async def _fetch_inventory_alias_documents(
    *,
    asin: str,
    location_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    _, collection = await _require_inventory_collection("amazonskualiases")
    filter_doc: dict[str, Any] = {"asin": asin}
    if location_ids is not None:
        normalized_location_ids = sorted({str(value) for value in location_ids if value})
        if not normalized_location_ids:
            return []
        filter_doc["locationId"] = {"$in": normalized_location_ids}
    cursor = collection.find(
        filter_doc,
        projection={"_id": 0, "locationId": 1, "asin": 1, "amazonSkuAlias": 1},
        max_time_ms=15_000,
    )
    return _jsonable(await cursor.to_list(length=5_000))


def _resolve_inventory_location_from_documents(
    location_query: str,
    documents: list[dict[str, Any]],
) -> ResolvedInventoryLocation:
    query_text = _normalize_text(location_query)
    query_ident = _normalize_identifier(location_query)
    if not query_text and not query_ident:
        raise ValueError("location_query must not be empty")

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for document in documents:
        score = _score_inventory_location_document(
            document,
            query_text=query_text,
            query_ident=query_ident,
        )
        if score is not None:
            ranked.append((score[0], score[1], document))

    if not ranked:
        available = [str(document.get("name", "")) for document in sorted(documents, key=_inventory_location_sort_key)]
        raise ValueError(
            "No inventory location matched location_query. "
            f"Available locations: {available}"
        )

    ranked.sort(key=lambda item: (-item[0],) + _inventory_location_sort_key(item[2]))
    matched_on = ranked[0][1]
    chosen_document = ranked[0][2]
    alternatives = [str(item[2].get("name", "")) for item in ranked[:5]]
    return _build_resolved_inventory_location(
        location_query,
        chosen_document,
        matched_on=matched_on,
        alternatives=alternatives,
    )


def _resolve_inventory_sku_from_documents(
    sku_query: str,
    documents: list[dict[str, Any]],
) -> ResolvedInventorySku:
    query_text = _normalize_text(sku_query)
    query_ident = _normalize_identifier(sku_query)
    if not query_text and not query_ident:
        raise ValueError("sku_query must not be empty")

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for document in documents:
        score = _score_inventory_sku_document(
            document,
            query_text=query_text,
            query_ident=query_ident,
        )
        if score is not None:
            ranked.append((score[0], score[1], document))

    if not ranked:
        sample = [str(document.get("sku", "")) for document in sorted(documents, key=_inventory_sku_sort_key)[:25]]
        raise ValueError(
            "No inventory SKU matched sku_query. "
            f"Sample available SKUs: {sample}"
        )

    ranked.sort(key=lambda item: (-item[0],) + _inventory_sku_sort_key(item[2]))
    matched_on = ranked[0][1]
    chosen_document = ranked[0][2]
    alternatives = [str(item[2].get("sku", "")) for item in ranked[:10]]
    return _build_resolved_inventory_sku(
        sku_query,
        chosen_document,
        matched_on=matched_on,
        alternatives=alternatives,
    )


async def _resolve_inventory_location_query(location_query: str) -> ResolvedInventoryLocation:
    documents = await _fetch_inventory_location_documents()
    return _resolve_inventory_location_from_documents(location_query, documents)


async def _resolve_inventory_sku_query(sku_query: str) -> ResolvedInventorySku:
    documents = await _fetch_inventory_sku_documents()
    return _resolve_inventory_sku_from_documents(sku_query, documents)


async def _select_inventory_locations(
    location_query: str | None,
) -> tuple[list[dict[str, Any]], ResolvedInventoryLocation | None]:
    documents = await _fetch_inventory_location_documents()
    ordered = sorted(documents, key=_inventory_location_sort_key)
    if location_query is None:
        return ordered, None
    resolved = _resolve_inventory_location_from_documents(location_query, ordered)
    selected = [document for document in ordered if str(document.get("_id", "")) == resolved.location_id]
    return selected, resolved


async def _get_inventory_sku_document(resolved_sku: ResolvedInventorySku) -> dict[str, Any]:
    documents = await _fetch_inventory_sku_documents({"sku": resolved_sku.sku})
    if not documents:
        raise ValueError(f"Could not load inventory SKU document for {resolved_sku.sku}.")
    return documents[0]


async def _build_inventory_quantity_summary(
    resolved_sku: ResolvedInventorySku,
    *,
    location_documents: list[dict[str, Any]],
    selected_location: ResolvedInventoryLocation | None,
    include_component_breakdown: bool,
    notes: list[str] | None = None,
) -> InventoryQuantitySummary:
    sku_document = await _get_inventory_sku_document(resolved_sku)
    component_skus = [
        str(component.get("componentSku", ""))
        for component in sku_document.get("kitComponents", []) or []
        if component.get("componentSku")
    ]
    sku_catalog_documents = await _fetch_inventory_sku_documents(
        {"sku": {"$in": sorted({resolved_sku.sku, *component_skus})}}
    )
    sku_catalog = {str(document.get("sku", "")): document for document in sku_catalog_documents}
    location_ids = [str(document.get("_id", "")) for document in location_documents]
    inventory_documents = await _fetch_inventory_level_documents(
        skus=sorted({resolved_sku.sku, *component_skus}),
        location_ids=location_ids,
    )
    inventory_by_pair = {
        (str(document.get("locationId", "")), str(document.get("sku", ""))): _coerce_int(document.get("quantity", 0))
        for document in inventory_documents
    }
    quantity_rows = _build_inventory_location_quantity_rows(
        sku_document,
        location_documents=location_documents,
        inventory_by_pair=inventory_by_pair,
        sku_catalog=sku_catalog,
        include_component_breakdown=include_component_breakdown,
    )
    total_recorded, total_buildable, locations_with_recorded, locations_with_buildable = _summarize_inventory_location_rows(quantity_rows)
    final_notes = list(notes or [])
    if resolved_sku.sku_type in {"kit", "option"}:
        final_notes.append(
            "For kit and option SKUs, recorded_quantity comes from direct inventory rows and buildable_quantity is derived from component stock. These are reported separately to avoid overstating availability."
        )
    else:
        final_notes.append(
            "For component SKUs, recorded_quantity is the direct on-hand quantity. buildable_quantity is omitted because no component explosion is required."
        )
    return InventoryQuantitySummary(
        resolved_sku=resolved_sku,
        selected_location=selected_location,
        quantities=quantity_rows,
        total_recorded_quantity=total_recorded,
        total_buildable_quantity=total_buildable,
        locations_with_recorded_stock=locations_with_recorded,
        locations_with_buildable_stock=locations_with_buildable,
        notes=final_notes,
    )


def create_mcp_server(settings: Settings | None = None) -> FastMCP:
    resolved = settings or get_settings()
    mcp = FastMCP(
        name=resolved.app_name,
        instructions=(
            "Read-only MongoDB MCP server for Amazon buy-box, search-term, and inventory analytics. "
            "Start with buybox_featured_offer_percent_list_collections or schema://catalog. For buy-box questions, first read "
            "bb2_offer_settings to learn the canonical internal seller name in our_seller_name, "
            "then use bb2_offer_sources to confirm valid source_slug values, bb2_offer_runs to find the "
            "latest completed runs and coverage, and bb2_offers for offer-level facts. In bb2_offers, "
            "a buy-box cell is one tracking_key at one received_at timestamp, and the winner is the row "
            "with the lowest offer_index. For run-over-run analysis, compare overlap cells first, then "
            "use run coverage and bb2_offer_missing_cells to explain gaps. Prefer buybox_featured_offer_percent_resolve_source "
            "when the question names a brand but not a source_slug, and prefer buybox_featured_offer_percent_summarize_status "
            "for the standard 'are we gaining or losing the buy box?' workflow. Do not assume every "
            "collection has rows for every date; some datasets are daily, some are weekly, and some are "
            "loaded only when requested. Legacy collections with names like ad hoc brand offer snapshots "
            "are not the current production fact table. For INV-Tracker questions about components, kits, "
            "options, and per-location stock, use the inventory_by_location_list_collections, "
            "inventory_by_location_find_documents, inventory_by_location_aggregate_documents, "
            "inventory_by_location_resolve_sku, inventory_by_location_resolve_location, "
            "inventory_by_location_quantity, inventory_by_location_buildable_quantity, and "
            "inventory_by_location_component_constraints tools. For kit and option SKUs, report "
            "recorded_quantity and buildable_quantity separately; do not assume zero recorded kit "
            "quantity means unavailable if components exist. For keyword rank tracking questions, use "
            "keyword_rank_tracking_list_sources, keyword_rank_tracking_list_collections, "
            "keyword_rank_tracking_find_documents, keyword_rank_tracking_aggregate_documents, "
            "keyword_rank_tracking_resolve_source, keyword_rank_tracking_resolve_keyword, "
            "keyword_rank_tracking_latest_search_results, keyword_rank_tracking_rank_history, and "
            "keyword_rank_tracking_search_query_volume. There is no "
            "precomputed keyword rank history table; reconstruct rank over time from search-term snapshot "
            "documents by following search_results[].position across received_at values. All Mongo access is read-only."
        ),
        host=resolved.host,
        port=resolved.port,
        stateless_http=True,
        json_response=True,
        streamable_http_path="/",
    )

    @mcp.resource(
        "schema://catalog",
        name="catalog",
        title="Collection Catalog",
        description=(
            "High-level catalog of collections, counts, categories, and buy-box analysis guidance, "
            "including which collections are production versus legacy."
        ),
    )
    async def collection_catalog() -> dict[str, Any]:
        runtime = _require_runtime()
        names = await runtime.mongo_database.list_collection_names()
        summaries: list[CollectionSummary] = []
        for name in sorted(names):
            estimated_count = await runtime.mongo_database[name].estimated_document_count()
            hint = _collection_hint(name)
            summaries.append(
                CollectionSummary(
                    name=name,
                    estimated_count=estimated_count,
                    category=hint.category,
                    description=hint.description,
                    recommended_for_buybox_analysis=hint.recommended_for_buybox_analysis,
                )
            )
        return {
            "database": runtime.settings.mongo_database,
            "collections": [item.model_dump() for item in summaries],
            "recommended_buybox_collections": [
                item.name for item in summaries if item.recommended_for_buybox_analysis
            ],
            "notes": [
                "Read bb2_offer_settings first when the question refers to 'us', 'our seller', or 'our buy box'.",
                "bb2_offers is the main production fact table for buy-box status and change analysis.",
                "A buy-box cell is one tracking_key at one received_at timestamp, and the winner is the row with the lowest offer_index.",
                "Use bb2_offer_runs to identify the latest completed run and compare expected versus observed coverage before interpreting trend changes.",
                "Prefer buybox_featured_offer_percent_summarize_status for the standard current-status question before dropping to manual aggregation.",
                "Prefer result_set.id or received_at for run alignment in status analysis; do not rely on meta.run_id as the first choice.",
                "For keyword rank tracking, start with kw_sources, kw_tracked_keywords, kw_search_query_volumes, and the source-specific collections named like source-*-search_terms.",
                "There is no dedicated keyword_rank_history collection. Reconstruct rank by reading search_results[].position across repeated search-term snapshots over time.",
                "Prefer the keyword_rank_tracking_ tool set for source resolution, scoped ad hoc queries, latest keyword snapshots, per-ASIN rank history, and search-query-volume lookups.",
                "Do not assume every collection has data for every calendar date; verify actual run cadence and available dates first.",
                "Treat legacy_buybox collections as historical or experimental unless the question explicitly targets them.",
                "The server also exposes INV-Tracker tools prefixed inventory_by_location_ for scoped ad hoc queries plus per-location component, kit, option, ASIN, and freshness questions.",
            ],
        }

    @mcp.resource(
        "schema://collection/{collection_name}",
        name="collection_profile",
        title="Collection Profile",
        description="Detailed schema profile for one collection, including indexes and sample documents.",
    )
    async def collection_profile(collection_name: str) -> dict[str, Any]:
        profile = await _build_collection_profile(collection_name)
        return profile.model_dump()

    @mcp.resource(
        "guide://query-patterns",
        name="query_patterns",
        title="Query Patterns",
        description=(
            "Practical workflow guidance for answering buy-box questions with the open Mongo tools, "
            "including winner semantics and run-comparison patterns."
        ),
    )
    def query_patterns() -> dict[str, Any]:
        return {
            "workflow": [
                "Read schema://catalog or call buybox_featured_offer_percent_list_collections first.",
                "If the question refers to 'us' or 'our buy box', read bb2_offer_settings and use our_seller_name as the canonical seller identity.",
                "Use bb2_offer_sources or buybox_featured_offer_percent_distinct_values on bb2_offers.source_slug to discover valid source_slug values before querying a brand.",
                "Use buybox_featured_offer_percent_resolve_source when the question gives you a human source name like Treasure Garden instead of an exact slug.",
                "Use bb2_offer_runs to identify the latest completed runs and inspect coverage before comparing outcomes.",
                "Use buybox_featured_offer_percent_summarize_status for the common executive question: current status, gaining or losing, and where. Pass explicit date or timestamp selectors when the question names specific dates.",
                "Verify date availability before comparing time periods. Collection cadence is not uniform: some data is daily, some weekly, and some exists only for manually requested runs.",
                "Use buybox_featured_offer_percent_inspect_collection if field names or document shape are unclear.",
                "Use buybox_featured_offer_percent_find_documents for raw rows and spot checks, and buybox_featured_offer_percent_aggregate_documents for summaries, grouping, ranking, and time-series analysis.",
            ],
            "inventory_workflow": [
                "Use inventory_by_location_list_collections, inventory_by_location_inspect_collection, inventory_by_location_find_documents, inventory_by_location_distinct_values, and inventory_by_location_aggregate_documents when you need ad hoc inventory queries without exposing non-inventory collections.",
                "Use inventory_by_location_resolve_sku when the user gives you a SKU fragment, alias, or child ASIN but not the exact internal SKU.",
                "Use inventory_by_location_resolve_location when the user names a warehouse or shipping suffix informally.",
                "Use inventory_by_location_quantity for direct on-hand quantities by location.",
                "Use inventory_by_location_buildable_quantity or inventory_by_location_component_constraints for kit and option SKUs that are assembled from components.",
                "Report recorded_quantity and buildable_quantity separately for kits and options. Do not assume zero recorded kit quantity means unavailable if components exist.",
                "Use inventory_by_location_parent_skus_for_component to understand shortage impact and inventory_by_location_availability_for_asin to map Amazon listings back to stock by location.",
                "Use inventory_by_location_uncovered_sku_gaps and inventory_by_location_ingestion_freshness to judge whether missing inventory is a data-model issue, an ingestion issue, or true zero stock.",
            ],
            "keyword_workflow": [
                "Use keyword_rank_tracking_list_collections, keyword_rank_tracking_inspect_collection, keyword_rank_tracking_find_documents, keyword_rank_tracking_distinct_values, and keyword_rank_tracking_aggregate_documents when you need ad hoc keyword/search-term queries without exposing buy-box collections.",
                "Use keyword_rank_tracking_list_sources to see valid keyword/search-term sources and their backing collections.",
                "Use keyword_rank_tracking_resolve_source when the user gives you an informal source name instead of an exact slug.",
                "Use keyword_rank_tracking_resolve_keyword when the user gives you a keyword and you need the canonical tracked term, source_slug, and source collection.",
                "Use keyword_rank_tracking_latest_search_results to inspect the latest ranked search-result snapshot for one term.",
                "Use keyword_rank_tracking_rank_history to trace one ASIN's position across repeated snapshots for one term over time.",
                "Use keyword_rank_tracking_search_query_volume to retrieve uploaded search query volume rows keyed by keyword and source.",
                "Verify actual source cadence from the *_searchterms_runs collections or available received_at values before answering time-based questions.",
            ],
            "buybox_semantics": {
                "cell_definition": "One buy-box cell is one tracking_key at one received_at timestamp. In practice, tracking_key usually encodes source_slug, ASIN, and zipcode.",
                "winner_rule": "Within one cell, sort by offer_index ascending. The row with the lowest offer_index is the buy-box winner.",
                "status_rule": "For current status questions, collapse bb2_offers to one winner row per cell before counting wins by seller, zipcode, or ASIN.",
                "comparison_rule": "For run-over-run change, compare overlap cells present in both runs first, then separately explain coverage differences from bb2_offer_runs and bb2_offer_missing_cells.",
                "run_link_rule": "For status work, prefer run-level received_at and result_set.id from bb2_offer_runs for alignment. Do not assume meta.run_id is the most stable join key.",
                "date_availability_rule": "Do not assume every collection contains every date. Confirm actual date coverage from run metadata or distinct received_at values before answering time-based questions.",
            },
            "keyword_semantics": {
                "snapshot_rule": "Each search-term document is one snapshot for one search_term at one received_at timestamp.",
                "rank_rule": "Keyword rank is not precomputed in a separate table. Use search_results[].position within each snapshot, then compare positions across received_at values.",
                "asin_rule": "ASIN presence in search_results indicates the listing was observed for that term in that snapshot. Absence means not present in the captured results, not necessarily zero demand.",
                "source_rule": "Resolve the source before comparing terms because different sources map to different search-term collections and cadences.",
                "volume_rule": "kw_search_query_volumes stores uploaded volume rows by keyword and source; use uploaded_at to judge freshness.",
            },
            "inventory_semantics": {
                "inventory_model": "INV-Tracker stores direct inventory rows in inventorylevels and assembly structure in skus.kitComponents.",
                "assembly_rule": "For kit and option SKUs, buildable_quantity is the minimum per-component build limit at a location, computed from component on-hand quantities divided by required quantities.",
                "quantity_rule": "Keep recorded_quantity and buildable_quantity separate in the answer unless the business has explicitly defined how to merge direct kit stock with buildable kit stock.",
                "location_rule": "Join inventorylevels.locationId to locations._id as a string and include lastSuccessfulIngestionAt when answering location-level stock questions.",
                "asin_rule": "Use childAsins and amazonskualiases together to map Amazon listings back to internal stock by location.",
            },
            "common_pitfalls": [
                "Do not treat legacy_buybox collections as the main production dataset when bb2_offers is available.",
                "Do not count every bb2_offers row as a separate buy-box outcome; first collapse each cell to its top-ranked winner.",
                "Do not guess source_slug values from brand names when bb2_offer_sources or buybox_featured_offer_percent_distinct_values can confirm them.",
                "Do not pass sort as a string like 'received_at:-1'; pass a list of objects like [{'field': 'received_at', 'direction': -1}].",
                "Do not assume missing dates mean zero activity; first confirm whether that collection or source actually ran on those dates.",
                "Do not collapse recorded kit quantity and component-derived buildable quantity into one number unless the business explicitly defines that merge rule.",
                "Do not treat uncoveredskuobservations as inventory truth; it is a gap and ingestion diagnostic table.",
            ],
            "buybox_examples": [
                {
                    "question": "How did the buy box change over time for Treasure Garden?",
                    "hint": (
                        "Start in bb2_offer_runs to find the latest completed Treasure Garden runs. "
                        "Then query bb2_offers for those received_at values, collapse each cell to its "
                        "winner by sorting on offer_index, and compare win share over time."
                    ),
                },
                {
                    "question": "Which SKUs saw the biggest buy-box swings?",
                    "hint": (
                        "Collapse bb2_offers to one winner per cell first, then aggregate by asin across "
                        "runs or time buckets and sort by net gains, losses, or win-share delta."
                    ),
                },
                {
                    "question": "Which zipcodes were stable versus volatile?",
                    "hint": (
                        "After collapsing to one winner per cell, group by zipcode and measure win-share "
                        "change, net gains versus losses, or distinct winner counts across runs."
                    ),
                },
            ],
            "keyword_examples": [
                {
                    "question": "What is our latest rank snapshot for gas fireplace logs?",
                    "hint": (
                        "Use keyword_rank_tracking_resolve_keyword to pin down the canonical tracked term and "
                        "source, then use keyword_rank_tracking_latest_search_results for the newest received_at snapshot."
                    ),
                },
                {
                    "question": "How has ASIN B000E86AKC ranked for a keyword over time?",
                    "hint": (
                        "Use keyword_rank_tracking_rank_history with the term and ASIN. The tool reconstructs "
                        "history from search_results[].position across repeated snapshots."
                    ),
                },
                {
                    "question": "What search volume do we have for patio umbrella keywords?",
                    "hint": (
                        "Resolve the source if needed, then use keyword_rank_tracking_search_query_volume to "
                        "read kw_search_query_volumes rows and judge freshness from uploaded_at."
                    ),
                },
            ],
        }

    @mcp.prompt(
        name="analyze_buybox_question",
        title="Analyze Buy Box Question",
        description=(
            "Guidance prompt for answering open-ended buy-box questions with the schema and query tools, "
            "including how to identify our seller, winners, and run-over-run changes."
        ),
    )
    def analyze_buybox_question(
        question: Annotated[
            str,
            Field(description="The plain-English business question to answer from MongoDB."),
        ],
    ) -> str:
        return (
            "You are using a read-only MongoDB MCP server.\n"
            "1. Read schema://catalog or call buybox_featured_offer_percent_list_collections.\n"
            "2. If the question refers to 'us', first read bb2_offer_settings and use our_seller_name.\n"
            "3. Prefer bb2_offer_sources or buybox_featured_offer_percent_distinct_values to confirm the correct source_slug.\n"
            "4. Prefer bb2_offer_runs for scrape timing, latest completed runs, and run coverage.\n"
            "5. Prefer bb2_offers for winner, seller, price, Prime/FBA, delivery, ASIN, zipcode, and time analysis.\n"
            "6. If the question is a standard status question, use buybox_featured_offer_percent_resolve_source and buybox_featured_offer_percent_summarize_status first. If the user asks for specific dates, pass date or timestamp selectors to buybox_featured_offer_percent_summarize_status.\n"
            "7. In bb2_offers, collapse each tracking_key plus received_at cell to the row with the lowest offer_index before counting buy-box winners.\n"
            "8. For run-over-run change, compare overlap cells first and explain coverage differences separately.\n"
            "9. Prefer received_at and result_set.id from bb2_offer_runs over meta.run_id when you need a stable run key.\n"
            "10. Do not assume the same dates exist in every collection; confirm actual cadence and date availability first.\n"
            "11. State the collection and key fields you used in the final answer.\n\n"
            f"Question to answer:\n{question}"
        )

    @mcp.prompt(
        name="analyze_keyword_rank_tracking_question",
        title="Analyze Keyword Rank Tracking Question",
        description=(
            "Guidance prompt for answering keyword rank tracking questions with the keyword_rank_tracking_ tools "
            "and the underlying search-term snapshot collections."
        ),
    )
    def analyze_keyword_rank_tracking_question(
        question: Annotated[
            str,
            Field(description="The plain-English keyword rank tracking question to answer from MongoDB."),
        ],
    ) -> str:
        return (
            "You are using a read-only MongoDB MCP server.\n"
            "1. Read schema://catalog or call keyword_rank_tracking_list_collections and keyword_rank_tracking_list_sources.\n"
            "2. Use keyword_rank_tracking_inspect_collection, keyword_rank_tracking_find_documents, keyword_rank_tracking_distinct_values, or keyword_rank_tracking_aggregate_documents when you need scoped ad hoc keyword/search-term queries.\n"
            "3. Resolve the source with keyword_rank_tracking_resolve_source when the user names a brand, niche, or workflow informally.\n"
            "4. Resolve the canonical tracked term with keyword_rank_tracking_resolve_keyword when possible.\n"
            "5. Use keyword_rank_tracking_latest_search_results for the newest snapshot of one term.\n"
            "6. Use keyword_rank_tracking_rank_history to reconstruct one ASIN's rank from search_results[].position across received_at timestamps.\n"
            "7. Use keyword_rank_tracking_search_query_volume for uploaded demand metrics from kw_search_query_volumes.\n"
            "8. Do not assume a missing ASIN means rank zero forever; it only means the ASIN was absent from the captured snapshot results.\n"
            "9. Do not assume every source ran every day; confirm cadence from *_searchterms_runs or available received_at values.\n"
            "10. State the collections and key fields you used in the final answer.\n\n"
            f"Question to answer:\n{question}"
        )

    @mcp.tool(
        title="Server Status and Identity",
        description=(
            "Return the MCP server name, environment, auth mode, startup state, and whether MongoDB is configured. "
            "Use this first to confirm you are on the intended prod or dev server before running data queries."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_server_status() -> ServerStatus:
        runtime = get_runtime()
        return ServerStatus(
            name=runtime.settings.app_name,
            environment=runtime.settings.env,
            auth="bearer",
            mongo_configured=runtime.mongo_client is not None,
            mongo_database=runtime.settings.mongo_database,
            inventory_mongo_database=runtime.settings.inventory_mongo_database,
            startup_errors=runtime.startup_errors,
        )

    @mcp.tool(
        title="List Collections",
        description=(
            "List available MongoDB collections with approximate counts and usage hints. "
            "This is the best first tool for orienting an agent inside the dataset. For current buy-box "
            "reporting, start with bb2_offers, bb2_offer_runs, bb2_offer_sources, bb2_offer_settings, "
            "and bb2_zipcode_locations, and treat legacy_buybox collections as non-primary. Do not infer "
            "date cadence from this tool alone; verify actual run dates separately."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_list_collections() -> list[CollectionSummary]:
        runtime = _require_runtime()
        names = await runtime.mongo_database.list_collection_names()
        results: list[CollectionSummary] = []
        for name in sorted(names):
            estimated_count = await runtime.mongo_database[name].estimated_document_count()
            hint = _collection_hint(name)
            results.append(
                CollectionSummary(
                    name=name,
                    estimated_count=estimated_count,
                    category=hint.category,
                    description=hint.description,
                    recommended_for_buybox_analysis=hint.recommended_for_buybox_analysis,
                )
            )
        return results

    @mcp.tool(
        title="Inspect Collection",
        description=(
            "Inspect one collection's structure: approximate count, sample fields, nested field paths, indexes, "
            "and a few sample documents. Use this before writing ad hoc Mongo queries against unfamiliar data, "
            "especially legacy collections or any collection whose join keys and timestamps are unclear."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_inspect_collection(
        collection_name: Annotated[
            str,
            Field(description="Exact collection name, for example bb2_offers or bb2_offer_runs."),
        ],
        sample_size: Annotated[
            int,
            Field(description="How many sample documents to return. Maximum 5.", ge=1, le=MAX_SAMPLE_SIZE),
        ] = 2,
    ) -> CollectionProfile:
        return await _build_collection_profile(collection_name, sample_size=sample_size)

    @mcp.tool(
        title="Find Documents",
        description=(
            "Run a read-only MongoDB find query against one collection. "
            "Pass a Mongo filter object, optional projection, optional sort, and a limit. "
            "Use this for raw document inspection, recent rows, run metadata lookups, or direct lookups by ASIN, "
            "seller, source_slug, tracking_key, or zipcode. For a single buy-box cell, filter to one tracking_key "
            "and received_at and sort by offer_index ascending; the first row is the winner."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_find_documents(
        collection_name: Annotated[
            str,
            Field(description="Exact collection name to query."),
        ],
        filter: Annotated[
            dict[str, Any],
            Field(description="MongoDB filter document. Use {} to match all documents."),
        ] = Field(default_factory=dict),
        projection: Annotated[
            dict[str, Any] | None,
            Field(description="Optional MongoDB projection document, for example {'_id': 0, 'asin': 1}."),
        ] = None,
        sort: Annotated[
            list[SortField] | None,
            Field(
                description=(
                    "Optional sort order as a list of objects. Example: "
                    "[{'field': 'received_at', 'direction': -1}]. Do not pass strings like 'received_at:-1'."
                )
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(description="Maximum number of documents to return. Hard-capped at 200.", ge=1, le=MAX_FIND_LIMIT),
        ] = 25,
    ) -> QueryResult:
        _, collection = await _require_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_FIND_LIMIT)
        documents = await _execute_find_query(
            collection,
            filter=filter,
            projection=projection,
            sort=sort,
            limit=normalized_limit,
        )
        return QueryResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            documents=_jsonable(documents),
        )

    @mcp.tool(
        title="Distinct Values",
        description=(
            "Return distinct values for one field in one collection, with an optional filter. "
            "Use this to discover valid source slugs, seller names, zipcodes, ASINs, statuses, or other field values "
            "before composing a larger query. Prefer this over guessing source_slug or seller_name values."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_distinct_values(
        collection_name: Annotated[
            str,
            Field(description="Exact collection name to query."),
        ],
        field_name: Annotated[
            str,
            Field(
                description=(
                    "Field path to inspect, for example source_slug, seller_name, zipcode, "
                    "received_at, or meta.result_set_id. Prefer received_at or meta.result_set_id "
                    "over meta.run_id for run alignment."
                )
            ),
        ],
        filter: Annotated[
            dict[str, Any],
            Field(description="Optional MongoDB filter document used before collecting distinct values."),
        ] = Field(default_factory=dict),
        limit: Annotated[
            int,
            Field(description="Maximum number of distinct values to return. Hard-capped at 200.", ge=1, le=MAX_DISTINCT_LIMIT),
        ] = 50,
    ) -> DistinctResult:
        _, collection = await _require_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_DISTINCT_LIMIT)
        normalized_values = await _execute_distinct_query(
            collection,
            field_name=field_name,
            filter=filter,
            limit=normalized_limit,
        )
        return DistinctResult(
            collection=collection_name,
            field=field_name,
            returned_count=len(normalized_values),
            limit_applied=normalized_limit,
            values=normalized_values,
        )

    @mcp.tool(
        title="Resolve Buy Box Source",
        description=(
            "Resolve a human source name or partial slug, such as Treasure Garden, into the best matching "
            "production source slug from bb2_offer_sources. Use this before analysis when the user names a "
            "brand or collection informally instead of providing an exact source_slug."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_resolve_source(
        source_query: Annotated[
            str,
            Field(
                description=(
                    "Human source name or slug fragment, for example Treasure Garden or "
                    "all-treasure-garden-offers."
                )
            ),
        ],
    ) -> ResolvedSource:
        return await _resolve_source_query(source_query)

    @mcp.tool(
        title="Summarize Buy Box Status",
        description=(
            "Return a business-ready buy-box status summary for the latest completed run versus the prior "
            "completed run for one source. This tool resolves the source slug, loads our canonical seller "
            "identity, picks the latest completed runs, collapses bb2_offers to winner rows, compares overlap "
            "cells, and reports where we are gaining or losing by zipcode and competitor. By default it uses "
            "the latest two completed runs that actually exist, but you can also provide explicit date or "
            "timestamp selectors and it will use the latest completed run at or before each selector."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_summarize_status(
        source_query: Annotated[
            str,
            Field(
                description=(
                    "Human source name or slug fragment, for example Treasure Garden or "
                    "all-treasure-garden-offers."
                )
            ),
        ],
        latest_as_of: Annotated[
            str | None,
            Field(
                description=(
                    "Optional ISO date or timestamp for the newer side of the comparison. "
                    "Examples: 2026-04-30 or 2026-04-30T14:09:29Z. The tool uses the latest completed run "
                    "at or before this selector. If omitted, it uses the newest available completed run."
                )
            ),
        ] = None,
        previous_as_of: Annotated[
            str | None,
            Field(
                description=(
                    "Optional ISO date or timestamp for the older side of the comparison. "
                    "Examples: 2026-04-28 or 2026-04-28T13:03:03Z. The tool uses the latest completed run "
                    "at or before this selector. If omitted, it uses the completed run immediately before the "
                    "selected newer run."
                )
            ),
        ] = None,
        recent_run_count: Annotated[
            int,
            Field(
                description="How many recent completed runs to include in the trend output. Minimum 2, maximum 10.",
                ge=2,
                le=10,
            ),
        ] = 6,
        top_zipcodes: Annotated[
            int,
            Field(
                description="How many gain, loss, and weak-market zipcodes to return in each list. Minimum 1, maximum 10.",
                ge=1,
                le=10,
            ),
        ] = 5,
        top_competitors: Annotated[
            int,
            Field(
                description="How many competitor transition summaries to return. Minimum 1, maximum 10.",
                ge=1,
                le=10,
            ),
        ] = 5,
    ) -> BuyBoxStatusSummary:
        resolved_source = await _resolve_source_query(source_query)
        our_seller_name = await _fetch_our_seller_name()
        latest_selector = _normalize_as_of_selector(latest_as_of) if latest_as_of else None
        previous_selector = _normalize_as_of_selector(previous_as_of) if previous_as_of else None

        if latest_selector is None:
            recent_runs = await _fetch_completed_runs(resolved_source.source_slug, limit=recent_run_count)
            if len(recent_runs) < 2:
                raise ValueError(
                    "buybox_featured_offer_percent_summarize_status requires at least two completed runs for the resolved source."
                )
            latest_run_doc = recent_runs[0]
        else:
            latest_run_doc = await _fetch_completed_run_at_or_before(
                resolved_source.source_slug,
                selector=latest_selector,
            )
            if latest_run_doc is None:
                raise ValueError(
                    f"No completed run exists for source {resolved_source.source_slug} at or before latest_as_of={latest_as_of}."
                )
            recent_runs = await _fetch_completed_runs_up_to(
                resolved_source.source_slug,
                latest_received_at_or_before=str(latest_run_doc["received_at"]),
                limit=recent_run_count,
            )

        if previous_selector is None:
            previous_run_doc = await _fetch_completed_run_before(
                resolved_source.source_slug,
                received_at=str(latest_run_doc["received_at"]),
            )
        else:
            previous_run_doc = await _fetch_completed_run_at_or_before(
                resolved_source.source_slug,
                selector=previous_selector,
            )

        if previous_run_doc is None:
            raise ValueError(
                "buybox_featured_offer_percent_summarize_status could not find an older completed run for comparison."
            )

        if str(previous_run_doc["received_at"]) >= str(latest_run_doc["received_at"]):
            older_run = await _fetch_completed_run_before(
                resolved_source.source_slug,
                received_at=str(latest_run_doc["received_at"]),
            )
            if older_run is None:
                raise ValueError(
                    "buybox_featured_offer_percent_summarize_status needs two distinct completed runs, but the requested selectors resolved to the same run."
                )
            previous_run_doc = older_run

        if str(previous_run_doc["received_at"]) not in {str(run["received_at"]) for run in recent_runs}:
            recent_runs.append(previous_run_doc)
        recent_runs = sorted(recent_runs, key=lambda run: str(run["received_at"]), reverse=True)

        latest_received_at = str(latest_run_doc["received_at"])
        previous_received_at = str(previous_run_doc["received_at"])
        trend_received_at_values = [str(run["received_at"]) for run in recent_runs]
        comparison_received_at_values = [previous_received_at, latest_received_at]

        trend_rows = await _run_aggregate_query(
            "bb2_offers",
            _winner_per_cell_pipeline(
                {
                    "source_slug": resolved_source.source_slug,
                    "received_at": {"$in": trend_received_at_values},
                }
            )
            + [
                {
                    "$group": {
                        "_id": "$received_at",
                        "total_cells": {"$sum": 1},
                        "our_wins": {
                            "$sum": {
                                "$cond": [{"$eq": ["$winner_seller_name", our_seller_name]}, 1, 0]
                            }
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "received_at": "$_id",
                        "total_cells": 1,
                        "our_wins": 1,
                        "our_share": {
                            "$round": [{"$divide": ["$our_wins", "$total_cells"]}, 4]
                        },
                    }
                },
                {"$sort": {"received_at": -1}},
            ],
            limit=max(recent_run_count, 10),
        )
        trend_by_received_at = {str(row["received_at"]): row for row in trend_rows}

        def build_run_snapshot(run_document: dict[str, Any]) -> BuyBoxRunSnapshot:
            received_at = str(run_document["received_at"])
            trend_row = trend_by_received_at.get(received_at)
            if trend_row is None:
                raise ValueError(
                    f"Could not compute winner totals for run at received_at={received_at}."
                )
            result_set = run_document.get("result_set") or {}
            return BuyBoxRunSnapshot(
                received_at=received_at,
                result_set_id=result_set.get("id"),
                observed_cell_count=run_document.get("observed_cell_count"),
                missing_cell_count=run_document.get("missing_cell_count"),
                total_cells=int(trend_row["total_cells"]),
                our_wins=int(trend_row["our_wins"]),
                our_share=float(trend_row["our_share"]),
            )

        recent_trend = [build_run_snapshot(run_document) for run_document in recent_runs]
        latest_run = recent_trend[0]
        previous_run = recent_trend[1]

        overlap_rows = await _run_aggregate_query(
            "bb2_offers",
            _winner_per_cell_pipeline(
                {
                    "source_slug": resolved_source.source_slug,
                    "received_at": {"$in": comparison_received_at_values},
                }
            )
            + [
                {
                    "$group": {
                        "_id": "$tracking_key",
                        "run_count": {"$sum": 1},
                        "previous_winner": {"$first": "$winner_seller_name"},
                        "latest_winner": {"$last": "$winner_seller_name"},
                    }
                },
                {"$match": {"run_count": 2}},
                {
                    "$project": {
                        "_id": 0,
                        "gain": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$previous_winner", our_seller_name]},
                                        {"$eq": ["$latest_winner", our_seller_name]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        },
                        "loss": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$eq": ["$previous_winner", our_seller_name]},
                                        {"$ne": ["$latest_winner", our_seller_name]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        },
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "overlap_cells": {"$sum": 1},
                        "gains": {"$sum": "$gain"},
                        "losses": {"$sum": "$loss"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "overlap_cells": 1,
                        "gains": 1,
                        "losses": 1,
                        "net": {"$subtract": ["$gains", "$losses"]},
                    }
                },
            ],
            limit=1,
        )
        overlap_summary = overlap_rows[0] if overlap_rows else {
            "overlap_cells": 0,
            "gains": 0,
            "losses": 0,
            "net": 0,
        }

        zipcode_change_rows = await _run_aggregate_query(
            "bb2_offers",
            _winner_per_cell_pipeline(
                {
                    "source_slug": resolved_source.source_slug,
                    "received_at": {"$in": comparison_received_at_values},
                }
            )
            + [
                {
                    "$group": {
                        "_id": "$tracking_key",
                        "zipcode": {"$first": "$zipcode"},
                        "run_count": {"$sum": 1},
                        "previous_winner": {"$first": "$winner_seller_name"},
                        "latest_winner": {"$last": "$winner_seller_name"},
                    }
                },
                {"$match": {"run_count": 2}},
                {
                    "$project": {
                        "_id": 0,
                        "zipcode": 1,
                        "gain": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$previous_winner", our_seller_name]},
                                        {"$eq": ["$latest_winner", our_seller_name]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        },
                        "loss": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$eq": ["$previous_winner", our_seller_name]},
                                        {"$ne": ["$latest_winner", our_seller_name]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$zipcode",
                        "gains": {"$sum": "$gain"},
                        "losses": {"$sum": "$loss"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "zipcode": "$_id",
                        "gains": 1,
                        "losses": 1,
                        "net": {"$subtract": ["$gains", "$losses"]},
                    }
                },
                {"$sort": {"net": -1, "gains": -1, "zipcode": 1}},
            ],
            limit=50,
        )

        zipcode_share_rows = await _run_aggregate_query(
            "bb2_offers",
            _winner_per_cell_pipeline(
                {
                    "source_slug": resolved_source.source_slug,
                    "received_at": {"$in": comparison_received_at_values},
                }
            )
            + [
                {
                    "$group": {
                        "_id": "$zipcode",
                        "previous_total": {
                            "$sum": {
                                "$cond": [{"$eq": ["$received_at", previous_received_at]}, 1, 0]
                            }
                        },
                        "latest_total": {
                            "$sum": {
                                "$cond": [{"$eq": ["$received_at", latest_received_at]}, 1, 0]
                            }
                        },
                        "previous_wins": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$eq": ["$received_at", previous_received_at]},
                                            {"$eq": ["$winner_seller_name", our_seller_name]},
                                        ]
                                    },
                                    1,
                                    0,
                                ]
                            }
                        },
                        "latest_wins": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$eq": ["$received_at", latest_received_at]},
                                            {"$eq": ["$winner_seller_name", our_seller_name]},
                                        ]
                                    },
                                    1,
                                    0,
                                ]
                            }
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "zipcode": "$_id",
                        "previous_total": 1,
                        "latest_total": 1,
                        "previous_wins": 1,
                        "latest_wins": 1,
                        "previous_share": {
                            "$cond": [
                                {"$gt": ["$previous_total", 0]},
                                {"$round": [{"$divide": ["$previous_wins", "$previous_total"]}, 4]},
                                None,
                            ]
                        },
                        "latest_share": {
                            "$cond": [
                                {"$gt": ["$latest_total", 0]},
                                {"$round": [{"$divide": ["$latest_wins", "$latest_total"]}, 4]},
                                None,
                            ]
                        },
                        "share_change_pp": {
                            "$cond": [
                                {"$and": [{"$gt": ["$previous_total", 0]}, {"$gt": ["$latest_total", 0]}]},
                                {
                                    "$round": [
                                        {
                                            "$multiply": [
                                                {
                                                    "$subtract": [
                                                        {"$divide": ["$latest_wins", "$latest_total"]},
                                                        {"$divide": ["$previous_wins", "$previous_total"]},
                                                    ]
                                                },
                                                100,
                                            ]
                                        },
                                        2,
                                    ]
                                },
                                None,
                            ]
                        },
                    }
                },
                {"$sort": {"share_change_pp": 1, "zipcode": 1}},
            ],
            limit=50,
        )

        weakest_current_rows = await _run_aggregate_query(
            "bb2_offers",
            _winner_per_cell_pipeline(
                {
                    "source_slug": resolved_source.source_slug,
                    "received_at": latest_received_at,
                }
            )
            + [
                {
                    "$group": {
                        "_id": "$zipcode",
                        "latest_total": {"$sum": 1},
                        "latest_wins": {
                            "$sum": {
                                "$cond": [{"$eq": ["$winner_seller_name", our_seller_name]}, 1, 0]
                            }
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "zipcode": "$_id",
                        "latest_total": 1,
                        "latest_wins": 1,
                        "latest_share": {
                            "$round": [{"$divide": ["$latest_wins", "$latest_total"]}, 4]
                        },
                    }
                },
                {"$sort": {"latest_share": 1, "zipcode": 1}},
            ],
            limit=max(top_zipcodes, 20),
        )

        competitor_rows = await _run_aggregate_query(
            "bb2_offers",
            _winner_per_cell_pipeline(
                {
                    "source_slug": resolved_source.source_slug,
                    "received_at": {"$in": comparison_received_at_values},
                }
            )
            + [
                {
                    "$group": {
                        "_id": "$tracking_key",
                        "run_count": {"$sum": 1},
                        "previous_winner": {"$first": "$winner_seller_name"},
                        "latest_winner": {"$last": "$winner_seller_name"},
                    }
                },
                {"$match": {"run_count": 2}},
                {
                    "$project": {
                        "_id": 0,
                        "competitor": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {
                                            "$and": [
                                                {"$eq": ["$previous_winner", our_seller_name]},
                                                {"$ne": ["$latest_winner", our_seller_name]},
                                            ]
                                        },
                                        "then": "$latest_winner",
                                    },
                                    {
                                        "case": {
                                            "$and": [
                                                {"$ne": ["$previous_winner", our_seller_name]},
                                                {"$eq": ["$latest_winner", our_seller_name]},
                                            ]
                                        },
                                        "then": "$previous_winner",
                                    },
                                ],
                                "default": None,
                            }
                        },
                        "gain": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$previous_winner", our_seller_name]},
                                        {"$eq": ["$latest_winner", our_seller_name]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        },
                        "loss": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$eq": ["$previous_winner", our_seller_name]},
                                        {"$ne": ["$latest_winner", our_seller_name]},
                                    ]
                                },
                                1,
                                0,
                            ]
                        },
                    }
                },
                {"$match": {"competitor": {"$ne": None}}},
                {
                    "$group": {
                        "_id": "$competitor",
                        "gains_from_competitor": {"$sum": "$gain"},
                        "losses_to_competitor": {"$sum": "$loss"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "competitor": "$_id",
                        "gains_from_competitor": 1,
                        "losses_to_competitor": 1,
                        "net": {
                            "$subtract": ["$gains_from_competitor", "$losses_to_competitor"]
                        },
                    }
                },
                {"$sort": {"losses_to_competitor": -1, "gains_from_competitor": -1, "competitor": 1}},
            ],
            limit=max(top_competitors, 10),
        )

        zipcode_locations = await _fetch_zipcode_location_map()
        zipcode_summaries: dict[str, BuyBoxZipcodeSummary] = {}

        def get_zipcode_summary(zipcode: str) -> BuyBoxZipcodeSummary:
            if zipcode not in zipcode_summaries:
                location = zipcode_locations.get(zipcode, {})
                zipcode_summaries[zipcode] = BuyBoxZipcodeSummary(
                    zipcode=zipcode,
                    city=location.get("city"),
                    state=location.get("state"),
                )
            return zipcode_summaries[zipcode]

        for row in zipcode_change_rows:
            summary = get_zipcode_summary(str(row["zipcode"]))
            summary.gains = int(row.get("gains", 0))
            summary.losses = int(row.get("losses", 0))
            summary.net = int(row.get("net", 0))

        for row in zipcode_share_rows:
            summary = get_zipcode_summary(str(row["zipcode"]))
            summary.previous_total = row.get("previous_total")
            summary.latest_total = row.get("latest_total")
            summary.previous_wins = row.get("previous_wins")
            summary.latest_wins = row.get("latest_wins")
            summary.previous_share = row.get("previous_share")
            summary.latest_share = row.get("latest_share")
            summary.share_change_pp = row.get("share_change_pp")

        for row in weakest_current_rows:
            summary = get_zipcode_summary(str(row["zipcode"]))
            summary.latest_total = row.get("latest_total")
            summary.latest_wins = row.get("latest_wins")
            summary.latest_share = row.get("latest_share")

        top_gaining_zipcodes = [
            summary
            for summary in sorted(
                zipcode_summaries.values(),
                key=lambda item: (-item.net, -item.gains, item.zipcode),
            )
            if summary.net > 0
        ][:top_zipcodes]
        top_losing_zipcodes = [
            summary
            for summary in sorted(
                zipcode_summaries.values(),
                key=lambda item: (item.net, -item.losses, item.zipcode),
            )
            if summary.net < 0
        ][:top_zipcodes]
        weakest_current_zipcodes = [
            summary
            for summary in sorted(
                zipcode_summaries.values(),
                key=lambda item: (
                    item.latest_share if item.latest_share is not None else 1.0,
                    item.zipcode,
                ),
            )
            if summary.latest_share is not None
        ][:top_zipcodes]

        competitor_changes = [
            BuyBoxCompetitorChange(**row) for row in competitor_rows[:top_competitors]
        ]

        share_delta_points = round((latest_run.our_share - previous_run.our_share) * 100, 2)
        raw_win_delta = latest_run.our_wins - previous_run.our_wins
        overlap_net_change = int(overlap_summary["net"])

        if share_delta_points == 0 and overlap_net_change == 0:
            status_direction: Literal["gaining", "losing", "mixed", "flat"] = "flat"
        elif share_delta_points > 0 and overlap_net_change > 0:
            status_direction = "gaining"
        elif share_delta_points < 0 and overlap_net_change < 0:
            status_direction = "losing"
        else:
            status_direction = "mixed"

        notes = [
            (
                f"Compared latest completed run at {latest_run.received_at} to prior completed run "
                f"at {previous_run.received_at}."
            ),
            (
                f"Coverage moved from {previous_run.total_cells} observed winner cells to "
                f"{latest_run.total_cells} observed winner cells."
            ),
            (
                "Run alignment uses bb2_offer_runs.received_at and bb2_offer_runs.result_set.id rather than "
                "bb2_offers.meta.run_id."
            ),
        ]
        if latest_as_of or previous_as_of:
            notes.append(
                "Explicit date or timestamp selectors were provided. Each side of the comparison uses the latest completed run at or before the requested selector."
            )
        if len(resolved_source.alternatives_considered) > 1:
            notes.append(
                "Source resolution considered multiple matches and selected the best candidate by match score, recency, and observed coverage."
            )

        return BuyBoxStatusSummary(
            source=resolved_source,
            canonical_seller_name=our_seller_name,
            status_direction=status_direction,
            latest_run=latest_run,
            previous_run=previous_run,
            recent_trend=recent_trend,
            raw_win_delta=raw_win_delta,
            share_delta_points=share_delta_points,
            overlap_cells_compared=int(overlap_summary["overlap_cells"]),
            overlap_gains=int(overlap_summary["gains"]),
            overlap_losses=int(overlap_summary["losses"]),
            overlap_net_change=overlap_net_change,
            top_gaining_zipcodes=top_gaining_zipcodes,
            top_losing_zipcodes=top_losing_zipcodes,
            weakest_current_zipcodes=weakest_current_zipcodes,
            competitor_changes=competitor_changes,
            notes=notes,
            collections_used=[
                "bb2_offer_settings",
                "bb2_offer_sources",
                "bb2_offer_runs",
                "bb2_offers",
                "bb2_zipcode_locations",
            ],
            key_fields_used=[
                "bb2_offer_settings.our_seller_name",
                "bb2_offer_sources.name",
                "bb2_offer_sources.slug",
                "bb2_offer_sources.slug_lower",
                "bb2_offer_sources.webhook_slug_lower",
                "bb2_offer_runs.source_slug",
                "bb2_offer_runs.status",
                "bb2_offer_runs.received_at",
                "bb2_offer_runs.result_set.id",
                "bb2_offer_runs.observed_cell_count",
                "bb2_offer_runs.missing_cell_count",
                "bb2_offers.source_slug",
                "bb2_offers.received_at",
                "bb2_offers.tracking_key",
                "bb2_offers.offer_index",
                "bb2_offers.seller_name",
                "bb2_offers.zipcode",
                "bb2_zipcode_locations.zipcode",
                "bb2_zipcode_locations.city",
                "bb2_zipcode_locations.state",
            ],
        )

    @mcp.tool(
        title="List Keyword Tracking Sources",
        description=(
            "List the available keyword/search-term sources, their backing search-term collections, tracked keyword "
            "counts, and latest observed snapshot timestamps. Use this first to orient yourself before asking rank "
            "tracking questions."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_list_sources() -> list[KeywordTrackingSourceSummary]:
        return await _list_keyword_tracking_sources()

    @mcp.tool(
        title="List Keyword Tracking Collections",
        description=(
            "List the keyword/search-term collections available for scoped ad hoc querying. This surface is "
            "limited to keyword registries, query-volume tables, and search-term snapshot collections so agents "
            "do not drift into unrelated buy-box data."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_list_collections() -> list[ScopedCollectionSummary]:
        return await _list_keyword_tracking_collection_summaries()

    @mcp.tool(
        title="Inspect Keyword Tracking Collection",
        description=(
            "Inspect one keyword/search-term collection: approximate count, sample fields, nested field paths, "
            "indexes, and sample documents. This tool is scoped away from buy-box and inventory collections."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_inspect_collection(
        collection_name: Annotated[
            str,
            Field(description="Exact keyword/search-term collection name to inspect."),
        ],
        sample_size: Annotated[
            int,
            Field(description="How many sample documents to return. Maximum 5.", ge=1, le=MAX_SAMPLE_SIZE),
        ] = 2,
    ) -> ScopedCollectionProfile:
        return await _build_keyword_tracking_collection_profile(collection_name, sample_size=sample_size)

    @mcp.tool(
        title="Find Keyword Tracking Documents",
        description=(
            "Run a read-only MongoDB find query against one keyword/search-term collection. Use this for raw snapshot "
            "rows, tracked-keyword lookups, source registry lookups, and query-volume spot checks without exposing "
            "the broader buy-box collections."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_find_documents(
        collection_name: Annotated[
            str,
            Field(description="Exact keyword/search-term collection name to query."),
        ],
        filter: Annotated[
            dict[str, Any],
            Field(description="MongoDB filter document. Use {} to match all documents."),
        ] = Field(default_factory=dict),
        projection: Annotated[
            dict[str, Any] | None,
            Field(description="Optional MongoDB projection document, for example {'_id': 0, 'keyword': 1}."),
        ] = None,
        sort: Annotated[
            list[SortField] | None,
            Field(
                description=(
                    "Optional sort order as a list of objects. Example: "
                    "[{'field': 'received_at', 'direction': -1}]. Do not pass strings like 'received_at:-1'."
                )
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(description="Maximum number of documents to return. Hard-capped at 200.", ge=1, le=MAX_FIND_LIMIT),
        ] = 25,
    ) -> QueryResult:
        _, collection = await _require_keyword_tracking_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_FIND_LIMIT)
        documents = await _execute_find_query(
            collection,
            filter=filter,
            projection=projection,
            sort=sort,
            limit=normalized_limit,
        )
        return QueryResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            documents=documents,
        )

    @mcp.tool(
        title="Distinct Keyword Tracking Values",
        description=(
            "Return distinct values for one field in one keyword/search-term collection, with an optional filter. "
            "Use this to discover tracked source slugs, keywords, ASINs, dates, or search terms without traversing "
            "the larger buy-box dataset."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_distinct_values(
        collection_name: Annotated[
            str,
            Field(description="Exact keyword/search-term collection name to query."),
        ],
        field_name: Annotated[
            str,
            Field(description="Field path to inspect, for example source_slug, keyword, asin, search_term, or received_at."),
        ],
        filter: Annotated[
            dict[str, Any],
            Field(description="Optional MongoDB filter document used before collecting distinct values."),
        ] = Field(default_factory=dict),
        limit: Annotated[
            int,
            Field(description="Maximum number of distinct values to return. Hard-capped at 200.", ge=1, le=MAX_DISTINCT_LIMIT),
        ] = 50,
    ) -> DistinctResult:
        _, collection = await _require_keyword_tracking_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_DISTINCT_LIMIT)
        values = await _execute_distinct_query(
            collection,
            field_name=field_name,
            filter=filter,
            limit=normalized_limit,
        )
        return DistinctResult(
            collection=collection_name,
            field=field_name,
            returned_count=len(values),
            limit_applied=normalized_limit,
            values=values,
        )

    @mcp.tool(
        title="Aggregate Keyword Tracking Documents",
        description=(
            "Run a read-only MongoDB aggregation pipeline against one keyword/search-term collection. Use this for "
            "keyword grouping, search-result time series, ASIN rank rollups, and query-volume summaries while staying "
            "scoped to the keyword-tracking collections. Write stages like $out and $merge are blocked."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_aggregate_documents(
        collection_name: Annotated[
            str,
            Field(description="Exact keyword/search-term collection name to query."),
        ],
        pipeline: Annotated[
            list[dict[str, Any]],
            Field(description="MongoDB aggregation pipeline as a JSON array of stages."),
        ],
        limit: Annotated[
            int,
            Field(description="Maximum number of result rows to return if the pipeline does not already limit itself.", ge=1, le=MAX_AGGREGATE_LIMIT),
        ] = 100,
    ) -> AggregateResult:
        _, collection = await _require_keyword_tracking_collection(collection_name)
        pipeline_to_run, documents, normalized_limit = await _execute_aggregate_query(
            collection,
            pipeline=pipeline,
            limit=limit,
            tool_name="keyword_rank_tracking_aggregate_documents",
        )
        return AggregateResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            pipeline_executed=_jsonable(pipeline_to_run),
            documents=documents,
        )

    @mcp.tool(
        title="Resolve Keyword Tracking Source",
        description=(
            "Resolve a human source name, partial slug, or backing collection name into the canonical keyword "
            "tracking source and its search-term collection."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_resolve_source(
        source_query: Annotated[
            str,
            Field(description="Human source name or slug fragment, for example fireplaces or Treasure Garden Branded."),
        ],
    ) -> ResolvedKeywordSource:
        return await _resolve_keyword_source_query(source_query)

    @mcp.tool(
        title="Resolve Tracked Keyword",
        description=(
            "Resolve a keyword into the canonical tracked keyword row, including its source_slug, source_name, "
            "search-term collection, active flag, and freshness metadata."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_resolve_keyword(
        keyword_query: Annotated[
            str,
            Field(description="Keyword or search-term text, for example gas fireplace logs or patio umbrella."),
        ],
        source_query: Annotated[
            str | None,
            Field(
                description=(
                    "Optional source name or slug to narrow the match when the same term may exist in more than one source."
                )
            ),
        ] = None,
        active_only: Annotated[
            bool,
            Field(description="Whether to limit the match to tracked keywords whose is_active flag is true."),
        ] = True,
    ) -> ResolvedTrackedKeyword:
        resolved_source = await _resolve_keyword_source_query(source_query) if source_query else None
        return await _resolve_tracked_keyword_query(
            keyword_query,
            source_slug=resolved_source.source_slug if resolved_source else None,
            active_only=active_only,
        )

    @mcp.tool(
        title="Resolve Tracked ASIN",
        description=(
            "Resolve an ASIN against the tracked-ASIN registry used in keyword/search-term workflows. Use this to "
            "confirm whether an ASIN is part of the tracked set before analyzing rank history."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_resolve_asin(
        asin_query: Annotated[
            str,
            Field(description="ASIN or ASIN fragment, for example B000E86AKC."),
        ],
    ) -> ResolvedTrackedAsin:
        return await _resolve_tracked_asin_query(asin_query)

    @mcp.tool(
        title="Latest Keyword Search Results",
        description=(
            "Return the latest captured ranked search results for one keyword within one source. This is the fastest "
            "way to inspect the newest observed positions, prices, sponsorship flags, and which results belong to the "
            "tracked ASIN set."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_latest_search_results(
        search_term_query: Annotated[
            str,
            Field(description="Keyword or search-term text to inspect."),
        ],
        source_query: Annotated[
            str | None,
            Field(
                description=(
                    "Optional source name or slug. If omitted, the tool tries to infer the correct source from the "
                    "tracked keyword registry or the search-term collections."
                )
            ),
        ] = None,
        tracked_asins_only: Annotated[
            bool,
            Field(description="Whether to keep only rows whose ASIN appears in kw_tracked_asins."),
        ] = False,
        active_only: Annotated[
            bool,
            Field(description="Whether source inference should prefer active tracked keywords only."),
        ] = True,
        limit: Annotated[
            int,
            Field(description="Maximum number of ranked results to return. Minimum 1, maximum 100.", ge=1, le=MAX_KEYWORD_RESULT_LIMIT),
        ] = 25,
    ) -> KeywordSnapshotSummary:
        normalized_limit = _normalize_limit(limit, max_limit=MAX_KEYWORD_RESULT_LIMIT)
        resolved_source, resolved_keyword, canonical_search_term = await _resolve_keyword_scope(
            search_term_query,
            source_query=source_query,
            active_only=active_only,
        )
        latest_document = await _find_latest_search_term_document(
            resolved_source.search_terms_collection,
            canonical_search_term,
        )
        if not latest_document:
            raise ValueError(
                f"No search-term snapshot matched {canonical_search_term!r} in {resolved_source.search_terms_collection}."
            )

        tracked_asins = await _fetch_tracked_asin_set()
        all_rows = [
            _build_keyword_search_result_row(row, tracked_asins=tracked_asins)
            for row in latest_document.get("search_results", []) or []
        ]
        all_rows.sort(key=lambda row: (row.position is None, row.position or 10**9, row.asin))
        tracked_result_count = sum(1 for row in all_rows if row.tracked_asin)
        result_rows = [row for row in all_rows if row.tracked_asin] if tracked_asins_only else all_rows

        notes = [
            "Each row comes from search_results inside the latest captured snapshot for this search term."
        ]
        if resolved_keyword is None:
            notes.append("The term was resolved from the search-term snapshots rather than from kw_tracked_keywords.")
        if tracked_asins_only:
            notes.append("Only ASINs present in kw_tracked_asins were returned.")
        return KeywordSnapshotSummary(
            source=resolved_source,
            resolved_keyword=resolved_keyword,
            search_term=str(latest_document.get("search_term", canonical_search_term)),
            received_at=str(latest_document.get("received_at", "")),
            result_count=len(all_rows),
            tracked_result_count=tracked_result_count,
            top_results=result_rows[:normalized_limit],
            notes=notes,
            collections_used=[
                resolved_source.search_terms_collection,
                "kw_tracked_asins",
                *([] if resolved_keyword is None else ["kw_tracked_keywords"]),
                *([] if resolved_source.source_slug == DEFAULT_KEYWORD_SOURCE_SLUG else ["kw_sources"]),
            ],
            key_fields_used=[
                "search_term",
                "received_at",
                "search_results[].asin",
                "search_results[].position",
                "search_results[].price",
                "search_results[].sponsored",
                "kw_tracked_asins.asin",
            ],
        )

    @mcp.tool(
        title="Keyword Rank History For ASIN",
        description=(
            "Trace one ASIN's rank position across repeated snapshots for one keyword. The tool reconstructs history "
            "from search_results[].position over received_at timestamps because there is no separate rank-history table."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_rank_history(
        search_term_query: Annotated[
            str,
            Field(description="Keyword or search-term text to inspect."),
        ],
        asin: Annotated[
            str,
            Field(description="ASIN whose rank history to trace."),
        ],
        source_query: Annotated[
            str | None,
            Field(
                description=(
                    "Optional source name or slug. If omitted, the tool tries to infer the source from the tracked "
                    "keyword registry or the search-term collections."
                )
            ),
        ] = None,
        active_only: Annotated[
            bool,
            Field(description="Whether source inference should prefer active tracked keywords only."),
        ] = True,
        include_absent_snapshots: Annotated[
            bool,
            Field(
                description=(
                    "Whether to include snapshots where the ASIN was not present in the captured result set. "
                    "When false, the history output keeps only snapshots where the ASIN was observed."
                )
            ),
        ] = False,
        snapshot_limit: Annotated[
            int,
            Field(description="How many of the most recent snapshots to examine. Minimum 1, maximum 60.", ge=1, le=MAX_KEYWORD_HISTORY_LIMIT),
        ] = 30,
    ) -> KeywordRankHistorySummary:
        normalized_asin = asin.strip().upper()
        if not normalized_asin:
            raise ValueError("asin must not be empty")

        resolved_source, resolved_keyword, canonical_search_term = await _resolve_keyword_scope(
            search_term_query,
            source_query=source_query,
            active_only=active_only,
        )
        documents = await _find_search_term_documents(
            resolved_source.search_terms_collection,
            canonical_search_term,
            limit=snapshot_limit,
        )
        if not documents:
            raise ValueError(
                f"No search-term snapshots matched {canonical_search_term!r} in {resolved_source.search_terms_collection}."
            )

        history_all: list[KeywordRankHistoryPoint] = []
        for document in documents:
            row = _extract_keyword_search_result_for_asin(document, asin=normalized_asin)
            history_all.append(
                KeywordRankHistoryPoint(
                    received_at=str(document.get("received_at", "")),
                    present=row is not None,
                    position=_coerce_optional_int(row.get("position")) if row else None,
                    price=row.get("price") if row else None,
                    rating=row.get("rating") if row else None,
                    ratings_total=_coerce_optional_int(row.get("ratings_total")) if row else None,
                    sponsored=_coerce_optional_bool(row.get("sponsored")) if row else None,
                )
            )

        present_points = [point for point in history_all if point.present]
        positions = [point.position for point in present_points if point.position is not None]
        filtered_history = history_all if include_absent_snapshots else present_points
        resolved_asin = await _maybe_resolve_tracked_asin_query(normalized_asin)

        notes = [
            "History is reconstructed from search_results[].position across the selected search-term snapshots."
        ]
        if resolved_keyword is None:
            notes.append("The term was resolved from the search-term snapshots rather than from kw_tracked_keywords.")
        if resolved_asin is None:
            notes.append("The ASIN does not appear in kw_tracked_asins, but history was still computed from raw snapshots.")
        if history_all and not history_all[0].present:
            notes.append("The ASIN was absent from the newest snapshot in scope.")
        if not include_absent_snapshots:
            notes.append("Snapshots where the ASIN was absent were omitted from history.")
        if not present_points:
            notes.append("The ASIN was not observed in any of the snapshots examined.")

        return KeywordRankHistorySummary(
            source=resolved_source,
            resolved_keyword=resolved_keyword,
            resolved_asin=resolved_asin,
            search_term=str(documents[0].get("search_term", canonical_search_term)),
            asin=normalized_asin,
            latest_position=history_all[0].position if history_all else None,
            best_position=min(positions) if positions else None,
            worst_position=max(positions) if positions else None,
            appearance_count=len(present_points),
            snapshot_count=len(history_all),
            first_seen_at=present_points[-1].received_at if present_points else None,
            last_seen_at=present_points[0].received_at if present_points else None,
            history=filtered_history,
            notes=notes,
            collections_used=[
                resolved_source.search_terms_collection,
                *([] if resolved_keyword is None else ["kw_tracked_keywords"]),
                *([] if resolved_asin is None else ["kw_tracked_asins"]),
                *([] if resolved_source.source_slug == DEFAULT_KEYWORD_SOURCE_SLUG else ["kw_sources"]),
            ],
            key_fields_used=[
                "search_term",
                "received_at",
                "search_results[].asin",
                "search_results[].position",
                "search_results[].price",
                "search_results[].rating",
                "search_results[].ratings_total",
                "search_results[].sponsored",
            ],
        )

    @mcp.tool(
        title="Keyword Search Query Volume",
        description=(
            "Return uploaded search query volume rows for a keyword from kw_search_query_volumes, optionally scoped "
            "to one keyword source. Use uploaded_at to judge freshness."
        ),
        structured_output=False,
    )
    async def keyword_rank_tracking_search_query_volume(
        keyword_query: Annotated[
            str,
            Field(description="Keyword or search-term text whose volume rows you want to inspect."),
        ],
        source_query: Annotated[
            str | None,
            Field(description="Optional source name or slug to limit the lookup to one keyword source."),
        ] = None,
        active_only: Annotated[
            bool,
            Field(description="Whether source inference should prefer active tracked keywords only."),
        ] = True,
        limit: Annotated[
            int,
            Field(description="Maximum number of volume rows to return. Minimum 1, maximum 100.", ge=1, le=MAX_KEYWORD_VOLUME_LIMIT),
        ] = 25,
    ) -> KeywordSearchQueryVolumeSummary:
        normalized_limit = _normalize_limit(limit, max_limit=MAX_KEYWORD_VOLUME_LIMIT)
        resolved_source: ResolvedKeywordSource | None = None
        resolved_keyword: ResolvedTrackedKeyword | None = None
        canonical_keyword = _normalize_text(keyword_query)
        if not canonical_keyword:
            raise ValueError("keyword_query must not be empty")

        if source_query:
            resolved_source = await _resolve_keyword_source_query(source_query)
            try:
                resolved_keyword = await _resolve_tracked_keyword_query(
                    keyword_query,
                    source_slug=resolved_source.source_slug,
                    active_only=active_only,
                )
                canonical_keyword = _normalize_text(resolved_keyword.keyword)
            except ValueError:
                canonical_keyword = _normalize_text(keyword_query)
        else:
            try:
                resolved_keyword = await _resolve_tracked_keyword_query(
                    keyword_query,
                    active_only=active_only,
                )
                resolved_source = await _resolve_keyword_source_from_tracked_keyword(
                    {
                        "source_slug": resolved_keyword.source_slug,
                        "source_name": resolved_keyword.source_name,
                        "search_terms_collection": resolved_keyword.search_terms_collection,
                    },
                    query=keyword_query,
                )
                canonical_keyword = _normalize_text(resolved_keyword.keyword)
            except ValueError:
                canonical_keyword = _normalize_text(keyword_query)

        _, collection = await _require_collection("kw_search_query_volumes")
        filter_doc: dict[str, Any] = {"keyword_lower": canonical_keyword}
        if resolved_source is not None:
            filter_doc["source_slug"] = resolved_source.source_slug
        cursor = collection.find(
            filter_doc,
            projection={
                "_id": 0,
                "keyword": 1,
                "source_slug": 1,
                "source_name": 1,
                "search_query_volume": 1,
                "uploaded_at": 1,
            },
            sort=[("uploaded_at", -1), ("source_slug", 1)],
            limit=normalized_limit,
            max_time_ms=15_000,
        )
        documents = _jsonable(await cursor.to_list(length=normalized_limit))
        if not documents:
            scope_note = f" in source_slug={resolved_source.source_slug}" if resolved_source else ""
            raise ValueError(f"No search query volume rows matched keyword_query{scope_note}.")

        if resolved_source is None:
            distinct_source_slugs = sorted({str(document.get("source_slug", "")) for document in documents if document.get("source_slug")})
            if len(distinct_source_slugs) == 1:
                resolved_source = await _resolve_keyword_source_query(distinct_source_slugs[0])

        notes = ["Rows are sorted by uploaded_at descending so the freshest volume uploads appear first."]
        if resolved_keyword is None:
            notes.append("The keyword was matched directly against kw_search_query_volumes rather than via kw_tracked_keywords.")
        if resolved_source is None:
            notes.append("Multiple sources may be represented because no single source was resolved.")

        return KeywordSearchQueryVolumeSummary(
            source=resolved_source,
            resolved_keyword=resolved_keyword,
            records=[
                KeywordSearchQueryVolumeRow(
                    keyword=str(document.get("keyword", "")),
                    source_slug=str(document.get("source_slug", "")),
                    source_name=_stringify_optional(document.get("source_name")),
                    search_query_volume=_coerce_int(document.get("search_query_volume", 0)),
                    uploaded_at=_stringify_optional(document.get("uploaded_at")),
                )
                for document in documents
            ],
            notes=notes,
            collections_used=[
                "kw_search_query_volumes",
                *([] if resolved_keyword is None else ["kw_tracked_keywords"]),
                *([] if resolved_source is None or resolved_source.source_slug == DEFAULT_KEYWORD_SOURCE_SLUG else ["kw_sources"]),
            ],
            key_fields_used=[
                "kw_search_query_volumes.keyword",
                "kw_search_query_volumes.keyword_lower",
                "kw_search_query_volumes.source_slug",
                "kw_search_query_volumes.search_query_volume",
                "kw_search_query_volumes.uploaded_at",
            ],
        )

    @mcp.tool(
        title="List Inventory Collections",
        description=(
            "List the operational INV-Tracker collections available for scoped ad hoc querying. This surface is "
            "limited to locations, skus, inventorylevels, amazonskualiases, and uncoveredskuobservations so agents "
            "do not drift into unrelated session or admin tables."
        ),
        structured_output=False,
    )
    async def inventory_by_location_list_collections() -> list[ScopedCollectionSummary]:
        return await _list_inventory_query_collection_summaries()

    @mcp.tool(
        title="Inspect Inventory Collection",
        description=(
            "Inspect one operational INV-Tracker collection: approximate count, sample fields, nested field paths, "
            "indexes, and sample documents. This tool is scoped away from buy-box and keyword-tracking collections."
        ),
        structured_output=False,
    )
    async def inventory_by_location_inspect_collection(
        collection_name: Annotated[
            str,
            Field(description="Exact inventory collection name to inspect."),
        ],
        sample_size: Annotated[
            int,
            Field(description="How many sample documents to return. Maximum 5.", ge=1, le=MAX_SAMPLE_SIZE),
        ] = 2,
    ) -> ScopedCollectionProfile:
        return await _build_inventory_query_collection_profile(collection_name, sample_size=sample_size)

    @mcp.tool(
        title="Find Inventory Documents",
        description=(
            "Run a read-only MongoDB find query against one operational INV-Tracker collection. Use this for raw "
            "stock rows, SKU metadata, alias lookups, location inspection, and uncovered-SKU diagnostics without "
            "exposing unrelated collections."
        ),
        structured_output=False,
    )
    async def inventory_by_location_find_documents(
        collection_name: Annotated[
            str,
            Field(description="Exact inventory collection name to query."),
        ],
        filter: Annotated[
            dict[str, Any],
            Field(description="MongoDB filter document. Use {} to match all documents."),
        ] = Field(default_factory=dict),
        projection: Annotated[
            dict[str, Any] | None,
            Field(description="Optional MongoDB projection document, for example {'_id': 0, 'sku': 1}."),
        ] = None,
        sort: Annotated[
            list[SortField] | None,
            Field(
                description=(
                    "Optional sort order as a list of objects. Example: "
                    "[{'field': 'lastSuccessfulIngestionAt', 'direction': -1}]. Do not pass strings like 'field:-1'."
                )
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(description="Maximum number of documents to return. Hard-capped at 200.", ge=1, le=MAX_FIND_LIMIT),
        ] = 25,
    ) -> QueryResult:
        _, collection = await _require_inventory_query_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_FIND_LIMIT)
        documents = await _execute_find_query(
            collection,
            filter=filter,
            projection=projection,
            sort=sort,
            limit=normalized_limit,
        )
        return QueryResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            documents=documents,
        )

    @mcp.tool(
        title="Distinct Inventory Values",
        description=(
            "Return distinct values for one field in one operational INV-Tracker collection, with an optional filter. "
            "Use this to discover SKUs, aliases, location IDs, location names, child ASINs, or uncovered observed SKUs "
            "without traversing unrelated collections."
        ),
        structured_output=False,
    )
    async def inventory_by_location_distinct_values(
        collection_name: Annotated[
            str,
            Field(description="Exact inventory collection name to query."),
        ],
        field_name: Annotated[
            str,
            Field(description="Field path to inspect, for example sku, locationId, name, childAsins, or observedSku."),
        ],
        filter: Annotated[
            dict[str, Any],
            Field(description="Optional MongoDB filter document used before collecting distinct values."),
        ] = Field(default_factory=dict),
        limit: Annotated[
            int,
            Field(description="Maximum number of distinct values to return. Hard-capped at 200.", ge=1, le=MAX_DISTINCT_LIMIT),
        ] = 50,
    ) -> DistinctResult:
        _, collection = await _require_inventory_query_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_DISTINCT_LIMIT)
        values = await _execute_distinct_query(
            collection,
            field_name=field_name,
            filter=filter,
            limit=normalized_limit,
        )
        return DistinctResult(
            collection=collection_name,
            field=field_name,
            returned_count=len(values),
            limit_applied=normalized_limit,
            values=values,
        )

    @mcp.tool(
        title="Aggregate Inventory Documents",
        description=(
            "Run a read-only MongoDB aggregation pipeline against one operational INV-Tracker collection. Use this for "
            "inventory rollups, location summaries, SKU diagnostics, alias coverage, and uncovered-SKU analysis while "
            "staying scoped to the inventory collections. Write stages like $out and $merge are blocked."
        ),
        structured_output=False,
    )
    async def inventory_by_location_aggregate_documents(
        collection_name: Annotated[
            str,
            Field(description="Exact inventory collection name to query."),
        ],
        pipeline: Annotated[
            list[dict[str, Any]],
            Field(description="MongoDB aggregation pipeline as a JSON array of stages."),
        ],
        limit: Annotated[
            int,
            Field(description="Maximum number of result rows to return if the pipeline does not already limit itself.", ge=1, le=MAX_AGGREGATE_LIMIT),
        ] = 100,
    ) -> AggregateResult:
        _, collection = await _require_inventory_query_collection(collection_name)
        pipeline_to_run, documents, normalized_limit = await _execute_aggregate_query(
            collection,
            pipeline=pipeline,
            limit=limit,
            tool_name="inventory_by_location_aggregate_documents",
        )
        return AggregateResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            pipeline_executed=_jsonable(pipeline_to_run),
            documents=documents,
        )

    @mcp.tool(
        title="Resolve Inventory Location",
        description=(
            "Resolve a human location name, warehouse nickname, or Amazon SKU suffix into the exact INV-Tracker "
            "location. Use this before scoping location-specific inventory questions."
        ),
        structured_output=False,
    )
    async def inventory_by_location_resolve_location(
        location_query: Annotated[
            str,
            Field(description="Human location name or suffix, for example Ontario, CA, Bath, PA, -MF, or -EC."),
        ],
    ) -> ResolvedInventoryLocation:
        return await _resolve_inventory_location_query(location_query)

    @mcp.tool(
        title="Resolve Inventory SKU",
        description=(
            "Resolve an internal SKU, SKU alias, product name fragment, or child ASIN into the canonical "
            "INV-Tracker SKU. Use this first when the user is not providing the exact internal SKU."
        ),
        structured_output=False,
    )
    async def inventory_by_location_resolve_sku(
        sku_query: Annotated[
            str,
            Field(
                description=(
                    "Internal SKU, SKU alias, child ASIN, or identifying text. "
                    "Examples: CHD-24-G45, CHD-24-G45-MF, or B000E86AKC."
                )
            ),
        ],
    ) -> ResolvedInventorySku:
        return await _resolve_inventory_sku_query(sku_query)

    @mcp.tool(
        title="Explain Inventory SKU",
        description=(
            "Explain how one INV-Tracker SKU is modeled: its type, aliases, child ASINs, assembly components, "
            "parent assemblies that depend on it, and its quantity footprint by location."
        ),
        structured_output=False,
    )
    async def inventory_by_location_explain_sku(
        sku_query: Annotated[
            str,
            Field(description="Internal SKU, alias, child ASIN, or identifying text."),
        ],
        location_query: Annotated[
            str | None,
            Field(
                description=(
                    "Optional location name or suffix to narrow the quantity footprint. "
                    "If omitted, the tool reports across all INV-Tracker locations."
                )
            ),
        ] = None,
        parent_limit: Annotated[
            int,
            Field(
                description="How many parent assemblies that depend on this SKU to include. Minimum 0, maximum 25.",
                ge=0,
                le=25,
            ),
        ] = 10,
    ) -> InventorySkuExplanation:
        resolved_sku = await _resolve_inventory_sku_query(sku_query)
        location_documents, selected_location = await _select_inventory_locations(location_query)
        sku_document = await _get_inventory_sku_document(resolved_sku)
        component_skus = [
            str(component.get("componentSku", ""))
            for component in sku_document.get("kitComponents", []) or []
            if component.get("componentSku")
        ]
        component_documents = (
            await _fetch_inventory_sku_documents({"sku": {"$in": sorted(set(component_skus))}})
            if component_skus
            else []
        )
        sku_catalog = {str(document.get("sku", "")): document for document in component_documents}
        components = _build_inventory_component_blueprint(sku_document, sku_catalog)
        quantity_summary = await _build_inventory_quantity_summary(
            resolved_sku,
            location_documents=location_documents,
            selected_location=selected_location,
            include_component_breakdown=False,
            notes=[],
        )

        parent_documents = (
            await _fetch_inventory_sku_documents({"kitComponents.componentSku": resolved_sku.sku})
            if parent_limit > 0
            else []
        )
        parent_skus = [
            ResolvedInventorySku(
                query=resolved_sku.sku,
                sku=str(document.get("sku", "")),
                name=str(document.get("name", "")),
                sku_type=str(document.get("type", "")),
                aliases=[str(value) for value in document.get("aliases", []) if value],
                child_asins=[str(value) for value in document.get("childAsins", []) if value],
                is_indirect_component=document.get("isIndirectComponent"),
                component_count=len(document.get("kitComponents", []) or []),
                matched_on="component_reference",
                alternatives_considered=[],
            )
            for document in sorted(parent_documents, key=_inventory_sku_sort_key)[:parent_limit]
        ]
        positive_location_count = sum(
            1
            for row in quantity_summary.quantities
            if row.recorded_quantity > 0 or (row.buildable_quantity or 0) > 0
        )
        notes = list(quantity_summary.notes)
        if components:
            notes.append("This SKU is an assembly. components lists the bill of materials used to derive buildable_quantity.")
        if parent_skus:
            notes.append("parent_skus lists kits or options that directly reference this SKU in skus.kitComponents.")
        return InventorySkuExplanation(
            resolved_sku=resolved_sku,
            selected_location=selected_location,
            components=components,
            parent_skus=parent_skus,
            quantities=quantity_summary.quantities,
            location_count_with_inventory_rows=sum(1 for row in quantity_summary.quantities if row.has_recorded_row),
            positive_location_count=positive_location_count,
            notes=notes,
        )

    @mcp.tool(
        title="Inventory Quantity By Location",
        description=(
            "Return recorded INV-Tracker quantity for one SKU by location. For kit and option SKUs, the response "
            "also includes buildable_quantity derived from component stock, but keeps it separate from the recorded "
            "kit quantity."
        ),
        structured_output=False,
    )
    async def inventory_by_location_quantity(
        sku_query: Annotated[
            str,
            Field(description="Internal SKU, alias, child ASIN, or identifying text."),
        ],
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns all locations."),
        ] = None,
        include_zero_locations: Annotated[
            bool,
            Field(description="Whether to include locations whose recorded and buildable quantities are both zero."),
        ] = True,
    ) -> InventoryQuantitySummary:
        resolved_sku = await _resolve_inventory_sku_query(sku_query)
        location_documents, selected_location = await _select_inventory_locations(location_query)
        summary = await _build_inventory_quantity_summary(
            resolved_sku,
            location_documents=location_documents,
            selected_location=selected_location,
            include_component_breakdown=False,
            notes=["Use this tool for the direct quantity footprint of a SKU across locations."],
        )
        if include_zero_locations:
            return summary
        filtered_rows = [
            row
            for row in summary.quantities
            if row.recorded_quantity > 0 or (row.buildable_quantity or 0) > 0
        ]
        return _rebuild_inventory_quantity_summary(
            summary,
            filtered_rows,
            extra_note="Locations whose recorded_quantity and buildable_quantity were both zero were omitted.",
        )

    @mcp.tool(
        title="Buildable Quantity By Location",
        description=(
            "Return buildable_quantity for one kit or option SKU by location. buildable_quantity is derived from "
            "component stock using skus.kitComponents and is reported alongside any direct recorded kit quantity."
        ),
        structured_output=False,
    )
    async def inventory_by_location_buildable_quantity(
        sku_query: Annotated[
            str,
            Field(description="Kit or option SKU, alias, child ASIN, or identifying text."),
        ],
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns all locations."),
        ] = None,
        include_zero_locations: Annotated[
            bool,
            Field(description="Whether to include locations whose recorded and buildable quantities are both zero."),
        ] = True,
    ) -> InventoryQuantitySummary:
        resolved_sku = await _resolve_inventory_sku_query(sku_query)
        if resolved_sku.sku_type not in {"kit", "option"}:
            raise ValueError(
                "inventory_by_location_buildable_quantity is intended for kit and option SKUs. "
                "Use inventory_by_location_quantity for component SKUs."
            )
        location_documents, selected_location = await _select_inventory_locations(location_query)
        summary = await _build_inventory_quantity_summary(
            resolved_sku,
            location_documents=location_documents,
            selected_location=selected_location,
            include_component_breakdown=False,
            notes=["Use this tool when the user wants to know how many assembled units can be built from component stock."],
        )
        if include_zero_locations:
            return summary
        filtered_rows = [
            row
            for row in summary.quantities
            if row.recorded_quantity > 0 or (row.buildable_quantity or 0) > 0
        ]
        return _rebuild_inventory_quantity_summary(
            summary,
            filtered_rows,
            extra_note="Locations whose recorded_quantity and buildable_quantity were both zero were omitted.",
        )

    @mcp.tool(
        title="Inventory Component Constraints",
        description=(
            "Explain which components constrain a kit or option SKU at each location. The response identifies the "
            "limiting component and includes a per-component breakdown of required quantity, on-hand quantity, and "
            "component build limit."
        ),
        structured_output=False,
    )
    async def inventory_by_location_component_constraints(
        sku_query: Annotated[
            str,
            Field(description="Kit or option SKU, alias, child ASIN, or identifying text."),
        ],
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns all locations."),
        ] = None,
        include_zero_locations: Annotated[
            bool,
            Field(
                description=(
                    "Whether to include locations with no direct stock and no component stock. "
                    "Defaults to false so the response focuses on relevant locations."
                )
            ),
        ] = False,
    ) -> InventoryQuantitySummary:
        resolved_sku = await _resolve_inventory_sku_query(sku_query)
        if resolved_sku.sku_type not in {"kit", "option"}:
            raise ValueError(
                "inventory_by_location_component_constraints is intended for kit and option SKUs. "
                "Use inventory_by_location_quantity for component SKUs."
            )
        location_documents, selected_location = await _select_inventory_locations(location_query)
        summary = await _build_inventory_quantity_summary(
            resolved_sku,
            location_documents=location_documents,
            selected_location=selected_location,
            include_component_breakdown=True,
            notes=["Use this tool to explain why buildable_quantity is high, low, or zero at each location."],
        )
        if include_zero_locations:
            return summary
        filtered_rows = [
            row
            for row in summary.quantities
            if row.recorded_quantity > 0
            or (row.buildable_quantity or 0) > 0
            or any(component.on_hand_quantity > 0 for component in row.component_breakdown)
        ]
        return _rebuild_inventory_quantity_summary(
            summary,
            filtered_rows,
            extra_note="Locations with no direct stock and no component stock were omitted.",
        )

    @mcp.tool(
        title="Parent SKUs For Component",
        description=(
            "Return the kits and options that depend on one component SKU, along with their recorded and buildable "
            "quantities across the selected location scope. Use this to understand shortage impact."
        ),
        structured_output=False,
    )
    async def inventory_by_location_parent_skus_for_component(
        component_query: Annotated[
            str,
            Field(description="Component SKU, alias, child ASIN, or identifying text."),
        ],
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns all locations."),
        ] = None,
        limit: Annotated[
            int,
            Field(description="Maximum number of parent SKUs to return. Minimum 1, maximum 50.", ge=1, le=50),
        ] = 25,
    ) -> InventoryParentSkuSummary:
        resolved_component = await _resolve_inventory_sku_query(component_query)
        location_documents, selected_location = await _select_inventory_locations(location_query)
        parent_documents = await _fetch_inventory_sku_documents({"kitComponents.componentSku": resolved_component.sku})
        if not parent_documents:
            return InventoryParentSkuSummary(
                resolved_component=resolved_component,
                selected_location=selected_location,
                parent_skus=[],
                notes=["No kit or option currently references this SKU in skus.kitComponents."],
            )

        relevant_skus: set[str] = set()
        for document in parent_documents:
            relevant_skus.add(str(document.get("sku", "")))
            for component in document.get("kitComponents", []) or []:
                if component.get("componentSku"):
                    relevant_skus.add(str(component.get("componentSku", "")))

        sku_catalog_documents = await _fetch_inventory_sku_documents({"sku": {"$in": sorted(relevant_skus)}})
        sku_catalog = {str(document.get("sku", "")): document for document in sku_catalog_documents}
        location_ids = [str(document.get("_id", "")) for document in location_documents]
        inventory_documents = await _fetch_inventory_level_documents(
            skus=sorted(relevant_skus),
            location_ids=location_ids,
        )
        inventory_by_pair = {
            (str(document.get("locationId", "")), str(document.get("sku", ""))): _coerce_int(document.get("quantity", 0))
            for document in inventory_documents
        }

        parent_rows: list[InventoryParentSkuAvailability] = []
        for parent_document in parent_documents:
            quantity_rows = _build_inventory_location_quantity_rows(
                parent_document,
                location_documents=location_documents,
                inventory_by_pair=inventory_by_pair,
                sku_catalog=sku_catalog,
                include_component_breakdown=False,
            )
            total_recorded, total_buildable, locations_with_recorded, locations_with_buildable = _summarize_inventory_location_rows(quantity_rows)
            required_quantity = 0
            for component in parent_document.get("kitComponents", []) or []:
                if str(component.get("componentSku", "")) == resolved_component.sku:
                    required_quantity = max(_coerce_int(component.get("quantity", 1)), 1)
                    break
            parent_rows.append(
                InventoryParentSkuAvailability(
                    parent_sku=str(parent_document.get("sku", "")),
                    parent_name=str(parent_document.get("name", "")),
                    parent_sku_type=str(parent_document.get("type", "")),
                    required_quantity_of_component=required_quantity,
                    child_asins=[str(value) for value in parent_document.get("childAsins", []) if value],
                    total_recorded_quantity=total_recorded,
                    total_buildable_quantity=total_buildable,
                    locations_with_recorded_stock=locations_with_recorded,
                    locations_with_buildable_stock=locations_with_buildable,
                )
            )

        parent_rows.sort(
            key=lambda item: (
                -(item.total_buildable_quantity or 0),
                -item.total_recorded_quantity,
                item.parent_sku,
            )
        )
        notes = [
            "Each parent SKU summary reflects the selected location scope and uses component-derived buildable quantity for kit and option assemblies."
        ]
        if resolved_component.sku_type != "component":
            notes.append("The resolved SKU is not typed as component, but it is still referenced by parent assemblies.")
        return InventoryParentSkuSummary(
            resolved_component=resolved_component,
            selected_location=selected_location,
            parent_skus=parent_rows[:limit],
            notes=notes,
        )

    @mcp.tool(
        title="Availability For ASIN",
        description=(
            "Map one Amazon ASIN back to INV-Tracker stock by location using childAsins and amazonskualiases. "
            "The response includes the matched internal SKU when possible, the location-specific Amazon SKU alias, "
            "and recorded/buildable quantities."
        ),
        structured_output=False,
    )
    async def inventory_by_location_availability_for_asin(
        asin: Annotated[
            str,
            Field(description="Amazon ASIN to inspect, for example B000E86AKC."),
        ],
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns all matching locations."),
        ] = None,
    ) -> InventoryAsinAvailabilitySummary:
        normalized_asin = asin.strip().upper()
        if not normalized_asin:
            raise ValueError("asin must not be empty")

        location_documents, selected_location = await _select_inventory_locations(location_query)
        location_ids = [str(document.get("_id", "")) for document in location_documents]
        all_sku_documents = await _fetch_inventory_sku_documents()
        alias_documents = await _fetch_inventory_alias_documents(
            asin=normalized_asin,
            location_ids=location_ids,
        )
        alias_by_location = {
            str(document.get("locationId", "")): document
            for document in alias_documents
        }
        asin_match_documents = [
            document
            for document in all_sku_documents
            if _inventory_sku_matches_asin(document, normalized_asin)
        ]
        candidate_by_sku = {
            str(document.get("sku", "")): document
            for document in asin_match_documents
        }
        for alias_document in alias_documents:
            amazon_sku_alias = str(alias_document.get("amazonSkuAlias", ""))
            for document in all_sku_documents:
                if _inventory_sku_matches_alias(document, amazon_sku_alias):
                    candidate_by_sku[str(document.get("sku", ""))] = document

        candidate_documents = list(candidate_by_sku.values())
        relevant_skus: set[str] = {
            str(document.get("sku", ""))
            for document in candidate_documents
            if document.get("sku")
        }
        for document in candidate_documents:
            for component in document.get("kitComponents", []) or []:
                if component.get("componentSku"):
                    relevant_skus.add(str(component.get("componentSku", "")))
        sku_catalog_documents = (
            await _fetch_inventory_sku_documents({"sku": {"$in": sorted(relevant_skus)}})
            if relevant_skus
            else []
        )
        sku_catalog = {str(document.get("sku", "")): document for document in sku_catalog_documents}
        inventory_documents = await _fetch_inventory_level_documents(
            skus=sorted(relevant_skus),
            location_ids=location_ids,
        ) if relevant_skus else []
        inventory_by_pair = {
            (str(document.get("locationId", "")), str(document.get("sku", ""))): _coerce_int(document.get("quantity", 0))
            for document in inventory_documents
        }

        rows: list[InventoryAsinAvailabilityRow] = []
        for location in sorted(location_documents, key=_inventory_location_sort_key):
            location_id = str(location.get("_id", ""))
            alias_document = alias_by_location.get(location_id)
            amazon_sku_alias = _stringify_optional(alias_document.get("amazonSkuAlias")) if alias_document else None
            location_candidates = []
            if amazon_sku_alias:
                location_candidates = [
                    document
                    for document in candidate_documents
                    if _inventory_sku_matches_alias(document, amazon_sku_alias)
                ]
            if not location_candidates:
                location_candidates = [
                    document
                    for document in candidate_documents
                    if _inventory_sku_matches_asin(document, normalized_asin)
                ]

            if not location_candidates and amazon_sku_alias:
                rows.append(
                    InventoryAsinAvailabilityRow(
                        asin=normalized_asin,
                        location_id=location_id,
                        location_name=str(location.get("name", "")),
                        amazon_sku_suffix=_stringify_optional(location.get("amazonSkuSuffix")),
                        handling_time=location.get("handlingTime"),
                        last_successful_ingestion_at=_stringify_optional(location.get("lastSuccessfulIngestionAt")),
                        amazon_sku_alias=amazon_sku_alias,
                        resolved_sku=None,
                        resolved_sku_name=None,
                        resolved_sku_type=None,
                        recorded_quantity=None,
                        buildable_quantity=None,
                        alias_match=False,
                        child_asin_match=False,
                    )
                )
                continue

            for candidate in sorted(location_candidates, key=_inventory_sku_sort_key):
                candidate_sku = str(candidate.get("sku", ""))
                recorded_quantity = inventory_by_pair.get((location_id, candidate_sku))
                buildable_quantity: int | None = None
                if str(candidate.get("type", "")) in {"kit", "option"}:
                    _, buildable_quantity, _ = _build_inventory_component_availability(
                        candidate,
                        location_id=location_id,
                        inventory_by_pair=inventory_by_pair,
                        sku_catalog=sku_catalog,
                    )
                rows.append(
                    InventoryAsinAvailabilityRow(
                        asin=normalized_asin,
                        location_id=location_id,
                        location_name=str(location.get("name", "")),
                        amazon_sku_suffix=_stringify_optional(location.get("amazonSkuSuffix")),
                        handling_time=location.get("handlingTime"),
                        last_successful_ingestion_at=_stringify_optional(location.get("lastSuccessfulIngestionAt")),
                        amazon_sku_alias=amazon_sku_alias,
                        resolved_sku=candidate_sku,
                        resolved_sku_name=str(candidate.get("name", "")),
                        resolved_sku_type=str(candidate.get("type", "")),
                        recorded_quantity=recorded_quantity,
                        buildable_quantity=buildable_quantity,
                        alias_match=bool(amazon_sku_alias and _inventory_sku_matches_alias(candidate, amazon_sku_alias)),
                        child_asin_match=_inventory_sku_matches_asin(candidate, normalized_asin),
                    )
                )

        rows.sort(key=lambda item: (item.location_name, item.resolved_sku or "", item.amazon_sku_alias or ""))
        notes = [
            "The tool uses childAsins and amazonskualiases together. alias-only rows indicate a location-specific Amazon alias that did not map cleanly back to one internal SKU."
        ]
        if not rows:
            notes.append("No INV-Tracker SKU or Amazon alias matched this ASIN in the selected location scope.")
        return InventoryAsinAvailabilitySummary(
            asin=normalized_asin,
            selected_location=selected_location,
            matches=rows,
            notes=notes,
        )

    @mcp.tool(
        title="Uncovered SKU Gaps",
        description=(
            "Return the latest uncoveredskuobservations rows by location and observed SKU. Use this to find inventory "
            "items that were observed in ingestion but are not cleanly covered by the SKU model."
        ),
        structured_output=False,
    )
    async def inventory_by_location_uncovered_sku_gaps(
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns gaps across all locations."),
        ] = None,
        positive_only: Annotated[
            bool,
            Field(description="Whether to keep only uncovered SKUs whose latest observed quantity is greater than zero."),
        ] = False,
        limit: Annotated[
            int,
            Field(description="Maximum number of latest uncovered SKU rows to return. Minimum 1, maximum 100.", ge=1, le=100),
        ] = 25,
    ) -> InventoryUncoveredGapSummary:
        location_documents, selected_location = await _select_inventory_locations(location_query)
        location_map = {
            str(document.get("_id", "")): document
            for document in location_documents
        }
        location_ids = sorted(location_map)
        _, collection = await _require_inventory_collection("uncoveredskuobservations")
        pipeline: list[dict[str, Any]] = []
        if location_ids:
            pipeline.append({"$match": {"locationId": {"$in": location_ids}}})
        pipeline.extend(
            [
                {"$sort": {"observedAt": -1}},
                {
                    "$group": {
                        "_id": {"locationId": "$locationId", "observedSku": "$observedSku"},
                        "latestQuantity": {"$first": "$quantity"},
                        "latestObservedAt": {"$first": "$observedAt"},
                        "sourceMethod": {"$first": "$sourceMethod"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "locationId": "$_id.locationId",
                        "observedSku": "$_id.observedSku",
                        "latestQuantity": 1,
                        "latestObservedAt": 1,
                        "sourceMethod": 1,
                    }
                },
            ]
        )
        if positive_only:
            pipeline.append({"$match": {"latestQuantity": {"$gt": 0}}})
        pipeline.extend(
            [
                {"$sort": {"latestQuantity": -1, "latestObservedAt": -1, "observedSku": 1}},
                {"$limit": limit},
            ]
        )
        cursor = await collection.aggregate(pipeline, allowDiskUse=True, maxTimeMS=20_000)
        documents = _jsonable(await cursor.to_list(length=limit))
        gaps = [
            InventoryUncoveredSkuGap(
                location_id=str(document.get("locationId", "")),
                location_name=str(location_map.get(str(document.get("locationId", "")), {}).get("name", "")),
                amazon_sku_suffix=_stringify_optional(location_map.get(str(document.get("locationId", "")), {}).get("amazonSkuSuffix")),
                last_successful_ingestion_at=_stringify_optional(location_map.get(str(document.get("locationId", "")), {}).get("lastSuccessfulIngestionAt")),
                observed_sku=str(document.get("observedSku", "")),
                latest_quantity=_coerce_int(document.get("latestQuantity", 0)),
                latest_observed_at=str(document.get("latestObservedAt", "")),
                source_method=_stringify_optional(document.get("sourceMethod")),
            )
            for document in documents
        ]
        notes = ["Each row reflects the latest uncovered observation for one location and observed SKU."]
        if positive_only:
            notes.append("Only uncovered SKUs with latest observed quantity greater than zero were returned.")
        return InventoryUncoveredGapSummary(
            selected_location=selected_location,
            gaps=gaps,
            notes=notes,
        )

    @mcp.tool(
        title="Inventory Ingestion Freshness",
        description=(
            "Return location-level freshness and coverage stats for INV-Tracker, including lastSuccessfulIngestionAt, "
            "inventory row counts, positive inventory rows, total recorded quantity, and uncovered SKU counts."
        ),
        structured_output=False,
    )
    async def inventory_by_location_ingestion_freshness(
        location_query: Annotated[
            str | None,
            Field(description="Optional location name or suffix. If omitted, returns all locations."),
        ] = None,
    ) -> InventoryFreshnessSummary:
        location_documents, selected_location = await _select_inventory_locations(location_query)
        location_map = {
            str(document.get("_id", "")): document
            for document in location_documents
        }
        location_ids = sorted(location_map)

        _, inventory_collection = await _require_inventory_collection("inventorylevels")
        inventory_pipeline: list[dict[str, Any]] = []
        if location_ids:
            inventory_pipeline.append({"$match": {"locationId": {"$in": location_ids}}})
        inventory_pipeline.extend(
            [
                {
                    "$group": {
                        "_id": "$locationId",
                        "inventory_row_count": {"$sum": 1},
                        "positive_inventory_row_count": {
                            "$sum": {"$cond": [{"$gt": ["$quantity", 0]}, 1, 0]}
                        },
                        "total_recorded_quantity": {"$sum": "$quantity"},
                    }
                }
            ]
        )
        inventory_cursor = await inventory_collection.aggregate(
            inventory_pipeline,
            allowDiskUse=True,
            maxTimeMS=20_000,
        )
        inventory_documents = _jsonable(await inventory_cursor.to_list(length=200))
        inventory_by_location = {
            str(document.get("_id", "")): document
            for document in inventory_documents
        }

        _, uncovered_collection = await _require_inventory_collection("uncoveredskuobservations")
        uncovered_pipeline: list[dict[str, Any]] = []
        if location_ids:
            uncovered_pipeline.append({"$match": {"locationId": {"$in": location_ids}}})
        uncovered_pipeline.extend(
            [
                {"$sort": {"observedAt": -1}},
                {
                    "$group": {
                        "_id": {"locationId": "$locationId", "observedSku": "$observedSku"},
                        "latestQuantity": {"$first": "$quantity"},
                    }
                },
                {
                    "$group": {
                        "_id": "$_id.locationId",
                        "distinct_uncovered_skus": {"$sum": 1},
                        "positive_uncovered_skus": {
                            "$sum": {"$cond": [{"$gt": ["$latestQuantity", 0]}, 1, 0]}
                        },
                    }
                },
            ]
        )
        uncovered_cursor = await uncovered_collection.aggregate(
            uncovered_pipeline,
            allowDiskUse=True,
            maxTimeMS=20_000,
        )
        uncovered_documents = _jsonable(await uncovered_cursor.to_list(length=200))
        uncovered_by_location = {
            str(document.get("_id", "")): document
            for document in uncovered_documents
        }

        rows = [
            InventoryLocationFreshness(
                location_id=location_id,
                location_name=str(location.get("name", "")),
                amazon_sku_suffix=_stringify_optional(location.get("amazonSkuSuffix")),
                handling_time=location.get("handlingTime"),
                last_successful_ingestion_at=_stringify_optional(location.get("lastSuccessfulIngestionAt")),
                inventory_row_count=_coerce_int(inventory_by_location.get(location_id, {}).get("inventory_row_count", 0)),
                positive_inventory_row_count=_coerce_int(inventory_by_location.get(location_id, {}).get("positive_inventory_row_count", 0)),
                total_recorded_quantity=_coerce_int(inventory_by_location.get(location_id, {}).get("total_recorded_quantity", 0)),
                distinct_uncovered_skus=_coerce_int(uncovered_by_location.get(location_id, {}).get("distinct_uncovered_skus", 0)),
                positive_uncovered_skus=_coerce_int(uncovered_by_location.get(location_id, {}).get("positive_uncovered_skus", 0)),
            )
            for location_id, location in location_map.items()
        ]
        rows.sort(
            key=lambda item: (
                item.last_successful_ingestion_at or "",
                item.location_name,
            )
        )
        return InventoryFreshnessSummary(
            selected_location=selected_location,
            locations=rows,
            notes=[
                "Use last_successful_ingestion_at to judge location freshness, then compare uncovered SKU counts to separate data-model gaps from true zero inventory."
            ],
        )

    @mcp.tool(
        title="Aggregate Documents",
        description=(
            "Run a read-only MongoDB aggregation pipeline against one collection. "
            "Use this for grouped metrics, time-series analysis, change detection, and ranking questions. "
            "For buy-box status, collapse bb2_offers to one winner row per tracking_key plus received_at cell before "
            "rolling up by seller, zipcode, ASIN, or run. Write stages like $out and $merge are blocked. If the "
            "pipeline does not already end with $limit, the server appends one automatically."
        ),
        structured_output=False,
    )
    async def buybox_featured_offer_percent_aggregate_documents(
        collection_name: Annotated[
            str,
            Field(description="Exact collection name to query."),
        ],
        pipeline: Annotated[
            list[dict[str, Any]],
            Field(
                description=(
                    "MongoDB aggregation pipeline as a JSON array of stages. "
                    "Example: [{'$match': {'source_slug': 'all-treasure-garden-offers'}}, {'$group': {'_id': '$asin', 'count': {'$sum': 1}}}]"
                )
            ),
        ],
        limit: Annotated[
            int,
            Field(description="Maximum number of result rows to return if the pipeline does not already limit itself.", ge=1, le=MAX_AGGREGATE_LIMIT),
        ] = 100,
    ) -> AggregateResult:
        _, collection = await _require_collection(collection_name)
        pipeline_to_run, documents, normalized_limit = await _execute_aggregate_query(
            collection,
            pipeline=pipeline,
            limit=limit,
            tool_name="buybox_featured_offer_percent_aggregate_documents",
        )
        return AggregateResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            pipeline_executed=_jsonable(pipeline_to_run),
            documents=_jsonable(documents),
        )

    @mcp.tool(
        title="FedEx Rates and Delivery Promise",
        description=(
            "Get FedEx pricing AND committed delivery date/time for any origin->destination "
            "shipment, across every available service tier (Ground, Express Saver, 2Day, 2Day AM, "
            "Standard Overnight, Priority Overnight, First Overnight). Returns both account-rated "
            "(your contract discounts) and list rates, plus the committed delivery timestamp per "
            "service so you can pick the cheapest tier that still meets a deadline. "
            "Standing assumptions (matching our 3PL workflow): pickupType=USE_SCHEDULED_PICKUP "
            "(daily pickup already on the schedule), packagingType=YOUR_PACKAGING, no signature. "
            "Useful for landed-cost analysis per ASIN, choosing service tier by deadline, "
            "answering 'how much to ship X to ZIP Y by date Z'."
        ),
        structured_output=False,
    )
    async def fedex_rates_and_promise(
        origin_zip: Annotated[
            str,
            Field(description="Origin postal code (US 5-digit zip).", min_length=3, max_length=10),
        ],
        dest_zip: Annotated[
            str,
            Field(description="Destination postal code (US 5-digit zip).", min_length=3, max_length=10),
        ],
        weight_lb: Annotated[
            float,
            Field(description="Package weight in pounds.", gt=0, le=150),
        ],
        ship_date: Annotated[
            str | None,
            Field(
                description=(
                    "Date the package will be picked up, formatted YYYY-MM-DD. "
                    "Defaults to today if omitted."
                ),
                pattern=r"^\d{4}-\d{2}-\d{2}$",
            ),
        ] = None,
        length_in: Annotated[
            float | None,
            Field(description="Package length in inches. If any dimension is set, all three must be set.", gt=0),
        ] = None,
        width_in: Annotated[
            float | None,
            Field(description="Package width in inches.", gt=0),
        ] = None,
        height_in: Annotated[
            float | None,
            Field(description="Package height in inches.", gt=0),
        ] = None,
        service_type: Annotated[
            str | None,
            Field(
                description=(
                    "Optional FedEx service code to filter to a single tier, e.g. FEDEX_GROUND, "
                    "FEDEX_2_DAY, FEDEX_2_DAY_AM, FEDEX_EXPRESS_SAVER, STANDARD_OVERNIGHT, "
                    "PRIORITY_OVERNIGHT, FIRST_OVERNIGHT. Omit to get all available tiers."
                ),
            ),
        ] = None,
        saturday_delivery: Annotated[
            bool,
            Field(description="Request Saturday delivery as a special service. Default false."),
        ] = False,
        origin_country: Annotated[
            str,
            Field(description="ISO-2 country code for origin. Default US.", min_length=2, max_length=2),
        ] = "US",
        dest_country: Annotated[
            str,
            Field(description="ISO-2 country code for destination. Default US.", min_length=2, max_length=2),
        ] = "US",
    ) -> FedexRateQuoteSummary:
        runtime = get_runtime()
        client = runtime.fedex_client
        if client is None or not client.configured:
            raise FedexNotConfiguredError(
                "FedEx credentials are not configured on this server. Set FEDEX_API_KEY, "
                "FEDEX_API_SECRET, and FEDEX_ACCOUNT_NUMBER in the environment."
            )

        dims_set = (length_in, width_in, height_in)
        any_dim = any(d is not None for d in dims_set)
        all_dims = all(d is not None for d in dims_set)
        if any_dim and not all_dims:
            raise ValueError(
                "Provide all three of length_in, width_in, height_in, or none of them."
            )

        try:
            payload = await client.quote_rates(
                origin_zip=origin_zip,
                dest_zip=dest_zip,
                weight_lb=weight_lb,
                ship_date=ship_date,
                length_in=length_in,
                width_in=width_in,
                height_in=height_in,
                service_type=service_type,
                saturday_delivery=saturday_delivery,
                origin_country=origin_country,
                dest_country=dest_country,
            )
        except FedexApiError as exc:
            raise RuntimeError(
                f"FedEx rate request failed ({exc.code}): {exc.message}"
            ) from exc

        offers: list[FedexRateOffer] = []
        for entry in payload.get("output", {}).get("rateReplyDetails", []) or []:
            commit = entry.get("commit") or {}
            date_detail = commit.get("dateDetail") or {}
            dest_detail = commit.get("derivedDestinationDetail") or {}
            op_detail = entry.get("operationalDetail") or {}

            account_rate: float | None = None
            list_rate: float | None = None
            currency: str | None = None
            for shipment_detail in entry.get("ratedShipmentDetails", []) or []:
                rate_type = shipment_detail.get("rateType", "")
                charge = shipment_detail.get("totalNetCharge")
                if charge is None:
                    continue
                try:
                    charge_value = float(charge)
                except (TypeError, ValueError):
                    continue
                currency = shipment_detail.get("currency", currency)
                if "ACCOUNT" in rate_type:
                    account_rate = charge_value
                elif "LIST" in rate_type or "RATED" in rate_type:
                    list_rate = charge_value if list_rate is None else list_rate
                else:
                    if account_rate is None:
                        account_rate = charge_value

            offers.append(
                FedexRateOffer(
                    service_type=entry.get("serviceType", ""),
                    service_name=entry.get("serviceName", ""),
                    account_rate_usd=account_rate,
                    list_rate_usd=list_rate,
                    currency=currency,
                    committed_delivery_at=date_detail.get("dayFormat"),
                    committed_delivery_dow=date_detail.get("dayOfWeek"),
                    saturday_delivery=commit.get("saturdayDelivery"),
                    destination_airport_id=dest_detail.get("airportId"),
                    money_back_guarantee_eligible=(
                        not op_detail.get("ineligibleForMoneyBackGuarantee")
                        if "ineligibleForMoneyBackGuarantee" in op_detail
                        else None
                    ),
                )
            )

        offers.sort(
            key=lambda o: (
                o.account_rate_usd if o.account_rate_usd is not None else float("inf")
            )
        )

        dims_payload: dict[str, float] | None = None
        if all_dims:
            assert length_in is not None and width_in is not None and height_in is not None
            dims_payload = {"length": length_in, "width": width_in, "height": height_in}

        notes = [
            "Pricing comes back with both account-rated (your contract) and list rates when available.",
            "committed_delivery_at is the FedEx-committed local delivery timestamp; null means transit time was not returned for that tier.",
            "Pickup type is USE_SCHEDULED_PICKUP, which assumes the 3PL has a daily pickup already on the schedule. Switch the server config if that ever changes.",
            "money_back_guarantee_eligible is best-effort: null when FedEx did not return that detail for the service.",
        ]

        account_value = runtime.settings.fedex_account_number or ""
        masked_account = (
            f"****{account_value[-4:]}" if len(account_value) >= 4 else account_value
        )

        return FedexRateQuoteSummary(
            api_base=runtime.settings.fedex_api_base,
            account_number=masked_account,
            origin_postal_code=origin_zip,
            destination_postal_code=dest_zip,
            ship_date=ship_date,
            weight_lb=weight_lb,
            dimensions_in=dims_payload,
            pickup_type="USE_SCHEDULED_PICKUP",
            packaging_type="YOUR_PACKAGING",
            services_returned=len(offers),
            offers=offers,
            notes=notes,
        )

    return mcp
