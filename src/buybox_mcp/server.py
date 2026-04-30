from __future__ import annotations

from typing import Annotated, Any, Literal

from bson import json_util
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from buybox_mcp.config import Settings, get_settings
from buybox_mcp.runtime import ApplicationRuntime, get_runtime

MAX_FIND_LIMIT = 200
MAX_AGGREGATE_LIMIT = 200
MAX_DISTINCT_LIMIT = 200
MAX_SAMPLE_SIZE = 5
MAX_FIELD_DEPTH = 3
FORBIDDEN_AGGREGATE_STAGES = {"$out", "$merge"}


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


class SortField(BaseModel):
    field: str = Field(description="Field path to sort on, for example received_at or offer.seller.name.")
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


KNOWN_COLLECTION_HINTS: dict[str, CollectionHint] = {
    "bb2_offers": CollectionHint(
        category="buybox",
        description=(
            "Main buy-box fact table. Each document is one offer observation for an ASIN, "
            "source, zipcode, and scrape time. This is the primary collection for winner changes, "
            "seller shifts, price comparisons, Prime/FBA status, and delivery text."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "bb2_offer_runs": CollectionHint(
        category="buybox",
        description=(
            "Run-level metadata for buy-box scrapes. Use this to find the latest completed runs, "
            "source coverage, batch timing, and stored counts."
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
            "Registry of configured buy-box sources. Maps human-friendly source names to slugs and "
            "related collections."
        ),
        recommended_for_buybox_analysis=True,
    ),
    "bb2_offer_settings": CollectionHint(
        category="config",
        description="Application-level settings for the buy-box scraper, including internal seller naming.",
        recommended_for_buybox_analysis=False,
    ),
    "bb2_zipcode_locations": CollectionHint(
        category="reference",
        description=(
            "Reference table mapping tracked zipcodes to location metadata such as city, state, and coordinates."
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
        description="Run-level metadata for legacy search-term collection loads.",
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


def _jsonable(value: Any) -> Any:
    return json_util.loads(json_util.dumps(value))


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
                "Legacy or experimental product/offer snapshot collection. Inspect the schema before using it "
                "for production analysis."
            ),
            recommended_for_buybox_analysis=False,
        )
    return CollectionHint(
        category="unclassified",
        description="Unclassified collection. Use inspect_collection to understand its structure before querying it.",
        recommended_for_buybox_analysis=False,
    )


def _normalize_limit(limit: int, *, max_limit: int) -> int:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    return min(limit, max_limit)


def _normalize_sort(sort: list[SortField] | None) -> list[tuple[str, int]] | None:
    if not sort:
        return None
    return [(item.field, item.direction) for item in sort]


def _validate_pipeline(pipeline: list[dict[str, Any]]) -> None:
    if not pipeline:
        raise ValueError("pipeline must contain at least one stage")
    for stage in pipeline:
        if not isinstance(stage, dict) or len(stage) != 1:
            raise ValueError("each pipeline stage must be an object with exactly one operator")
        operator = next(iter(stage))
        if operator in FORBIDDEN_AGGREGATE_STAGES:
            raise ValueError(f"{operator} is not allowed in aggregate_documents")


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
        raise ValueError(f"Unknown collection: {name}. Use list_collections first.")
    return runtime, runtime.mongo_database[name]


async def _build_collection_profile(name: str, sample_size: int = 2) -> CollectionProfile:
    _, collection = await _require_collection(name)
    sample_size = _normalize_limit(sample_size, max_limit=MAX_SAMPLE_SIZE)

    estimated_count = await collection.estimated_document_count()
    first_doc = await collection.find_one()
    sample_sort = _preferred_sample_sort(name, first_doc)
    cursor = collection.find({}, limit=sample_size, sort=sample_sort)
    sample_documents = await cursor.to_list(length=sample_size)

    top_level_fields: set[str] = set()
    nested_fields: set[str] = set()
    for document in sample_documents or ([] if first_doc is None else [first_doc]):
        top_level_fields.update(document.keys())
        nested_fields.update(_flatten_keys(document))

    index_cursor = await collection.list_indexes()
    indexes = await index_cursor.to_list(length=None)
    hint = _collection_hint(name)

    return CollectionProfile(
        name=name,
        estimated_count=estimated_count,
        category=hint.category,
        description=hint.description,
        recommended_for_buybox_analysis=hint.recommended_for_buybox_analysis,
        sample_top_level_fields=sorted(top_level_fields),
        sample_nested_fields=sorted(nested_fields),
        indexes=[
            IndexSummary(
                name=str(index.get("name")),
                keys=_jsonable(index.get("key", {})),
            )
            for index in indexes
        ],
        sample_documents=_jsonable(sample_documents),
    )


def create_mcp_server(settings: Settings | None = None) -> FastMCP:
    resolved = settings or get_settings()
    mcp = FastMCP(
        name=resolved.app_name,
        instructions=(
            "Read-only MongoDB MCP server for Amazon buy-box and search-term analysis. "
            "Start with list_collections or the schema://catalog resource. "
            "For buy-box work, bb2_offers is the main fact collection, bb2_offer_runs contains scrape batches, "
            "bb2_offer_missing_cells contains coverage gaps, bb2_offer_sources maps source names to slugs, and "
            "bb2_zipcode_locations maps tracked zipcodes to cities and states. "
            "Use inspect_collection before querying unfamiliar collections. "
            "Use find_documents for direct document lookup, distinct_values for field discovery, and "
            "aggregate_documents for grouped or time-series analysis. All Mongo access is read-only."
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
        description="High-level catalog of collections, counts, categories, and buy-box analysis hints.",
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
                "bb2_offers is the main collection for buy-box change analysis.",
                "Use aggregate_documents for grouped questions like changes by SKU, seller, zipcode, or hour.",
                "Use inspect_collection before querying collections that are not already familiar.",
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
        description="Practical guidance for exploring the dataset with the open Mongo tools.",
    )
    def query_patterns() -> dict[str, Any]:
        return {
            "workflow": [
                "Call list_collections or read schema://catalog first.",
                "Inspect the target collection with inspect_collection if field names are unclear.",
                "Use distinct_values to learn common source_slug, seller_name, zipcode, or ASIN values.",
                "Use find_documents for raw rows and aggregate_documents for summaries, grouping, and time-series analysis.",
            ],
            "buybox_examples": [
                {
                    "question": "How did the buy box change over time for Treasure Garden?",
                    "hint": (
                        "Start in bb2_offers, filter source_slug to all-treasure-garden-offers, "
                        "sort by received_at, and group by asin plus receive-time buckets or change points."
                    ),
                },
                {
                    "question": "Which SKUs saw the biggest buy-box swings?",
                    "hint": (
                        "Aggregate bb2_offers by asin and time period, summarize winner changes or price deltas, "
                        "then sort descending on the change metric."
                    ),
                },
                {
                    "question": "Which zipcodes were stable versus volatile?",
                    "hint": (
                        "Group bb2_offers by zipcode and asin, then measure distinct winners, price spread, "
                        "or winner-change counts over time."
                    ),
                },
            ],
        }

    @mcp.prompt(
        name="analyze_buybox_question",
        title="Analyze Buy Box Question",
        description="Guidance prompt for answering open-ended buy-box questions with the schema and query tools.",
    )
    def analyze_buybox_question(
        question: Annotated[
            str,
            Field(description="The plain-English business question to answer from MongoDB."),
        ],
    ) -> str:
        return (
            "You are using a read-only MongoDB MCP server.\n"
            "1. Read schema://catalog or call list_collections.\n"
            "2. If the collection is unfamiliar, call inspect_collection.\n"
            "3. Prefer bb2_offers for buy-box winner, seller, price, Prime/FBA, delivery, ASIN, zipcode, and time analysis.\n"
            "4. Prefer bb2_offer_runs for scrape timing and run coverage.\n"
            "5. Use aggregate_documents for grouped questions and find_documents for raw rows.\n"
            "6. State the collection and key fields you used in the final answer.\n\n"
            f"Question to answer:\n{question}"
        )

    @mcp.tool(
        title="Server Status",
        description=(
            "Return the MCP server bootstrap state, auth mode, and whether MongoDB is configured. "
            "Use this when confirming connectivity before running data queries."
        ),
        structured_output=False,
    )
    async def server_status() -> ServerStatus:
        runtime = get_runtime()
        return ServerStatus(
            name=runtime.settings.app_name,
            environment=runtime.settings.env,
            auth="bearer",
            mongo_configured=runtime.mongo_client is not None,
            mongo_database=runtime.settings.mongo_database,
            startup_errors=runtime.startup_errors,
        )

    @mcp.tool(
        title="List Collections",
        description=(
            "List available MongoDB collections with approximate counts and usage hints. "
            "This is the best first tool for orienting an agent inside the dataset."
        ),
        structured_output=False,
    )
    async def list_collections() -> list[CollectionSummary]:
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
            "and a few sample documents. Use this before writing ad hoc Mongo queries against unfamiliar data."
        ),
        structured_output=False,
    )
    async def inspect_collection(
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
            "Use this for raw document inspection, recent rows, or direct lookups by ASIN, seller, source_slug, or zipcode."
        ),
        structured_output=False,
    )
    async def find_documents(
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
            Field(description="Optional sort order. Example: [{'field': 'received_at', 'direction': -1}]."),
        ] = None,
        limit: Annotated[
            int,
            Field(description="Maximum number of documents to return. Hard-capped at 200.", ge=1, le=MAX_FIND_LIMIT),
        ] = 25,
    ) -> QueryResult:
        _, collection = await _require_collection(collection_name)
        normalized_limit = _normalize_limit(limit, max_limit=MAX_FIND_LIMIT)
        cursor = collection.find(
            filter or {},
            projection=projection,
            sort=_normalize_sort(sort),
            limit=normalized_limit,
            max_time_ms=15_000,
        )
        documents = await cursor.to_list(length=normalized_limit)
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
            "before composing a larger query."
        ),
        structured_output=False,
    )
    async def distinct_values(
        collection_name: Annotated[
            str,
            Field(description="Exact collection name to query."),
        ],
        field_name: Annotated[
            str,
            Field(description="Field path to inspect, for example source_slug, seller_name, zipcode, or meta.run_id."),
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
        values = await collection.distinct(field_name, filter=filter or {}, maxTimeMS=15_000)
        normalized_values = _jsonable(values)[:normalized_limit]
        return DistinctResult(
            collection=collection_name,
            field=field_name,
            returned_count=len(normalized_values),
            limit_applied=normalized_limit,
            values=normalized_values,
        )

    @mcp.tool(
        title="Aggregate Documents",
        description=(
            "Run a read-only MongoDB aggregation pipeline against one collection. "
            "Use this for grouped metrics, time-series analysis, change detection, and ranking questions. "
            "Write stages like $out and $merge are blocked. If the pipeline does not already end with $limit, "
            "the server appends one automatically."
        ),
        structured_output=False,
    )
    async def aggregate_documents(
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
        normalized_limit = _normalize_limit(limit, max_limit=MAX_AGGREGATE_LIMIT)
        _validate_pipeline(pipeline)
        pipeline_to_run = _ensure_limit_stage(pipeline, normalized_limit)
        cursor = await collection.aggregate(
            pipeline_to_run,
            allowDiskUse=True,
            maxTimeMS=20_000,
        )
        documents = await cursor.to_list(length=normalized_limit)
        return AggregateResult(
            collection=collection_name,
            returned_count=len(documents),
            limit_applied=normalized_limit,
            pipeline_executed=_jsonable(pipeline_to_run),
            documents=_jsonable(documents),
        )

    return mcp
