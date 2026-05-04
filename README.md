# Buy Box Analytics MCP

Python MCP server scaffold for exposing your Amazon buy-box, search-term, and inventory MongoDB data over HTTP with a static bearer token in front of the MCP endpoint.

This version is intentionally open but read-only:

- Streamable HTTP MCP endpoint at `/mcp`
- Bearer-token auth on MCP traffic
- Public health check at `/healthz`
- Mongo-backed schema inspection resources and tools
- Open read-only Mongo `find`, `distinct`, and `aggregate` tools
- Render Blueprint deployment via `render.yaml`

The main server surface lives in `src/buybox_mcp/server.py`.

## Local setup

1. Create a virtual environment and install dependencies:

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

2. Create your local env file:

```bash
cp .env.example .env.local
```

3. Set at minimum:

- `BUYBOX_MCP_BEARER_TOKEN`

For Mongo, the app accepts either:

- `BUYBOX_MCP_MONGO_URI`
- `MONGODB_URI`

The primary analytics database defaults to `bb2`. Override it only if you want a different database.

The inventory tools use a second database that defaults to `INV-Tracker`. Override it with:

- `BUYBOX_MCP_INVENTORY_MONGO_DATABASE`

4. Run the server:

```bash
python -m buybox_mcp
```

Default local URL: `http://localhost:8000`

## Endpoints

- `GET /`
  Returns basic connection info, including the MCP URL.
- `GET /healthz`
  Public health check for Render.
- `POST /mcp`
- `GET /mcp`
- `DELETE /mcp`
  MCP Streamable HTTP transport, protected by `Authorization: Bearer <token>`.

## Recommended MCP names

Use names that encode both environment and transport:

- `buybox_prod_http`: production remote MCP over HTTP
- `buybox_dev_http`: local development MCP over HTTP
- `buybox_dev_stdio`: local development MCP over stdio

Avoid generic names like `buybox_render` or `buybox_local_stdio`. They hide the thing future agents care about most: whether the server is production or development, and whether it is HTTP or stdio.

## Local Codex

HTTP MCP registration:

```bash
export BUYBOX_MCP_DEV_TOKEN=replace-with-your-local-token
codex mcp add buybox_dev_http --url http://127.0.0.1:3001/mcp/ --bearer-token-env-var BUYBOX_MCP_DEV_TOKEN
```

Then run the server locally:

```bash
BUYBOX_MCP_BEARER_TOKEN="$BUYBOX_MCP_DEV_TOKEN" BUYBOX_MCP_HOST=127.0.0.1 BUYBOX_MCP_PORT=3001 python -m buybox_mcp
```

Stdio MCP registration:

```bash
codex mcp add buybox_dev_stdio --env BUYBOX_MCP_BEARER_TOKEN=stdio-local-token -- .venv/bin/python -m buybox_mcp.stdio
```

The stdio entrypoint is `src/buybox_mcp/stdio.py`. It uses the same Mongo-backed server logic as the HTTP app and is useful for local MCP clients that prefer subprocess transport.

Production HTTP registration should also follow the same naming pattern:

```bash
export BUYBOX_MCP_PROD_TOKEN=replace-with-your-prod-token
codex mcp add buybox_prod_http --url https://<your-service>.onrender.com/mcp/ --bearer-token-env-var BUYBOX_MCP_PROD_TOKEN
```

## Local test

```bash
pytest
```

## MCP surface

Tools:

- `buybox_featured_offer_percent_server_status`
- `buybox_featured_offer_percent_list_collections`
- `buybox_featured_offer_percent_inspect_collection`
- `buybox_featured_offer_percent_find_documents`
- `buybox_featured_offer_percent_distinct_values`
- `buybox_featured_offer_percent_resolve_source`
- `buybox_featured_offer_percent_summarize_status`
- `buybox_featured_offer_percent_aggregate_documents`
- `keyword_rank_tracking_list_collections`
- `keyword_rank_tracking_inspect_collection`
- `keyword_rank_tracking_find_documents`
- `keyword_rank_tracking_distinct_values`
- `keyword_rank_tracking_aggregate_documents`
- `keyword_rank_tracking_list_sources`
- `keyword_rank_tracking_resolve_source`
- `keyword_rank_tracking_resolve_keyword`
- `keyword_rank_tracking_resolve_asin`
- `keyword_rank_tracking_latest_search_results`
- `keyword_rank_tracking_rank_history`
- `keyword_rank_tracking_search_query_volume`
- `inventory_by_location_list_collections`
- `inventory_by_location_inspect_collection`
- `inventory_by_location_find_documents`
- `inventory_by_location_distinct_values`
- `inventory_by_location_aggregate_documents`
- `inventory_by_location_resolve_location`
- `inventory_by_location_resolve_sku`
- `inventory_by_location_explain_sku`
- `inventory_by_location_quantity`
- `inventory_by_location_buildable_quantity`
- `inventory_by_location_component_constraints`
- `inventory_by_location_parent_skus_for_component`
- `inventory_by_location_availability_for_asin`
- `inventory_by_location_uncovered_sku_gaps`
- `inventory_by_location_ingestion_freshness`

Resources:

- `schema://catalog`
- `schema://collection/{collection_name}`
- `guide://query-patterns`

Prompt:

- `analyze_buybox_question`
- `analyze_keyword_rank_tracking_question`

The important design point is that agents can orient themselves without hardcoded business logic:

- `schema://catalog` explains which collections matter
- `buybox_featured_offer_percent_inspect_collection` exposes nested field paths and indexes
- `buybox_featured_offer_percent_distinct_values` helps discover valid slugs, sellers, zipcodes, and statuses
- `buybox_featured_offer_percent_resolve_source` maps a human name like "Treasure Garden" to the best production source slug
- `buybox_featured_offer_percent_summarize_status` answers the common executive status question directly and can use either the latest available runs or explicit date/timestamp selectors
- `buybox_featured_offer_percent_aggregate_documents` handles grouped and time-series questions
- `keyword_rank_tracking_list_collections` and `inventory_by_location_list_collections` expose scoped ad hoc query surfaces so agents stay inside the right part of the data model
- `keyword_rank_tracking_list_sources` exposes the valid keyword/search-term sources and backing collections
- `keyword_rank_tracking_resolve_keyword` pins a plain-English term to the canonical tracked keyword row and source collection
- `keyword_rank_tracking_latest_search_results` and `keyword_rank_tracking_rank_history` reconstruct search rank from snapshot documents without requiring ad hoc aggregation
- `keyword_rank_tracking_search_query_volume` retrieves uploaded demand rows from `kw_search_query_volumes`

The MCP guidance now also encodes the core buy-box semantics agents kept having to rediscover:

- in `bb2_offers`, one buy-box cell is one `tracking_key` at one `received_at`
- within a cell, the winner is the row with the lowest `offer_index`
- `bb2_offer_settings.our_seller_name` is the canonical identity for questions about "us"
- `bb2_offer_runs` is the starting point for latest-run and coverage analysis
- for run alignment, prefer `bb2_offer_runs.received_at` and `bb2_offer_runs.result_set.id` over `bb2_offers.meta.run_id`
- do not assume every collection has rows for every date; some data is daily, some weekly, and some only exists when manually requested
- `legacy_buybox` collections are not the current production fact table

The keyword tools encode the main rank-tracking rules as well:

- each search-term document is one snapshot for one `search_term` at one `received_at`
- there is no dedicated rank-history collection; rank comes from `search_results[].position` over time
- source-specific collections like `source-fireplaces-search_terms` are the primary snapshot tables
- `kw_sources`, `kw_tracked_keywords`, `kw_tracked_asins`, and `kw_search_query_volumes` provide source registry, tracked term, tracked ASIN, and demand metadata

The inventory tools encode the main INV-Tracker rules as well:

- `inventorylevels` stores direct recorded quantity by `locationId` and `sku`
- `skus.kitComponents` is the bill of materials for `kit` and `option` SKUs
- for `kit` and `option` SKUs, `recorded_quantity` and `buildable_quantity` are kept separate in tool outputs
- `buildable_quantity` is derived from component stock and should not be blindly merged with direct recorded kit quantity
- `amazonskualiases` and `childAsins` together map Amazon listings back to internal stock by location
- `uncoveredskuobservations` is a data-gap signal, not inventory truth

For buy-box analysis, the most important collections are:

- `bb2_offers`: one offer observation per scrape row; collapse to the lowest `offer_index` per `tracking_key` + `received_at` to get the winner
- `bb2_offer_runs`: scrape batch metadata and coverage counters for run-over-run comparisons
- `bb2_offer_missing_cells`: parsing/coverage gaps
- `bb2_offer_sources`: source registry and the safest place to confirm valid `source_slug` values
- `bb2_offer_settings`: canonical internal seller identity in `our_seller_name`
- `bb2_zipcode_locations`: zipcode reference data for city/state rollups

When you need shared state or startup-time resources, use `src/buybox_mcp/runtime.py`.

## Render deploy

Render Blueprints look for `render.yaml` at repo root by default. This project includes a Blueprint that creates one Python web service and sets:

- `buildCommand: pip install .`
- `startCommand: python -m buybox_mcp`
- `healthCheckPath: /healthz`

Secret env vars are declared with `sync: false`, which means Render prompts you for them during the initial Blueprint creation flow:

- `BUYBOX_MCP_BEARER_TOKEN`
- `BUYBOX_MCP_MONGO_URI`

Deploy flow:

1. Push this repo to GitHub/GitLab/Bitbucket.
2. In Render, choose `New > Blueprint`.
3. Select the repo and deploy the `render.yaml` Blueprint.
4. Provide the secret values when prompted.
5. After deploy, connect clients to `https://<your-service>.onrender.com/mcp` with `Authorization: Bearer <token>`.

## Query model

This server intentionally allows open read-only querying instead of forcing every analysis path into a custom tool. That is useful for questions like:

- "For Treasure Garden, how did the buy box change over time?"
- "Which SKUs had the biggest buy-box swings?"
- "Which zipcodes were stable versus volatile?"

The guardrails are:

- No write tools
- No Mongo write stages in aggregation pipelines
- Result-size caps on `find`, `distinct`, and `aggregate`
- Tool and resource descriptions that help the agent locate the right data and apply the right buy-box semantics

Recommended agent workflow for buy-box status questions:

1. Read `schema://catalog` or call `buybox_featured_offer_percent_list_collections`.
2. Read `bb2_offer_settings` if the question refers to "our" seller.
3. Confirm the `source_slug` with `bb2_offer_sources` or `buybox_featured_offer_percent_distinct_values`.
4. Prefer `buybox_featured_offer_percent_resolve_source` if the question gives you a brand or collection name instead of an exact slug.
5. Prefer `buybox_featured_offer_percent_summarize_status` for the standard “are we gaining or losing?” question.
It defaults to the newest completed runs, but it can also compare the latest completed runs at or before specific dates you ask for.
6. Use `bb2_offer_runs` to find the latest completed run or runs to compare.
7. In `bb2_offers`, collapse each `tracking_key` + `received_at` cell to the row with the lowest `offer_index`.
8. Compare overlap cells first for gain/loss analysis, then explain any coverage differences separately.
9. Do not assume a missing date means zero activity. First confirm whether that collection or source actually ran on that date.

Recommended agent workflow for INV-Tracker questions:

1. Use `inventory_by_location_list_collections`, `inventory_by_location_inspect_collection`, `inventory_by_location_find_documents`, `inventory_by_location_distinct_values`, and `inventory_by_location_aggregate_documents` for scoped ad hoc inventory queries.
2. Use `inventory_by_location_resolve_sku` if the user gives you an alias, ASIN, or partial SKU.
3. Use `inventory_by_location_resolve_location` if the location name or suffix is informal.
4. Use `inventory_by_location_quantity` for direct stock by location.
5. Use `inventory_by_location_buildable_quantity` or `inventory_by_location_component_constraints` for `kit` and `option` SKUs.
6. Use `inventory_by_location_parent_skus_for_component` to understand shortage impact upstream.
7. Use `inventory_by_location_availability_for_asin` when the question starts from an Amazon ASIN.
8. Use `inventory_by_location_uncovered_sku_gaps` and `inventory_by_location_ingestion_freshness` before assuming a zero is real inventory rather than a modeling or ingestion gap.

Recommended agent workflow for keyword rank tracking questions:

1. Call `keyword_rank_tracking_list_collections`, `keyword_rank_tracking_inspect_collection`, `keyword_rank_tracking_find_documents`, `keyword_rank_tracking_distinct_values`, and `keyword_rank_tracking_aggregate_documents` for scoped ad hoc keyword/search-term queries.
2. Call `keyword_rank_tracking_list_sources` to see the valid keyword sources and collections.
3. Use `keyword_rank_tracking_resolve_source` if the source name is informal.
4. Use `keyword_rank_tracking_resolve_keyword` to pin the canonical tracked term and its source collection.
5. Use `keyword_rank_tracking_latest_search_results` for the newest snapshot of a term.
6. Use `keyword_rank_tracking_rank_history` to trace one ASIN's position across repeated snapshots.
7. Use `keyword_rank_tracking_search_query_volume` for demand metrics from `kw_search_query_volumes`.
8. Do not assume missing ASINs mean zero demand; they only mean the ASIN was absent from the captured result set for that snapshot.

## Why this auth shape

This uses a static bearer token because it is the shortest path to a remote MCP URL you can protect today.

It is not the full MCP OAuth 2.1 resource-server flow. If you later want per-user auth, consent, or first-class enterprise auth, you should swap this for the MCP SDK's OAuth-based auth support instead of extending the static token approach.

For Claude work-org connectors (which require OAuth, not bearer), `cloudflare/oauth-proxy/` deploys a Cloudflare Worker that fronts this service with OAuth 2.1 against Google and forwards the static bearer to Render. See `cloudflare/oauth-proxy/README.md`.
