# Buy Box MCP

Python MCP server scaffold for exposing your Amazon buy-box MongoDB over HTTP with a static bearer token in front of the MCP endpoint.

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

The database name defaults to `bb2`. Override it only if you want a different database.

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

## Local Codex

HTTP MCP registration:

```bash
export BUYBOX_MCP_LOCAL_TOKEN=replace-with-your-local-token
codex mcp add buybox_local --url http://127.0.0.1:3001/mcp/ --bearer-token-env-var BUYBOX_MCP_LOCAL_TOKEN
```

Then run the server locally:

```bash
BUYBOX_MCP_BEARER_TOKEN="$BUYBOX_MCP_LOCAL_TOKEN" BUYBOX_MCP_HOST=127.0.0.1 BUYBOX_MCP_PORT=3001 python -m buybox_mcp
```

Stdio MCP registration:

```bash
codex mcp add buybox_local_stdio --env BUYBOX_MCP_BEARER_TOKEN=stdio-local-token -- .venv/bin/python -m buybox_mcp.stdio
```

The stdio entrypoint is `src/buybox_mcp/stdio.py`. It uses the same Mongo-backed server logic as the HTTP app and is useful for local MCP clients that prefer subprocess transport.

## Local test

```bash
pytest
```

## MCP surface

Tools:

- `server_status`
- `list_collections`
- `inspect_collection`
- `find_documents`
- `distinct_values`
- `aggregate_documents`

Resources:

- `schema://catalog`
- `schema://collection/{collection_name}`
- `guide://query-patterns`

Prompt:

- `analyze_buybox_question`

The important design point is that agents can orient themselves without hardcoded business logic:

- `schema://catalog` explains which collections matter
- `inspect_collection` exposes nested field paths and indexes
- `distinct_values` helps discover valid slugs, sellers, zipcodes, and statuses
- `aggregate_documents` handles grouped and time-series questions

For buy-box analysis, the most important collections are:

- `bb2_offers`: one offer observation per scrape row
- `bb2_offer_runs`: scrape batch metadata
- `bb2_offer_missing_cells`: parsing/coverage gaps
- `bb2_offer_sources`: source registry
- `bb2_zipcode_locations`: zipcode reference data

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
- Tool and resource descriptions that help the agent locate the right data

## Why this auth shape

This uses a static bearer token because it is the shortest path to a remote MCP URL you can protect today.

It is not the full MCP OAuth 2.1 resource-server flow. If you later want per-user auth, consent, or first-class enterprise auth, you should swap this for the MCP SDK's OAuth-based auth support instead of extending the static token approach.
