# Buy Box MCP OAuth Proxy

A Cloudflare Worker that fronts the Render-hosted Buy Box MCP server with the OAuth 2.1 flow that Claude Desktop's work-org "Add custom connector" requires.

## What it does

- Acts as the OAuth 2.1 authorization server for `/mcp` (issues its own access tokens, exposes RFC 8414 + RFC 9728 metadata, supports Dynamic Client Registration).
- Authenticates users against Google (workspace-domain restricted).
- Once authenticated, proxies `/mcp` traffic to the Render origin and injects the static `BUYBOX_MCP_BEARER_TOKEN` so the origin doesn't have to change.
- Anonymous traffic to the origin's `/`, `/healthz`, etc. is unchanged — the proxy only fronts `/mcp`.

```
Claude Desktop ──OAuth──▶ Worker ──static bearer──▶ Render MCP origin
                            │
                            └── upstream OAuth via Google
```

## One-time setup

You'll need: a Cloudflare account, a Google Cloud project, and `wrangler` logged in.

### 1. Cloudflare KV namespace

```bash
cd cloudflare/oauth-proxy
npm install
npx wrangler login
npx wrangler kv namespace create OAUTH_KV
```

Copy the resulting `id` into `wrangler.jsonc` (replace `REPLACE_WITH_KV_ID`).

### 2. Google OAuth client

In Google Cloud Console (the Workspace org for `gentling.ai`):

1. APIs & Services → Credentials → Create credentials → OAuth client ID.
2. Application type: Web application.
3. Authorized redirect URI: `https://<your-worker>.<account>.workers.dev/google-callback`.
4. Save. Copy the Client ID and Client Secret.

### 3. Set worker secrets

```bash
npx wrangler secret put GOOGLE_CLIENT_ID
npx wrangler secret put GOOGLE_CLIENT_SECRET
npx wrangler secret put MCP_ORIGIN_BEARER   # the Render BUYBOX_MCP_BEARER_TOKEN value
```

`MCP_ORIGIN` and `ALLOWED_HD` are set in `wrangler.jsonc` `vars` (non-secret); change them there.

### 4. Deploy

```bash
npx wrangler deploy
```

Note the deployed URL (something like `https://buybox-mcp-oauth-proxy.<account>.workers.dev`). That's the URL you'll give Claude.

### 5. Smoke test

```bash
curl -i https://<worker-url>/healthz
curl -s https://<worker-url>/.well-known/oauth-authorization-server | jq .
curl -s https://<worker-url>/.well-known/oauth-protected-resource | jq .
```

The two well-known docs prove the OAuth surface is live. The protected-resource doc is what Claude reads first.

## Wiring up Claude

In Claude (work org) → Settings → Connectors → Add custom connector:

- Name: `Buy Box MCP`
- Remote MCP server URL: `https://<worker-url>/mcp`
- Leave OAuth Client ID and Secret empty — Claude will use Dynamic Client Registration against the Worker.

When you click Add and then connect, Claude will redirect you to the Worker, which will redirect to Google, and you'll consent with your `@gentling.ai` account.

## Local dev

```bash
npx wrangler dev
```

Then point an MCP client at `http://127.0.0.1:8787/mcp`. You'll need a real Google OAuth client whose redirect URI matches the local worker URL — the simplest option is a separate "dev" Google OAuth client with `http://127.0.0.1:8787/google-callback` as the redirect URI, set via `.dev.vars`:

```
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
MCP_ORIGIN_BEARER=...
```

`.dev.vars` is gitignored.

## Files

- `src/index.ts` — Worker entrypoint. Defines two handlers: a `defaultHandler` that runs the OAuth dance with Google and a small `apiHandler` that proxies `/mcp` to the Render origin.
- `wrangler.jsonc` — Worker config. Non-secret vars and KV binding live here.
- `package.json` / `tsconfig.json` — TypeScript + Wrangler setup.

## Why this shape

The MCP server stays bearer-token-protected internally — nothing on the Render side changes. The Worker is the only thing that speaks OAuth to Claude, so when we eventually swap Google for Okta/Auth0, only the Worker needs to change.

Per-user authorization context flows through to the origin via `X-Forwarded-User` and `X-Forwarded-User-Hd` headers, so the Render server can log who's calling without doing any auth work itself.
