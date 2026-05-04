import {
  OAuthProvider,
  type AuthRequest,
  type OAuthHelpers,
} from "@cloudflare/workers-oauth-provider";

interface Env {
  OAUTH_KV: KVNamespace;
  OAUTH_PROVIDER: OAuthHelpers;
  GOOGLE_CLIENT_ID: string;
  GOOGLE_CLIENT_SECRET: string;
  ALLOWED_HD: string;
  MCP_ORIGIN: string;
  MCP_ORIGIN_BEARER: string;
}

interface UserProps {
  email: string;
  name?: string;
  hd?: string;
  sub: string;
}

const GOOGLE_AUTH = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN = "https://oauth2.googleapis.com/token";
const GOOGLE_USERINFO = "https://openidconnect.googleapis.com/v1/userinfo";

function callbackUrl(request: Request): string {
  const u = new URL(request.url);
  return `${u.origin}/google-callback`;
}

function allowedDomains(env: Env): string[] {
  return (env.ALLOWED_HD ?? "")
    .split(",")
    .map((d) => d.trim())
    .filter(Boolean);
}

function encodeState(req: AuthRequest): string {
  return btoa(JSON.stringify(req));
}

function decodeState(state: string): AuthRequest {
  return JSON.parse(atob(state)) as AuthRequest;
}

const defaultHandler = {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/" && request.method === "GET") {
      return Response.json({
        name: "Buy Box MCP OAuth Proxy",
        upstream: env.MCP_ORIGIN,
        mcp_url: `${url.origin}/mcp`,
        authorize: `${url.origin}/authorize`,
      });
    }

    if (url.pathname === "/healthz") {
      return new Response("ok", { status: 200 });
    }

    if (url.pathname === "/authorize" && request.method === "GET") {
      const oauthReq = await env.OAUTH_PROVIDER.parseAuthRequest(request);

      const params = new URLSearchParams({
        client_id: env.GOOGLE_CLIENT_ID,
        redirect_uri: callbackUrl(request),
        response_type: "code",
        scope: "openid email profile",
        access_type: "online",
        prompt: "select_account",
        state: encodeState(oauthReq),
      });
      const domains = allowedDomains(env);
      if (domains.length === 1) {
        params.set("hd", domains[0]!);
      }
      return Response.redirect(`${GOOGLE_AUTH}?${params.toString()}`, 302);
    }

    if (url.pathname === "/google-callback" && request.method === "GET") {
      const code = url.searchParams.get("code");
      const stateParam = url.searchParams.get("state");
      const errorParam = url.searchParams.get("error");
      if (errorParam) {
        return new Response(`google auth error: ${errorParam}`, { status: 400 });
      }
      if (!code || !stateParam) {
        return new Response("missing code or state", { status: 400 });
      }

      let oauthReq: AuthRequest;
      try {
        oauthReq = decodeState(stateParam);
      } catch {
        return new Response("invalid state", { status: 400 });
      }

      const tokenRes = await fetch(GOOGLE_TOKEN, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          code,
          client_id: env.GOOGLE_CLIENT_ID,
          client_secret: env.GOOGLE_CLIENT_SECRET,
          redirect_uri: callbackUrl(request),
          grant_type: "authorization_code",
        }),
      });
      if (!tokenRes.ok) {
        return new Response(`google token exchange failed: ${tokenRes.status}`, {
          status: 502,
        });
      }
      const googleTokens = (await tokenRes.json()) as { access_token: string };

      const userRes = await fetch(GOOGLE_USERINFO, {
        headers: { Authorization: `Bearer ${googleTokens.access_token}` },
      });
      if (!userRes.ok) {
        return new Response(`google userinfo failed: ${userRes.status}`, {
          status: 502,
        });
      }
      const user = (await userRes.json()) as {
        sub: string;
        email: string;
        email_verified?: boolean;
        name?: string;
        hd?: string;
      };

      if (!user.email_verified) {
        return new Response("email not verified", { status: 403 });
      }
      const domains = allowedDomains(env);
      if (domains.length > 0 && !domains.includes(user.hd ?? "")) {
        return new Response(
          `account ${user.email} is not in an allowed workspace (${domains.join(", ")})`,
          { status: 403 },
        );
      }

      const props: UserProps = {
        sub: user.sub,
        email: user.email,
        name: user.name,
        hd: user.hd,
      };

      const { redirectTo } = await env.OAUTH_PROVIDER.completeAuthorization({
        request: oauthReq,
        userId: user.sub,
        scope: oauthReq.scope?.length ? oauthReq.scope : ["mcp"],
        metadata: { email: user.email, hd: user.hd ?? null },
        props,
      });

      return Response.redirect(redirectTo, 302);
    }

    return new Response("not found", { status: 404 });
  },
};

const apiHandler = {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const props = (ctx as ExecutionContext & { props?: UserProps }).props;
    const incoming = new URL(request.url);
    const target = new URL(incoming.pathname + incoming.search, env.MCP_ORIGIN);

    const headers = new Headers(request.headers);
    headers.set("authorization", `Bearer ${env.MCP_ORIGIN_BEARER}`);
    headers.delete("host");
    if (props?.email) headers.set("x-forwarded-user", props.email);
    if (props?.hd) headers.set("x-forwarded-user-hd", props.hd);

    const init: RequestInit & { duplex?: "half" } = {
      method: request.method,
      headers,
      redirect: "manual",
    };
    if (request.method !== "GET" && request.method !== "HEAD") {
      init.body = request.body;
      init.duplex = "half";
    }

    return fetch(target.toString(), init);
  },
};

export default new OAuthProvider<Env>({
  apiRoute: ["/mcp"],
  apiHandler,
  defaultHandler,
  authorizeEndpoint: "/authorize",
  tokenEndpoint: "/token",
  clientRegistrationEndpoint: "/register",
  scopesSupported: ["mcp"],
});
