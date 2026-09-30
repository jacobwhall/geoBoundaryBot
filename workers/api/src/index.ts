/**
 * geoBoundaries API Worker
 *
 * Serves /api/{version}/{product}/{ISO|ALL}/{ADM|ALL}/ on
 * www.geoboundaries-dev.org. Concrete boundary requests read one metadata JSON
 * from R2. Requests containing ALL read a prebuilt product index and return
 * the matching records. Download URLs are rewritten to point at the public
 * R2 custom domain (data.geoboundaries-dev.org).
 *
 * `current` is resolved at request time by reading /current.json from
 * the bucket root, which the build pipeline writes during a promoted
 * release run. Versioned requests (e.g. /api/v6/...) skip that lookup.
 */

export interface Env {
  BUCKET: R2Bucket;
  DATA_DOMAIN: string;
}

const VALID_PRODUCTS = new Set(["gbOpen", "gbHumanitarian", "gbAuthoritative"]);
const ISO_RE = /^[A-Z]{3}$/;
const ADM_RE = /^ADM\d$/;
const ALL = "ALL";

// Fields in the metadata JSON whose values are download URLs that need
// to be rewritten to point at the R2 custom domain.
const URL_FIELDS = [
  "staticDownloadLink",
  "gjDownloadURL",
  "tjDownloadURL",
  "imagePreview",
  "simplifiedGeometryGeoJSON",
] as const;

// Synthetic key for caching the resolved `current` version at the edge.
const CURRENT_CACHE_KEY = "https://internal.geoboundaries/_/current";
const CURRENT_TTL_SECONDS = 60;
const META_TTL_SECONDS = 300;

interface ParsedPath {
  version: string;
  product: string;
  iso: string;
  adm: string;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return jsonError(405, "Method Not Allowed");
    }

    const url = new URL(request.url);

    // Edge cache lookup keyed on the full URL.
    const cacheKey = new Request(url.toString(), { method: "GET" });
    const cache = caches.default;
    const cached = await cache.match(cacheKey);
    if (cached) return cached;

    const parsed = parseApiPath(url.pathname);
    if (!parsed) return jsonError(404, "Not Found");

    let resolvedVersion = parsed.version;
    if (resolvedVersion === "current") {
      const v = await resolveCurrent(env, ctx);
      if (!v) return jsonError(503, "No current release promoted yet");
      resolvedVersion = v;
    }

    let payload: Record<string, unknown> | Record<string, unknown>[];
    if (parsed.iso === ALL || parsed.adm === ALL) {
      const key = indexKey(resolvedVersion, parsed.product);
      const obj = await env.BUCKET.get(key);
      if (!obj) return jsonError(404, "Aggregate index not found", { key });

      let value: unknown;
      try {
        value = await obj.json();
      } catch {
        return jsonError(500, "Aggregate index JSON is malformed", { key });
      }
      if (!isMetadataIndex(value)) {
        return jsonError(500, "Aggregate index records are malformed", { key });
      }

      payload = value.filter((meta) => {
        const matchesIso = parsed.iso === ALL || meta.boundaryISO === parsed.iso;
        const matchesAdm = parsed.adm === ALL || meta.boundaryType === parsed.adm;
        return matchesIso && matchesAdm;
      });
      for (const meta of payload) {
        rewriteUrls(
          meta,
          env.DATA_DOMAIN,
          resolvedVersion,
          parsed.product,
          meta.boundaryISO as string,
          meta.boundaryType as string,
        );
      }
    } else {
      const key = metadataKey(resolvedVersion, parsed.product, parsed.iso, parsed.adm);
      const obj = await env.BUCKET.get(key);
      if (!obj) return jsonError(404, "Boundary not found", { key });

      try {
        const value: unknown = await obj.json();
        if (!isRecord(value)) throw new Error("metadata root is not an object");
        payload = value;
      } catch {
        return jsonError(500, "Metadata JSON is malformed", { key });
      }

      rewriteUrls(
        payload,
        env.DATA_DOMAIN,
        resolvedVersion,
        parsed.product,
        parsed.iso,
        parsed.adm,
      );
    }

    const response = new Response(JSON.stringify(payload, null, 2), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": `public, max-age=${META_TTL_SECONDS}, s-maxage=${META_TTL_SECONDS}`,
        "Access-Control-Allow-Origin": "*",
        "X-GB-Version": resolvedVersion,
        "X-GB-Requested-Version": parsed.version,
      },
    });

    ctx.waitUntil(cache.put(cacheKey, response.clone()));
    return response;
  },
};

export function parseApiPath(pathname: string): ParsedPath | null {
  // Accept with or without trailing slash.
  const parts = pathname.split("/").filter((p) => p.length > 0);
  if (parts.length !== 5) return null;
  if (parts[0] !== "api") return null;
  const [, version, product, iso, adm] = parts;
  if (!VALID_PRODUCTS.has(product)) return null;
  if (iso !== ALL && !ISO_RE.test(iso)) return null;
  if (adm !== ALL && !ADM_RE.test(adm)) return null;
  return { version, product, iso, adm };
}

function metadataKey(version: string, product: string, iso: string, adm: string): string {
  return `${version}/${product}/${iso}/${adm}/geoBoundaries-${iso}-${adm}-metaData.json`;
}

function indexKey(version: string, product: string): string {
  return `${version}/${product}/index.json`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMetadataIndex(value: unknown): value is Record<string, unknown>[] {
  return Array.isArray(value) && value.every((record) => {
    if (!isRecord(record)) return false;
    const iso = record.boundaryISO;
    const adm = record.boundaryType;
    return typeof iso === "string" && ISO_RE.test(iso) && iso !== ALL
      && typeof adm === "string" && ADM_RE.test(adm);
  });
}

async function resolveCurrent(env: Env, ctx: ExecutionContext): Promise<string | null> {
  const cache = caches.default;
  const cacheKey = new Request(CURRENT_CACHE_KEY, { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) {
    const data = (await cached.json()) as { version?: string };
    return data.version ?? null;
  }

  const obj = await env.BUCKET.get("current.json");
  if (!obj) return null;
  let data: { version?: string };
  try {
    data = await obj.json();
  } catch {
    return null;
  }
  if (!data.version) return null;

  const cacheResp = new Response(JSON.stringify(data), {
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${CURRENT_TTL_SECONDS}`,
    },
  });
  ctx.waitUntil(cache.put(cacheKey, cacheResp));
  return data.version;
}

/**
 * Replace the host portion of every URL field with our R2 custom domain.
 * The build pipeline writes bare file names (older records carry GitHub raw
 * URLs); we keep only the file name and re-anchor it at
 * data.geoboundaries-dev.org/{version}/...
 *
 * Mutates `meta` in place.
 */
function rewriteUrls(
  meta: Record<string, unknown>,
  dataDomain: string,
  version: string,
  product: string,
  iso: string,
  adm: string,
): void {
  const base = `https://${dataDomain}/${version}/${product}/${iso}/${adm}`;
  for (const field of URL_FIELDS) {
    const v = meta[field];
    if (typeof v !== "string") continue;
    const basename = v.split("/").pop();
    if (basename) meta[field] = `${base}/${basename}`;
  }
}

function jsonError(status: number, message: string, extra: Record<string, unknown> = {}): Response {
  return new Response(JSON.stringify({ error: message, ...extra }, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    },
  });
}
