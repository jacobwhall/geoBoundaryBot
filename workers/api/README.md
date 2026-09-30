# geoBoundaries API Worker

Cloudflare Worker that serves
`/api/{version}/{product}/{ISO|ALL}/{ADM|ALL}/` on
`www.geoboundaries.org`. It reads per-boundary metadata or a prebuilt product
index from R2 and rewrites embedded download URLs to point at
`data.geoboundaries.org`.

## Architecture

```
                                              ┌─────────────────────────┐
GET www.geoboundaries.org/api/current/...     │  R2 bucket              │
              │                               │  ├─ current.json        │
              ▼                               │  ├─ nightly/...         │
       ┌──────────────┐         R2 binding    │  ├─ v6/                 │
       │  This Worker │ ◄────────────────────►│  │  └─ gbOpen/          │
       │              │                       │  │     ├─ index.json    │
       │  • parse URL │                       │  │     └─ KEN/ADM1/     │
       │  • resolve   │                       │  │        └─ ...-meta   │
       │    current   │                       │  └─ v7/...              │
       │  • rewrite   │                       └─────────────────────────┘
       │    URLs      │
       └──────────────┘
              │
              ▼
       JSON response with
       data.geoboundaries.org/v6/... URLs
```

Static download URLs (`gjDownloadURL`, `staticDownloadLink`, etc.) point
at `data.geoboundaries.org`, which is configured as a public R2 custom
domain — no Worker involved on that path; Cloudflare's edge serves
objects directly from the bucket.

## URL behaviour

- `/api/current/gbOpen/KEN/ADM1/` → resolves `current` to a real version
  via the bucket's `current.json`, then returns the rewritten metadata
  for that version's `KEN/ADM1` boundary.
- `/api/v6/gbOpen/KEN/ADM1/` → skips the `current.json` lookup and
  returns `v6/KEN/ADM1` directly.
- `/api/current/gbOpen/KEN/ALL/` → returns every available ADM level for
  Kenya as a JSON array.
- `/api/current/gbOpen/ALL/ADM1/` → returns every available ADM1 boundary
  as a JSON array.
- `/api/current/gbOpen/ALL/ALL/` → returns every gbOpen metadata record as
  a JSON array. This is the endpoint used by the downloads page.
- Trailing slash is optional.

Requests with concrete ISO and ADM selectors return a JSON object. A request
with `ALL` in either selector returns a JSON array, including when it has zero
or one matching record.

The response shape is identical to the legacy API — same field names,
same JSON keys. Only the values of `staticDownloadLink`, `gjDownloadURL`,
`tjDownloadURL`, `imagePreview`, and `simplifiedGeometryGeoJSON` differ:
they now point at `data.geoboundaries.org/{version}/...` instead of
GitHub raw URLs.

## One-time Cloudflare setup

1. **R2 bucket.** Create a bucket named `geoboundaries` in the Cloudflare
   dashboard (R2 → Create bucket).

2. **Public custom domain on the bucket.** R2 bucket → Settings → Public
   access → Connect Domain. Use `data.geoboundaries.org`. Cloudflare
   provisions DNS and a TLS cert automatically. No Worker required.

3. **Worker route.** Once this Worker is deployed (`wrangler deploy`),
   the route in `wrangler.toml` attaches it to
   `www.geoboundaries.org/api/*`. Requests to that path are intercepted
   before they reach the existing origin; everything else on `www.`
   continues to hit the docsify site.

4. **R2 credentials for the build pipeline.** Generate an R2 API token
   (R2 → Manage R2 API Tokens) with read+write on the bucket, and create
   the Kubernetes secret the build CronJob expects:

   ```bash
   kubectl -n geoboundaries create secret generic gb-s3-credentials \
     --from-literal=access-key-id=<key> \
     --from-literal=secret-access-key=<secret>
   ```

   Then in `values.yaml`:

   ```yaml
   s3:
     enabled: true
     endpoint: "https://<account-id>.r2.cloudflarestorage.com"
     bucket: geoboundaries
   ```

## Deploy

```bash
cd workers/api
npm install
npx wrangler deploy
```

The first deploy will prompt for Cloudflare auth.

## Local development

```bash
npm run typecheck
npm test
npx wrangler dev
```

Hits the real R2 bucket by default. Pass `--local` to use a local
emulator, but note that the bucket will be empty in that case.

## Cache behaviour

- Per-boundary metadata responses: cached at the edge for 5 minutes
  (`META_TTL_SECONDS`).
- `current.json` lookup: cached per-edge for 60 seconds
  (`CURRENT_TTL_SECONDS`), so a release promotion becomes visible within
  ~1 minute even without an explicit CDN purge.

Bump those constants in `src/index.ts` if you want different TTLs.

## Aggregate indexes

The build pipeline writes one JSON array per product at
`{version}/{product}/index.json`. The Worker filters that index for every
request containing `ALL`, avoiding an R2 list plus hundreds of object reads on
each edge-cache miss. A release must publish all three product indexes before
`current.json` is promoted.

When introducing the Worker route for the first time, populate the indexes for
the version named by `current.json` before deploying the route. Otherwise the
aggregate endpoints correctly report that their release index is missing.
