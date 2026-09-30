import assert from "node:assert/strict";
import { beforeEach, describe, test } from "node:test";

import worker, { parseApiPath } from "../.test-dist/index.js";

const records = [
  {
    boundaryISO: "CAN",
    boundaryType: "ADM0",
    staticDownloadLink: "https://legacy.example/geoBoundaries-CAN-ADM0-all.zip",
    gjDownloadURL: "https://legacy.example/geoBoundaries-CAN-ADM0.geojson",
  },
  {
    boundaryISO: "USA",
    boundaryType: "ADM0",
    staticDownloadLink: "https://legacy.example/geoBoundaries-USA-ADM0-all.zip",
  },
  {
    boundaryISO: "USA",
    boundaryType: "ADM1",
    imagePreview: "https://legacy.example/geoBoundaries-USA-ADM1-PREVIEW.png",
  },
];

function r2Object(value) {
  return {
    async json() {
      if (value instanceof Error) throw value;
      return structuredClone(value);
    },
  };
}

function environment(objects) {
  return {
    DATA_DOMAIN: "data.geoboundaries.test",
    BUCKET: {
      async get(key) {
        return Object.hasOwn(objects, key) ? r2Object(objects[key]) : null;
      },
    },
  };
}

function executionContext() {
  const pending = [];
  return {
    pending,
    waitUntil(promise) {
      pending.push(promise);
    },
  };
}

beforeEach(() => {
  const cached = new Map();
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: {
      default: {
        async match(request) {
          return cached.get(request.url)?.clone();
        },
        async put(request, response) {
          cached.set(request.url, response.clone());
        },
      },
    },
  });
});

describe("API path parsing", () => {
  test("accepts concrete and ALL selectors with optional trailing slashes", () => {
    assert.deepEqual(parseApiPath("/api/v7/gbOpen/USA/ADM1/"), {
      version: "v7",
      product: "gbOpen",
      iso: "USA",
      adm: "ADM1",
    });
    assert.deepEqual(parseApiPath("/api/current/gbOpen/ALL/ALL"), {
      version: "current",
      product: "gbOpen",
      iso: "ALL",
      adm: "ALL",
    });
    assert.equal(parseApiPath("/api/v7/gbOpen/USA/all/"), null);
    assert.equal(parseApiPath("/api/v7/unknown/ALL/ALL/"), null);
  });
});

describe("Worker responses", () => {
  test("keeps concrete lookups as an object and rewrites their URLs", async () => {
    const env = environment({
      "v7/gbOpen/USA/ADM0/geoBoundaries-USA-ADM0-metaData.json": records[1],
    });
    const ctx = executionContext();
    const response = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/v7/gbOpen/USA/ADM0/"),
      env,
      ctx,
    );
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(Array.isArray(body), false);
    assert.equal(
      body.staticDownloadLink,
      "https://data.geoboundaries.test/v7/gbOpen/USA/ADM0/geoBoundaries-USA-ADM0-all.zip",
    );
    assert.equal(response.headers.get("X-GB-Version"), "v7");
    await Promise.all(ctx.pending);
  });

  test("anchors the builder's bare download file names at the data domain", async () => {
    const env = environment({
      "nightly/gbOpen/KEN/ADM1/geoBoundaries-KEN-ADM1-metaData.json": {
        boundaryISO: "KEN",
        boundaryType: "ADM1",
        staticDownloadLink: "geoBoundaries-KEN-ADM1-all.zip",
        gjDownloadURL: "geoBoundaries-KEN-ADM1.geojson",
        tjDownloadURL: "geoBoundaries-KEN-ADM1.topojson",
        imagePreview: "geoBoundaries-KEN-ADM1-PREVIEW.png",
        simplifiedGeometryGeoJSON: "geoBoundaries-KEN-ADM1_simplified.geojson",
      },
    });
    const ctx = executionContext();
    const response = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/nightly/gbOpen/KEN/ADM1/"),
      env,
      ctx,
    );
    const body = await response.json();

    const base = "https://data.geoboundaries.test/nightly/gbOpen/KEN/ADM1";
    assert.equal(response.status, 200);
    assert.equal(body.staticDownloadLink, `${base}/geoBoundaries-KEN-ADM1-all.zip`);
    assert.equal(body.gjDownloadURL, `${base}/geoBoundaries-KEN-ADM1.geojson`);
    assert.equal(body.tjDownloadURL, `${base}/geoBoundaries-KEN-ADM1.topojson`);
    assert.equal(body.imagePreview, `${base}/geoBoundaries-KEN-ADM1-PREVIEW.png`);
    assert.equal(
      body.simplifiedGeometryGeoJSON,
      `${base}/geoBoundaries-KEN-ADM1_simplified.geojson`,
    );
    await Promise.all(ctx.pending);
  });

  for (const [selectors, expected] of [
    ["USA/ALL", ["USA/ADM0", "USA/ADM1"]],
    ["ALL/ADM0", ["CAN/ADM0", "USA/ADM0"]],
    ["ALL/ADM1", ["USA/ADM1"]],
    ["ALL/ALL", ["CAN/ADM0", "USA/ADM0", "USA/ADM1"]],
  ]) {
    test(`serves ${selectors} from the product index`, async () => {
      const env = environment({ "v7/gbOpen/index.json": records });
      const ctx = executionContext();
      const response = await worker.fetch(
        new Request(`https://www.geoboundaries.test/api/v7/gbOpen/${selectors}/`),
        env,
        ctx,
      );
      const body = await response.json();

      assert.equal(response.status, 200);
      assert.deepEqual(
        body.map((record) => `${record.boundaryISO}/${record.boundaryType}`),
        expected,
      );
      for (const record of body) {
        for (const field of ["staticDownloadLink", "gjDownloadURL", "imagePreview"]) {
          if (record[field]) {
            assert.match(
              record[field],
              new RegExp(`^https://data\\.geoboundaries\\.test/v7/gbOpen/${record.boundaryISO}/${record.boundaryType}/`),
            );
          }
        }
      }
      await Promise.all(ctx.pending);
    });
  }

  test("resolves current before loading an aggregate index", async () => {
    const env = environment({
      "current.json": { version: "v7" },
      "v7/gbOpen/index.json": records,
    });
    const ctx = executionContext();
    const response = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/current/gbOpen/USA/ADM9/"),
      env,
      ctx,
    );
    assert.equal(response.status, 404);

    const aggregate = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/current/gbOpen/USA/ALL/"),
      env,
      ctx,
    );
    assert.equal(aggregate.status, 200);
    assert.equal(aggregate.headers.get("X-GB-Version"), "v7");
    assert.equal((await aggregate.json()).length, 2);
    await Promise.all(ctx.pending);
  });

  test("returns an empty array when an aggregate filter has no matches", async () => {
    const env = environment({ "v7/gbOpen/index.json": records });
    const response = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/v7/gbOpen/CAN/ADM5/"),
      env,
      executionContext(),
    );
    assert.equal(response.status, 404);

    const aggregate = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/v7/gbOpen/ALL/ADM5/"),
      env,
      executionContext(),
    );
    assert.equal(aggregate.status, 200);
    assert.deepEqual(await aggregate.json(), []);
  });

  test("distinguishes missing and malformed aggregate indexes", async () => {
    const missing = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/v7/gbOpen/ALL/ALL/"),
      environment({}),
      executionContext(),
    );
    assert.equal(missing.status, 404);

    const malformed = await worker.fetch(
      new Request("https://www.geoboundaries.test/api/v7/gbOpen/ALL/ALL/"),
      environment({
        "v7/gbOpen/index.json": [{ boundaryISO: "USA" }],
      }),
      executionContext(),
    );
    assert.equal(malformed.status, 500);
  });
});
