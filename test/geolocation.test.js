import test from "node:test";
import assert from "node:assert/strict";
import { geolocateIps } from "../lib/geolocation.js";

function response(payload, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() { return payload; }
  };
}

test("country lookups are split into API-safe batches of 100", async () => {
  const ips = Array.from({ length: 205 }, (_, index) => `192.0.2.${index + 1}`);
  const batchSizes = [];
  const result = await geolocateIps(ips, {
    pauseMs: 0,
    fetchImpl: async (_url, options) => {
      const batch = JSON.parse(options.body);
      batchSizes.push(batch.length);
      return response(batch.map((ip) => ({ ip, country: "FR" })));
    }
  });

  assert.deepEqual(batchSizes, [100, 100, 5]);
  assert.equal(result.requested, 205);
  assert.equal(result.resolved, 205);
  assert.equal(result.map["192.0.2.205"], "FR");
  assert.deepEqual(result.warnings, []);
});

test("an error object in one batch does not discard successful batches", async () => {
  const ips = Array.from({ length: 101 }, (_, index) => `198.51.100.${index + 1}`);
  let call = 0;
  const result = await geolocateIps(ips, {
    pauseMs: 0,
    fetchImpl: async (_url, options) => {
      call++;
      const batch = JSON.parse(options.body);
      if (call === 1) return response({ error: "temporary failure" }, 429);
      return response({ results: batch.map((ip) => ({ ip, country: "US" })) });
    }
  });

  assert.equal(result.resolved, 1);
  assert.equal(result.map["198.51.100.101"], "US");
  assert.match(result.warnings[0], /429.*temporary failure/);
});

test("invalid subscriber IP values are skipped before calling country.is", async () => {
  const result = await geolocateIps(["not-an-ip", "203.0.113.9"], {
    pauseMs: 0,
    fetchImpl: async (_url, options) => {
      const batch = JSON.parse(options.body);
      assert.deepEqual(batch, ["203.0.113.9"]);
      return response([{ ip: "203.0.113.9", country: "de" }]);
    }
  });

  assert.equal(result.valid, 1);
  assert.equal(result.map["203.0.113.9"], "DE");
  assert.match(result.warnings[0], /Skipped 1 invalid/);
});
