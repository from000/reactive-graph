import { describe, expect, it } from "vitest";
import { RedisCache } from "../../src/storage/index.js";

/**
 * M3-F8: Redis cache backend. Run against a real redis when reachable; skip
 * when not (set RGP_REDIS_URL, e.g. in CI).
 */
const URL = process.env["RGP_REDIS_URL"] ?? "redis://127.0.0.1:6379/15";

const redisAvailable = await (async () => {
  try {
    const cache = new RedisCache(URL);
    await cache.set("__probe__", { value: new Uint8Array([1]) });
    await cache.delete("__probe__");
    await cache.close();
    return true;
  } catch {
    return false;
  }
})();

const OR_DISABLED = redisAvailable ? "" : " (redis unavailable; set RGP_REDIS_URL)";

describe.skipIf(!redisAvailable)(`RedisCache${OR_DISABLED}`, () => {
  it("set/get/has round-trips values", async () => {
    const cache = new RedisCache(URL);
    try {
      await cache.set("k1", { value: new Uint8Array([7, 8, 9]) });
      expect(await cache.has("k1")).toBe(true);
      expect(Array.from((await cache.get("k1"))!.value)).toEqual([7, 8, 9]);
    } finally {
      await cache.delete("k1");
      await cache.close();
    }
  });

  it("TTL entries expire", async () => {
    const cache = new RedisCache(URL);
    try {
      await cache.set("kttl", { value: new Uint8Array([1]), expiresAt: Date.now() - 1 });
      expect(await cache.get("kttl")).toBeNull();
    } finally {
      await cache.delete("kttl");
      await cache.close();
    }
  });

  it("delete removes a key", async () => {
    const cache = new RedisCache(URL);
    try {
      await cache.set("kdel", { value: new Uint8Array([1]) });
      await cache.delete("kdel");
      expect(await cache.has("kdel")).toBe(false);
    } finally {
      await cache.close();
    }
  });
});
