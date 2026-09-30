/**
 * Redis durable backend (M3-F8): cache over ioredis.
 *
 * Values are opaque serializer bytes stored as redis strings; TTL entries use
 * PX expiry so the cache self-evicts. Keys are namespaced with an `rgp:cache:`
 * prefix to keep the redis database shareable with other applications.
 */

import { Redis } from "ioredis";
import type { Cache, CacheEntry } from "./types.js";

const KEY_PREFIX = "rgp:cache:";

function keyOf(key: string): string {
  return `${KEY_PREFIX}${key}`;
}

export class RedisCache implements Cache {
  private readonly client: Redis;

  constructor(url: string) {
    this.client = new Redis(url);
  }

  async get(key: string): Promise<CacheEntry | null> {
    const raw = await this.client.getBuffer(keyOf(key));
    if (!raw) return null;
    const ttlSeconds = await this.client.ttl(keyOf(key));
    return {
      value: new Uint8Array(raw),
      expiresAt: ttlSeconds > 0 ? Date.now() + ttlSeconds * 1000 : undefined,
    };
  }

  async set(key: string, entry: CacheEntry): Promise<void> {
    const raw = Buffer.from(entry.value);
    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      // Already expired: treat as absent (delete any stale entry).
      await this.client.del(keyOf(key));
      return;
    }
    if (entry.expiresAt !== undefined && entry.expiresAt > 0) {
      const ms = Math.max(1, entry.expiresAt - Date.now());
      await this.client.set(keyOf(key), raw, "PX", ms);
    } else {
      await this.client.set(keyOf(key), raw);
    }
  }

  async delete(key: string): Promise<void> {
    await this.client.del(keyOf(key));
  }

  async has(key: string): Promise<boolean> {
    return (await this.client.exists(keyOf(key))) === 1;
  }

  async close(): Promise<void> {
    await this.client.quit();
  }
}
