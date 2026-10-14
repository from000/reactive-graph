import { PROTOCOL_NAME, PROTOCOL_VERSION } from "../src/version.js";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { describe, expect, it } from "vitest";

const here = path.dirname(fileURLToPath(import.meta.url));
const fixture = JSON.parse(
  readFileSync(path.resolve(here, "../../../docs/spec/protocol-version.json"), "utf8"),
);

describe("RGP/1 protocol version", () => {
  it("matches the shared cross-language fixture", () => {
    expect(PROTOCOL_NAME).toBe(fixture.protocolName);
    expect(PROTOCOL_VERSION).toBe(fixture.protocolVersion);
  });

  it("is the exact integer 1", () => {
    expect(PROTOCOL_VERSION).toBe(1);
    expect(Number.isInteger(PROTOCOL_VERSION)).toBe(true);
  });
});
