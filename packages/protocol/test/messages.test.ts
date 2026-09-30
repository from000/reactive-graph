import { describe, expect, it } from "vitest";
import { METHOD_LIST, isEnvelope } from "../src/messages.js";

describe("RGP/1 method list", () => {
  it("includes the read-only trace query method", () => {
    expect(METHOD_LIST).toContain("TRACE_QUERY");
    expect(
      isEnvelope({
        version: 1,
        id: "r1",
        kind: "request",
        method: "TRACE_QUERY",
        payload: { runId: "run-1" },
      }),
    ).toBe(true);
  });
});
