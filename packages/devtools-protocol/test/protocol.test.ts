import { describe, expect, it } from "vitest";
import { TRACE_EVENT_KINDS, isTraceEventKind, pathToDotted } from "../src/index.js";

describe("devtools-protocol (Task 12 / D.9.106)", () => {
  it("defines the stable causal-trace event kinds", () => {
    expect(TRACE_EVENT_KINDS).toContain("commit");
    expect(TRACE_EVENT_KINDS).toContain("invalidate");
    expect(TRACE_EVENT_KINDS).toContain("patch");
    expect(TRACE_EVENT_KINDS).toContain("checkpoint");
    expect(isTraceEventKind("span_start")).toBe(true);
    expect(isTraceEventKind("nope")).toBe(false);
  });

  it("renders dotted paths for tooling", () => {
    expect(pathToDotted(["a", "b"])).toBe("a.b");
    expect(pathToDotted(["a", 0, "c"])).toBe("a[0].c");
  });
});
