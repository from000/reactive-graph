import { describe, expect, it } from "vitest";
import { FaultInjector, FakeClock, TESTKIT_VERSION } from "../src/index.js";

describe("testkit", () => {
  it("exposes a version constant", () => {
    expect(TESTKIT_VERSION).toBe("0.1.0");
  });

  it("FakeClock controls time deterministically", () => {
    const clock = new FakeClock(100);
    expect(clock.now()).toBe(100);
    clock.advance(250);
    expect(clock.now()).toBe(350);
  });

  it("FaultInjector fails N times then succeeds", () => {
    const injector = new FaultInjector(2);
    expect(injector.shouldFail()).toBe(true);
    expect(injector.shouldFail()).toBe(true);
    expect(injector.shouldFail()).toBe(false);
    expect(injector.remaining()).toBe(0);
  });
});
