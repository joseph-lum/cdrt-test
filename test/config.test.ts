import { describe, expect, it } from "vitest";
import { readConfig } from "../src/config.js";

describe("wire log configuration", () => {
  it("enables full payload logging with a bare flag", () => {
    expect(readConfig(["--wire-log"], {}).wireLog).toBe("full");
  });

  it("supports summary mode and defaults to off", () => {
    expect(readConfig(["--wire-log=summary"], {}).wireLog).toBe("summary");
    expect(readConfig([], {}).wireLog).toBe("off");
  });
});
