import { afterEach, describe, expect, it, vi } from "vitest";
import { WireLogger } from "../src/wire-log.js";

describe("trace classification", () => {
  afterEach(() => vi.restoreAllMocks());

  it("distinguishes LAN UDP packets from local decoded and browser records", () => {
    const output: string[] = [];
    vi.spyOn(console, "log").mockImplementation((line) => output.push(String(line)));
    const logger = new WireLogger("summary", "local-peer");

    logger.log({ channel: "peer", direction: "tx", event: "udp.data", remote: "192.168.1.8:4310", bytes: 75 });
    logger.log({ channel: "peer", direction: "rx", event: "udp.ack", remote: "192.168.1.8:4310", bytes: 28 });
    logger.sync("tx", "remote-peer", new Uint8Array([0]));
    logger.log({ channel: "client", direction: "tx", event: "state", bytes: 973 });

    const records = output.map((line) => JSON.parse(line.slice(line.indexOf(" ") + 1)) as Record<string, unknown>);
    expect(output[0]?.startsWith("[comms] ")).toBe(true);
    expect(output[1]?.startsWith("[comms] ")).toBe(true);
    expect(output[2]?.startsWith("[trace] ")).toBe(true);
    expect(output[3]?.startsWith("[trace] ")).toBe(true);
    expect(records[0]).toMatchObject({ recordKind: "udp-datagram", networkScope: "lan" });
    expect(records[1]).toMatchObject({ recordKind: "udp-datagram", networkScope: "lan", direction: "rx" });
    expect(records[2]).toMatchObject({ recordKind: "decoded-payload", networkScope: "process" });
    expect(records[3]).toMatchObject({ recordKind: "websocket-message", networkScope: "local-client" });
  });
});
