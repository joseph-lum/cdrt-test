import dgram from "node:dgram";
import { describe, expect, it } from "vitest";
import { createAnnouncement, Discovery } from "../src/discovery.js";
import { WireLogger } from "../src/wire-log.js";

describe("request/response discovery", () => {
  it("discovers an existing same-host process through UDP loopback", async () => {
    const discoveryPort = 53_000 + (process.pid % 1_000);
    const room = `same-host-${process.pid}-${Date.now()}`;
    const alphaId = "11111111111111111111111111111111";
    const bravoId = "22222222222222222222222222222222";
    const found: Array<{ peerId: string; address: string }> = [];
    const alpha = new Discovery(
      createAnnouncement(alphaId, "Alpha", room, 4310),
      discoveryPort, "239.255.42.101", () => undefined, new WireLogger("off", alphaId),
    );
    const bravo = new Discovery(
      createAnnouncement(bravoId, "Bravo", room, 4320),
      discoveryPort, "239.255.42.101",
      (peer, address) => found.push({ peerId: peer.peerId, address }),
      new WireLogger("off", bravoId),
    );

    try {
      await alpha.start();
      await bravo.start();
      await waitUntil(() => found.length === 1, 1_500);
      expect(found).toEqual([{ peerId: alphaId, address: "127.0.0.1" }]);
    } finally {
      bravo.close();
      alpha.close();
    }
  });

  it("deduplicates a discovery burst and replies by unicast after bounded jitter", async () => {
    const discoveryPort = 52_000 + (process.pid % 1_000);
    const responderId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const discovery = new Discovery(
      createAnnouncement(responderId, "Bravo", "discovery-test", 4310),
      discoveryPort,
      "239.255.42.100",
      () => undefined,
      new WireLogger("off", responderId),
    );
    const requester = dgram.createSocket("udp4");

    try {
      await discovery.start();
      await new Promise<void>((resolve) => requester.bind(0, "127.0.0.1", resolve));
      const offers: unknown[] = [];
      requester.on("message", (data) => offers.push(JSON.parse(data.toString("utf8"))));
      const request = Buffer.from(JSON.stringify({
        protocol: "fieldmesh-discovery-v2",
        type: "discover",
        requestId: "fixed-request-id",
        peerId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        name: "Alpha",
        room: "discovery-test",
        port: 4320,
      }));

      requester.send(request, discoveryPort, "127.0.0.1");
      requester.send(request, discoveryPort, "127.0.0.1");
      requester.send(request, discoveryPort, "127.0.0.1");
      await waitUntil(() => offers.length === 1, 1_500);
      await new Promise((resolve) => setTimeout(resolve, 800));

      expect(offers).toHaveLength(1);
      expect(offers[0]).toMatchObject({
        protocol: "fieldmesh-discovery-v2",
        type: "offer",
        replyTo: "fixed-request-id",
        peerId: responderId,
      });
    } finally {
      discovery.close();
      requester.close();
    }
  });
});

async function waitUntil(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for discovery offer");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
