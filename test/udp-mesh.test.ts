import * as Automerge from "@automerge/automerge";
import { randomBytes } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { Mesh } from "../src/mesh.js";
import { createFieldDocument } from "../src/store.js";
import type { FieldDocument } from "../src/types.js";
import { WireLogger } from "../src/wire-log.js";

const meshes: Mesh[] = [];

afterEach(() => {
  for (const mesh of meshes) mesh.close();
  meshes.length = 0;
});

describe("UDP peer mesh", () => {
  it("fragments, acknowledges, and converges a large Automerge change", async () => {
    const alphaId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const bravoId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const basePort = 40_000 + (process.pid % 10_000);
    let alpha = createFieldDocument(alphaId);
    let bravo = createFieldDocument(bravoId);

    const alphaMesh = makeMesh(alphaId, "Alpha", basePort, () => alpha, (next) => { alpha = next });
    const bravoMesh = makeMesh(bravoId, "Bravo", basePort + 1, () => bravo, (next) => { bravo = next });
    meshes.push(alphaMesh, bravoMesh);
    await Promise.all([alphaMesh.start(), bravoMesh.start()]);

    alphaMesh.discovered(bravoId, "Bravo", "127.0.0.1", basePort + 1);
    bravoMesh.discovered(alphaId, "Alpha", "127.0.0.1", basePort);
    await waitUntil(() => alphaMesh.peers().length === 1 && bravoMesh.peers().length === 1);

    const text = randomBytes(950).toString("hex");
    alpha = Automerge.change(alpha, (draft) => {
      draft.messages.push({ id: "large", author: "Alpha", text, createdAt: 1 });
    });
    alphaMesh.documentChanged();

    await waitUntil(() => bravo.messages.some((message) => message.id === "large"));
    await waitUntil(() => alphaMesh.peers()[0]?.ourChangesAcknowledged === true);
    expect(bravo.messages[0]?.text).toBe(text);
    expect(Automerge.getHeads(bravo)).toEqual(Automerge.getHeads(alpha));
    expect(alphaMesh.peers()[0]?.pendingDatagrams).toBe(0);

    // Restart the peer immediately, before Alpha's stale-peer timeout, and
    // verify the new UDP session resets sequence state and catches up.
    bravoMesh.close();
    alpha = Automerge.change(alpha, (draft) => {
      draft.messages.push({ id: "offline", author: "Alpha", text: "created during restart", createdAt: 2 });
    });
    alphaMesh.documentChanged();
    const restartedBravo = makeMesh(bravoId, "Bravo", basePort + 1, () => bravo, (next) => { bravo = next });
    meshes.push(restartedBravo);
    await restartedBravo.start();
    restartedBravo.discovered(alphaId, "Alpha", "127.0.0.1", basePort);

    await waitUntil(() => bravo.messages.some((message) => message.id === "offline"));
    expect(Automerge.getHeads(bravo)).toEqual(Automerge.getHeads(alpha));
  });
});

function makeMesh(
  peerId: string,
  name: string,
  port: number,
  getDocument: () => Automerge.Doc<FieldDocument>,
  setDocument: (document: Automerge.Doc<FieldDocument>) => void,
): Mesh {
  return new Mesh({
    peerId, name, room: "udp-test", host: "127.0.0.1", port, getDocument, setDocument,
    onStatus: () => undefined, wireLog: new WireLogger("off", peerId),
  });
}

async function waitUntil(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for UDP mesh state");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
