import * as Automerge from "@automerge/automerge";
import { describe, expect, it } from "vitest";
import type { FieldDocument } from "../src/types.js";
import { createFieldDocument } from "../src/store.js";

function blank(actor: string): Automerge.Doc<FieldDocument> {
  return createFieldDocument(actor);
}

function synchronize(left: Automerge.Doc<FieldDocument>, right: Automerge.Doc<FieldDocument>) {
  let leftState = Automerge.initSyncState();
  let rightState = Automerge.initSyncState();
  for (let round = 0; round < 20; round += 1) {
    let leftMessage;
    [leftState, leftMessage] = Automerge.generateSyncMessage(left, leftState);
    if (leftMessage) [right, rightState] = Automerge.receiveSyncMessage(right, rightState, leftMessage);

    let rightMessage;
    [rightState, rightMessage] = Automerge.generateSyncMessage(right, rightState);
    if (rightMessage) [left, leftState] = Automerge.receiveSyncMessage(left, leftState, rightMessage);
    if (!leftMessage && !rightMessage) return [left, right] as const;
  }
  throw new Error("Sync did not quiesce");
}

describe("Automerge field document", () => {
  it("converges after concurrent offline edits", () => {
    let alpha = blank("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let bravo = blank("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    expect(Automerge.getHeads(alpha)).toEqual(Automerge.getHeads(bravo));

    alpha = Automerge.change(alpha, (draft) => {
      draft.messages.push({ id: "a1", author: "Alpha", text: "Checkpoint set", createdAt: 1 });
    });
    bravo = Automerge.change(bravo, (draft) => {
      draft.messages.push({ id: "b1", author: "Bravo", text: "Moving north", createdAt: 2 });
      draft.tracks.b1 = { id: "b1", callsign: "BRAVO-1", affiliation: "friendly", latitude: 1, longitude: 2, updatedAt: 2, updatedBy: "Bravo" };
    });

    [alpha, bravo] = synchronize(alpha, bravo);

    expect(Automerge.getHeads(alpha)).toEqual(Automerge.getHeads(bravo));
    expect(alpha.messages.map((message) => message.id).sort()).toEqual(["a1", "b1"]);
    expect(bravo.tracks.b1?.callsign).toBe("BRAVO-1");
  });
});
