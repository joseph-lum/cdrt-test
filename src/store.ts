import * as Automerge from "@automerge/automerge";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import type { FieldDocument } from "./types.js";

const GENESIS_ACTOR = "00000000000000000000000000000000";

export function createFieldDocument(actorId: string): Automerge.Doc<FieldDocument> {
  // Every replica must share the same root objects. Loading a deterministic
  // genesis change with a new actor gives each device a common history plus a
  // unique identity for subsequent local changes.
  const empty = Automerge.init<FieldDocument>({ actor: GENESIS_ACTOR });
  const genesis = Automerge.change(empty, { message: "FieldMesh schema v1", time: 0 }, (draft) => {
    draft.schemaVersion = 1;
    draft.messages = [];
    draft.tracks = {};
    draft.drawings = [];
  });
  return Automerge.load<FieldDocument>(Automerge.save(genesis), { actor: actorId });
}

export class DocumentStore {
  readonly filePath: string;
  private pendingSave: NodeJS.Timeout | undefined;

  constructor(
    private readonly dataDir: string,
    room: string,
    private readonly actorId: string,
  ) {
    const safeRoom = room.replace(/[^a-zA-Z0-9_-]/g, "_");
    this.filePath = path.join(dataDir, `${safeRoom}.automerge`);
  }

  async load(): Promise<Automerge.Doc<FieldDocument>> {
    await mkdir(this.dataDir, { recursive: true });
    try {
      const bytes = await readFile(this.filePath);
      return Automerge.load<FieldDocument>(bytes, { actor: this.actorId });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      return createFieldDocument(this.actorId);
    }
  }

  scheduleSave(getDocument: () => Automerge.Doc<FieldDocument>): void {
    clearTimeout(this.pendingSave);
    this.pendingSave = setTimeout(() => {
      void this.save(getDocument()).catch((error) => console.error("Failed to save document", error));
    }, 100);
  }

  async save(document: Automerge.Doc<FieldDocument>): Promise<void> {
    await writeFile(this.filePath, Automerge.save(document));
  }
}
