import * as Automerge from "@automerge/automerge";

export type WireLogLevel = "off" | "summary" | "full";

interface LogDetails {
  channel: "discovery" | "peer" | "client" | "http";
  direction: "tx" | "rx" | "event";
  event: string;
  remote?: string;
  bytes?: number;
  payload?: unknown;
  raw?: Uint8Array | string;
  outcome?: string;
}

export class WireLogger {
  constructor(
    private readonly level: WireLogLevel,
    private readonly localPeerId: string,
  ) {}

  get enabled(): boolean {
    return this.level !== "off";
  }

  log(details: LogDetails): void {
    if (!this.enabled) return;
    const entry: Record<string, unknown> = {
      timestamp: new Date().toISOString(),
      localPeerId: this.localPeerId,
      ...details,
    };
    delete entry.raw;
    delete entry.payload;
    if (details.payload !== undefined) entry.payload = makeJsonSafe(details.payload, this.level === "full");
    if (this.level === "full" && details.raw !== undefined) {
      const bytes = typeof details.raw === "string" ? Buffer.from(details.raw) : Buffer.from(details.raw);
      entry.rawBase64 = bytes.toString("base64");
    }
    console.log(`[wire] ${JSON.stringify(entry)}`);
  }

  sync(direction: "tx" | "rx", remote: string | undefined, message: Uint8Array): void {
    let decoded: unknown;
    try {
      decoded = Automerge.decodeSyncMessage(message);
    } catch (error) {
      decoded = { decodeError: error instanceof Error ? error.message : String(error) };
    }
    this.log({
      channel: "peer",
      direction,
      event: "automerge.sync",
      remote,
      bytes: message.byteLength,
      payload: decoded,
      raw: message,
    });
  }
}

function makeJsonSafe(value: unknown, includeBinary: boolean): unknown {
  if (value instanceof Uint8Array) {
    return includeBinary
      ? { type: "Uint8Array", bytes: value.byteLength, base64: Buffer.from(value).toString("base64") }
      : { type: "Uint8Array", bytes: value.byteLength };
  }
  if (Array.isArray(value)) return value.map((item) => makeJsonSafe(item, includeBinary));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, makeJsonSafe(item, includeBinary)]));
  }
  if (typeof value === "bigint") return value.toString();
  return value;
}
