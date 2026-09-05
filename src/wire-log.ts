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

type TraceKind = "udp-datagram" | "websocket-message" | "http-request" |
  "decoded-payload" | "socket-result" | "local-event";

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
      recordKind: traceKind(details),
      networkScope: networkScope(details),
      ...details,
    };
    delete entry.raw;
    delete entry.payload;
    if (details.payload !== undefined) entry.payload = makeJsonSafe(details.payload, this.level === "full");
    if (this.level === "full" && details.raw !== undefined) {
      const bytes = typeof details.raw === "string" ? Buffer.from(details.raw) : Buffer.from(details.raw);
      entry.rawBase64 = bytes.toString("base64");
    }
    const prefix = traceKind(details) === "udp-datagram" ? "[comms]" : "[trace]";
    console.log(`${prefix} ${JSON.stringify(entry)}`);
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

function traceKind(details: LogDetails): TraceKind {
  if (details.channel === "client") return "websocket-message";
  if (details.channel === "http") return "http-request";
  if (details.event === "automerge.sync") return "decoded-payload";
  if (details.event === "socket.send" || details.event === "udp.socket-send") return "socket-result";
  if ((details.channel === "peer" && details.direction !== "event" && details.event.startsWith("udp.")) ||
      (details.channel === "discovery" &&
       ((details.direction === "tx" && details.event.startsWith("udp.")) ||
        (details.direction === "rx" && details.event === "udp.datagram")))) {
    return "udp-datagram";
  }
  return "local-event";
}

function networkScope(details: LogDetails): "lan" | "loopback" | "local-client" | "process" {
  if (details.channel === "client") return "local-client";
  if (traceKind(details) !== "udp-datagram" && details.channel !== "http") return "process";
  const remote = details.remote ?? "";
  return remote.includes("127.0.0.1") || remote.includes("::1") || details.event.includes("loopback")
    ? "loopback"
    : "lan";
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
