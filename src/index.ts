import * as Automerge from "@automerge/automerge";
import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import WebSocket, { WebSocketServer } from "ws";
import { readConfig } from "./config.js";
import { createAnnouncement, Discovery } from "./discovery.js";
import { loadIdentity } from "./identity.js";
import { Mesh } from "./mesh.js";
import { DocumentStore } from "./store.js";
import type { ClientCommand, FieldDocument, Point } from "./types.js";
import { WireLogger } from "./wire-log.js";

const config = readConfig();
const peerId = await loadIdentity(config.dataDir);
const wireLog = new WireLogger(config.wireLog, peerId);
const store = new DocumentStore(config.dataDir, config.room, peerId);
let document = await store.load();
const browserClients = new Set<WebSocket>();
let discovery: Discovery | undefined;

const publicDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../public");
const contentTypes: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
};

const server = createServer(async (request, response) => {
  wireLog.log({ channel: "http", direction: "rx", event: "request", remote: request.socket.remoteAddress, payload: { method: request.method, url: request.url } });
  if (request.url === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: true, peerId, room: config.room, peers: mesh.peers().length }));
    return;
  }
  const requested = request.url === "/" ? "/index.html" : request.url?.split("?", 1)[0] ?? "/index.html";
  const filePath = path.resolve(publicDir, `.${requested}`);
  if (!filePath.startsWith(`${publicDir}${path.sep}`)) {
    response.writeHead(403).end("Forbidden");
    return;
  }
  try {
    const info = await stat(filePath);
    if (!info.isFile()) throw new Error("not a file");
    response.writeHead(200, {
      "content-type": contentTypes[path.extname(filePath)] ?? "application/octet-stream",
      "cache-control": "no-store",
    });
    createReadStream(filePath).pipe(response);
  } catch {
    response.writeHead(404).end("Not found");
  }
});

const browserServer = new WebSocketServer({ noServer: true, maxPayload: 2 * 1024 * 1024 });

const mesh = new Mesh({
  peerId,
  name: config.name,
  room: config.room,
  host: config.host,
  port: config.port,
  getDocument: () => document,
  setDocument: (next) => {
    document = next;
    changed();
  },
  onStatus: broadcast,
  onPeerUnavailable: () => {
    discovery?.search();
    connectConfiguredPeers();
  },
  wireLog,
});

await mesh.start();
browserServer.on("connection", (socket) => {
  browserClients.add(socket);
  sendState(socket);
  socket.on("message", (payload, isBinary) => {
    if (isBinary) return;
    try {
      const command = JSON.parse(payload.toString()) as ClientCommand;
      wireLog.log({ channel: "client", direction: "rx", event: "command", bytes: Buffer.byteLength(payload.toString()), payload: command, raw: payload.toString() });
      applyCommand(command);
    } catch (error) {
      socket.send(JSON.stringify({ type: "error", message: error instanceof Error ? error.message : "Invalid command" }));
    }
  });
  socket.on("close", () => browserClients.delete(socket));
});

server.on("upgrade", (request, socket, head) => {
  const pathname = new URL(request.url ?? "/", "http://localhost").pathname;
  if (pathname !== "/client") return socket.destroy();
  browserServer.handleUpgrade(request, socket, head, (ws) => browserServer.emit("connection", ws, request));
});

await new Promise<void>((resolve, reject) => {
  server.once("error", reject);
  server.listen(config.port, config.host, () => resolve());
});

if (!config.discoveryDisabled) {
  discovery = new Discovery(
    createAnnouncement(peerId, config.name, config.room, config.port),
    config.discoveryPort,
    config.multicastAddress,
    (peer, address) => mesh.discovered(peer.peerId, peer.name, address, peer.port),
    wireLog,
  );
  try {
    await discovery.start();
  } catch (error) {
    console.warn("Multicast discovery unavailable; use --peer host:port to connect manually.", error);
    discovery = undefined;
  }
}

function connectConfiguredPeers(): void {
  for (const peer of config.peers) {
    const [host, rawPort] = peer.split(":");
    if (host && rawPort && Number.isInteger(Number(rawPort)) && !mesh.isConnectedTo(host, Number(rawPort))) {
      mesh.connect(host, Number(rawPort));
    }
  }
}
connectConfiguredPeers();
const manualPeerTimer = config.peers.length ? setInterval(connectConfiguredPeers, 5_000) : undefined;

console.log(`\nFieldMesh node “${config.name}”`);
console.log(`  UI:        http://localhost:${config.port}`);
console.log(`  Room:      ${config.room}`);
console.log(`  Peer ID:   ${peerId}`);
console.log(`  Discovery: ${discovery ? `${config.multicastAddress}:${config.discoveryPort}` : "disabled"}`);
console.log(`  Data:      ${store.filePath}\n`);
console.log(`  Wire log:  ${config.wireLog}${config.wireLog === "off" ? " (enable with --wire-log)" : ""}\n`);

function changed(): void {
  store.scheduleSave(() => document);
  broadcast();
}

function broadcast(): void {
  for (const socket of browserClients) sendState(socket);
}

function sendState(socket: WebSocket): void {
  if (socket.readyState !== WebSocket.OPEN) return;
  const state = JSON.stringify({
    type: "state",
    self: { peerId, name: config.name, room: config.room },
    peers: mesh.peers(),
    document: Automerge.toJS(document),
  });
  wireLog.log({ channel: "client", direction: "tx", event: "state", bytes: Buffer.byteLength(state), payload: JSON.parse(state), raw: state });
  socket.send(state);
}

function applyCommand(command: ClientCommand): void {
  const now = Date.now();
  switch (command.type) {
    case "chat.add": {
      const text = requireText(command.text, "message", 2_000);
      document = Automerge.change(document, { message: "Add chat message" }, (draft) => {
        draft.messages.push({ id: randomUUID(), author: config.name, text, createdAt: now });
      });
      break;
    }
    case "track.upsert": {
      const callsign = requireText(command.callsign, "callsign", 80);
      const latitude = requireCoordinate(command.latitude, -90, 90, "latitude");
      const longitude = requireCoordinate(command.longitude, -180, 180, "longitude");
      if (!["friendly", "unknown", "hostile"].includes(command.affiliation)) throw new Error("Invalid affiliation");
      const id = command.id && /^[a-zA-Z0-9_-]{1,80}$/.test(command.id) ? command.id : randomUUID();
      document = Automerge.change(document, { message: "Update force track" }, (draft) => {
        draft.tracks[id] = {
          id, callsign, affiliation: command.affiliation, latitude, longitude, updatedAt: now, updatedBy: config.name,
        };
      });
      break;
    }
    case "drawing.add": {
      if (!/^#[0-9a-fA-F]{6}$/.test(command.color)) throw new Error("Invalid drawing color");
      if (!Array.isArray(command.points) || command.points.length < 2 || command.points.length > 5_000) {
        throw new Error("A drawing must contain 2–5000 points");
      }
      const points = command.points.map(validatePoint);
      document = Automerge.change(document, { message: "Add map drawing" }, (draft) => {
        draft.drawings.push({ id: randomUUID(), author: config.name, color: command.color, points, createdAt: now });
      });
      break;
    }
    case "drawing.clear":
      document = Automerge.change(document, { message: "Clear map drawings" }, (draft) => {
        draft.drawings.splice(0, draft.drawings.length);
      });
      break;
    default:
      throw new Error("Unknown command");
  }
  changed();
  mesh.documentChanged();
}

function requireText(value: unknown, label: string, maxLength: number): string {
  if (typeof value !== "string" || !value.trim() || value.length > maxLength) throw new Error(`Invalid ${label}`);
  return value.trim();
}

function requireCoordinate(value: unknown, min: number, max: number, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < min || value > max) throw new Error(`Invalid ${label}`);
  return value;
}

function validatePoint(point: Point): Point {
  if (!point || !Number.isFinite(point.x) || !Number.isFinite(point.y) || point.x < 0 || point.x > 1 || point.y < 0 || point.y > 1) {
    throw new Error("Invalid drawing point");
  }
  return { x: point.x, y: point.y };
}

async function shutdown(): Promise<void> {
  clearInterval(manualPeerTimer);
  discovery?.close();
  mesh.close();
  await store.save(document);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 2_000).unref();
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
