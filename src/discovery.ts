import { randomBytes } from "node:crypto";
import dgram, { type RemoteInfo } from "node:dgram";
import { mkdirSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { networkInterfaces, tmpdir } from "node:os";
import { join } from "node:path";
import type { WireLogger } from "./wire-log.js";

const PROTOCOL = "fieldmesh-discovery-v2";
const MULTICAST_BURST_MS = [0];
const BROADCAST_FALLBACK_AFTER_MS = 1_750;
const BROADCAST_BURST_MS = [0];
const RESPONSE_JITTER_MAX_MS = 750;

export interface PeerAnnouncement {
  peerId: string;
  name: string;
  room: string;
  port: number;
}

interface DiscoveryMessage extends PeerAnnouncement {
  protocol: typeof PROTOCOL;
  type: "discover" | "offer";
  requestId: string;
  replyTo?: string;
}

interface LocalRegistryRecord {
  pid: number;
  peerId: string;
  room: string;
  queryPort: number;
}

export class Discovery {
  private readonly listenerSocket = dgram.createSocket({ type: "udp4", reuseAddr: true });
  private readonly querySocket = dgram.createSocket("udp4");
  private readonly timers = new Set<NodeJS.Timeout>();
  private readonly activeRequests = new Map<string, number>();
  private readonly scheduledResponses = new Set<string>();
  private searchInProgress = false;
  private closed = false;
  private localRegistryPath?: string;

  constructor(
    private readonly announcement: PeerAnnouncement,
    private readonly discoveryPort: number,
    private readonly multicastAddress: string,
    private readonly onPeer: (peer: PeerAnnouncement, address: string) => void,
    private readonly wireLog: WireLogger,
  ) {}

  async start(): Promise<void> {
    this.listenerSocket.on("message", (data, remote) => this.receive(data, remote));
    this.querySocket.on("message", (data, remote) => this.receive(data, remote));
    this.listenerSocket.on("error", (error) => console.error("Discovery listener error", error.message));
    this.querySocket.on("error", (error) => console.error("Discovery query socket error", error.message));
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      this.listenerSocket.once("error", onError);
      this.listenerSocket.bind(this.discoveryPort, "0.0.0.0", () => {
        this.listenerSocket.off("error", onError);
        try {
          // Passing no interface lets the OS choose one membership. On hosts
          // with VPN/Hyper-V adapters that can differ from the interface used
          // for outbound multicast, so join on every active IPv4 interface.
          for (const address of multicastInterfaces()) {
            this.listenerSocket.addMembership(this.multicastAddress, address);
          }
          this.listenerSocket.setMulticastLoopback(true);
          resolve();
        } catch (error) {
          reject(error);
        }
      });
    });
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      this.querySocket.once("error", onError);
      this.querySocket.bind(0, "0.0.0.0", () => {
        this.querySocket.off("error", onError);
        try {
          this.querySocket.setMulticastTTL(1);
          this.querySocket.setMulticastLoopback(true);
          this.querySocket.setBroadcast(true);
          resolve();
        } catch (error) {
          reject(error);
        }
      });
    });
    this.registerLocalProcess();
    this.search();
  }

  search(): void {
    if (this.closed || this.searchInProgress) return;
    this.searchInProgress = true;
    const multicastRequestId = requestId();
    this.activeRequests.set(multicastRequestId, 0);
    for (const delay of MULTICAST_BURST_MS) {
      this.later(() => this.sendDiscover(multicastRequestId, this.multicastAddress, "multicast-discover"), delay);
    }
    const localRequestId = requestId();
    this.activeRequests.set(localRequestId, 0);
    this.sendLocalDiscovers(localRequestId);
    this.later(() => {
      if ((this.activeRequests.get(multicastRequestId) ?? 0) === 0) {
        const broadcastRequestId = requestId();
        this.activeRequests.set(broadcastRequestId, 0);
        for (const delay of BROADCAST_BURST_MS) {
          this.later(() => this.sendDiscover(broadcastRequestId, "255.255.255.255", "broadcast-discover"), delay);
        }
        this.later(() => this.activeRequests.delete(broadcastRequestId), 5_000);
      }
      this.later(() => {
        this.activeRequests.delete(multicastRequestId);
        this.activeRequests.delete(localRequestId);
        this.searchInProgress = false;
      }, 1_000);
    }, BROADCAST_FALLBACK_AFTER_MS);
  }

  close(): void {
    this.closed = true;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    this.listenerSocket.close();
    this.querySocket.close();
    this.unregisterLocalProcess();
  }

  private receive(data: Buffer, remote: RemoteInfo): void {
    let message: Partial<DiscoveryMessage>;
    try {
      message = JSON.parse(data.toString("utf8")) as Partial<DiscoveryMessage>;
    } catch {
      return;
    }
    if (!validMessage(message) || message.room !== this.announcement.room || message.peerId === this.announcement.peerId) return;

    const endpoint = `${remote.address}:${remote.port}`;
    this.wireLog.log({
      channel: "discovery", direction: "rx", event: "udp.datagram", remote: endpoint,
      bytes: data.byteLength, payload: message, raw: data,
    });

    if (message.type === "offer") {
      if (!message.replyTo || !this.activeRequests.has(message.replyTo)) return;
      this.activeRequests.set(message.replyTo, (this.activeRequests.get(message.replyTo) ?? 0) + 1);
      this.wireLog.log({
        channel: "discovery", direction: "event", event: "offer.accepted", remote: endpoint,
        payload: { replyTo: message.replyTo, peerId: message.peerId }, outcome: "peer-candidate",
      });
      this.onPeer(message, remote.address);
      return;
    }

    const responseKey = `${message.peerId}:${message.requestId}`;
    if (this.scheduledResponses.has(responseKey)) return;
    this.scheduledResponses.add(responseKey);
    const delay = Math.floor(Math.random() * (RESPONSE_JITTER_MAX_MS + 1));
    this.wireLog.log({
      channel: "discovery", direction: "event", event: "offer.scheduled", remote: endpoint,
      payload: { replyTo: message.requestId, delayMs: delay },
    });
    this.later(() => {
      this.sendOffer(message.requestId, remote.address, remote.port);
      this.later(() => this.scheduledResponses.delete(responseKey), 5_000);
    }, delay);
  }

  private sendDiscover(id: string, address: string, event: string): void {
    const message: DiscoveryMessage = { protocol: PROTOCOL, type: "discover", requestId: id, ...this.announcement };
    this.send(this.querySocket, message, address, this.discoveryPort, event);
  }

  private sendLocalDiscovers(id: string): void {
    for (const peer of localPeers(this.announcement.room, this.announcement.peerId)) {
      const message: DiscoveryMessage = {
        protocol: PROTOCOL, type: "discover", requestId: id, ...this.announcement,
      };
      this.send(this.querySocket, message, "127.0.0.1", peer.queryPort, "loopback-discover");
    }
  }

  private registerLocalProcess(): void {
    const address = this.querySocket.address();
    if (typeof address === "string") return;
    const directory = localRegistryDirectory();
    mkdirSync(directory, { recursive: true });
    this.localRegistryPath = join(directory, `${this.announcement.peerId}.json`);
    const record: LocalRegistryRecord = {
      pid: process.pid,
      peerId: this.announcement.peerId,
      room: this.announcement.room,
      queryPort: address.port,
    };
    writeFileSync(this.localRegistryPath, JSON.stringify(record), { encoding: "utf8", mode: 0o600 });
  }

  private unregisterLocalProcess(): void {
    if (!this.localRegistryPath) return;
    try {
      const record = JSON.parse(readFileSync(this.localRegistryPath, "utf8")) as Partial<LocalRegistryRecord>;
      if (record.pid === process.pid) unlinkSync(this.localRegistryPath);
    } catch {
      // Already removed or replaced by a newer process using the same peer ID.
    }
  }

  private sendOffer(replyTo: string, address: string, port: number): void {
    const message: DiscoveryMessage = {
      protocol: PROTOCOL, type: "offer", requestId: requestId(), replyTo, ...this.announcement,
    };
    this.send(this.listenerSocket, message, address, port, "unicast-offer");
  }

  private send(socket: dgram.Socket, message: DiscoveryMessage, address: string, port: number, event: string): void {
    if (this.closed) return;
    const payload = Buffer.from(JSON.stringify(message));
    const remote = `${address}:${port}`;
    this.wireLog.log({
      channel: "discovery", direction: "tx", event: `udp.${event}`, remote,
      bytes: payload.byteLength, payload: message, raw: payload,
    });
    socket.send(payload, port, address, (error) => {
      this.wireLog.log({
        channel: "discovery", direction: "event", event: "socket.send", remote,
        bytes: payload.byteLength, outcome: error ? `error: ${error.message}` : "accepted-by-os",
      });
    });
  }

  private later(callback: () => void, delay: number): void {
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      if (!this.closed) callback();
    }, delay);
    this.timers.add(timer);
  }
}

function validMessage(message: Partial<DiscoveryMessage>): message is DiscoveryMessage {
  return message.protocol === PROTOCOL && (message.type === "discover" || message.type === "offer") &&
    typeof message.requestId === "string" && message.requestId.length <= 64 &&
    typeof message.peerId === "string" && typeof message.name === "string" &&
    typeof message.room === "string" && typeof message.port === "number" &&
    Number.isInteger(message.port) && message.port > 0 && message.port <= 65_535;
}

function requestId(): string {
  return randomBytes(8).toString("hex");
}

function multicastInterfaces(): string[] {
  const addresses = Object.values(networkInterfaces())
    .flatMap((entries) => entries ?? [])
    .filter((entry) => entry.family === "IPv4" && !entry.internal)
    .map((entry) => entry.address);
  return addresses.length > 0 ? [...new Set(addresses)] : ["0.0.0.0"];
}

function localRegistryDirectory(): string {
  return join(tmpdir(), "fieldmesh-discovery-v2");
}

function localPeers(room: string, localPeerId: string): LocalRegistryRecord[] {
  const directory = localRegistryDirectory();
  try {
    return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
      if (!entry.isFile() || !entry.name.endsWith(".json")) return [];
      const path = join(directory, entry.name);
      try {
        const record = JSON.parse(readFileSync(path, "utf8")) as Partial<LocalRegistryRecord>;
        if (!validLocalRecord(record)) return [];
        if (!processExists(record.pid)) {
          unlinkSync(path);
          return [];
        }
        return record.room === room && record.peerId !== localPeerId ? [record] : [];
      } catch {
        return [];
      }
    });
  } catch {
    return [];
  }
}

function validLocalRecord(record: Partial<LocalRegistryRecord>): record is LocalRegistryRecord {
  return Number.isInteger(record.pid) && (record.pid ?? 0) > 0 &&
    typeof record.peerId === "string" && typeof record.room === "string" &&
    Number.isInteger(record.queryPort) && (record.queryPort ?? 0) > 0 && (record.queryPort ?? 0) <= 65_535;
}

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

export function createAnnouncement(peerId: string, name: string, room: string, port: number): PeerAnnouncement {
  return { peerId, name, room, port };
}
