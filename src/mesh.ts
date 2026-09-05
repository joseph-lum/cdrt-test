import * as Automerge from "@automerge/automerge";
import { randomBytes } from "node:crypto";
import dgram, { type RemoteInfo } from "node:dgram";
import type { FieldDocument } from "./types.js";
import type { WireLogger } from "./wire-log.js";

const MAGIC = Buffer.from("FMU1");
const HEADER_BYTES = 28;
const MAX_FRAGMENT_PAYLOAD = 1_000;
const RETRANSMIT_AFTER_MS = 750;
const MAX_RETRIES = 10;
const MAX_FRAGMENTS = 8_192;

enum PacketType { Hello = 1, HelloAck = 2, Data = 3, Ack = 4 }

interface Hello { protocol: "fieldmesh-udp-v1"; peerId: string; name: string; room: string; port: number }
interface Endpoint { address: string; port: number }
interface PendingMessage { sequence: number; packets: Buffer[]; lastSentAt: number; attempts: number }
interface Assembly { fragments: Array<Buffer | undefined>; received: number }
interface PeerState {
  peerId: string;
  name: string;
  endpoint: Endpoint;
  ready: boolean;
  remoteSession?: number;
  localSession: number;
  nextSequence: number;
  expectedSequence: number;
  syncState: Automerge.SyncState;
  pending: Map<number, PendingMessage>;
  assemblies: Map<number, Assembly>;
  complete: Map<number, Buffer>;
  lastSeenAt: number;
}
interface ParsedPacket {
  type: PacketType;
  senderSession: number;
  sequence: number;
  ackSession: number;
  ackSequence: number;
  fragmentIndex: number;
  fragmentCount: number;
  payload: Buffer;
}

export interface PeerSummary {
  peerId: string;
  name: string;
  endpoint: string;
  transport: "udp";
  ourChangesAcknowledged: boolean;
  pendingDatagrams: number;
}

export interface MeshOptions {
  peerId: string;
  name: string;
  room: string;
  host: string;
  port: number;
  getDocument: () => Automerge.Doc<FieldDocument>;
  setDocument: (document: Automerge.Doc<FieldDocument>) => void;
  onStatus: () => void;
  onPeerUnavailable?: () => void;
  wireLog: WireLogger;
}

export class Mesh {
  private readonly socket = dgram.createSocket("udp4");
  private readonly sessionId = randomBytes(4).readUInt32BE();
  private readonly peersById = new Map<string, PeerState>();
  private readonly peersByEndpoint = new Map<string, PeerState>();
  private maintenanceTimer: NodeJS.Timeout | undefined;
  private started = false;

  constructor(private readonly options: MeshOptions) {}

  async start(): Promise<void> {
    this.socket.on("message", (packet, remote) => this.receivePacket(packet, remote));
    this.socket.on("error", (error) => console.error("Peer UDP socket error", error.message));
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      this.socket.once("error", onError);
      this.socket.bind(this.options.port, this.options.host, () => {
        this.socket.off("error", onError);
        this.started = true;
        resolve();
      });
    });
    this.maintenanceTimer = setInterval(() => this.maintain(), 250);
  }

  connect(host: string, port: number): void {
    const endpoint = { address: host, port };
    if (this.peersByEndpoint.get(endpointKey(endpoint))?.ready) return;
    this.sendHello(endpoint, PacketType.Hello);
  }

  isConnectedTo(host: string, port: number): boolean {
    return this.peersByEndpoint.get(endpointKey({ address: host, port }))?.ready === true;
  }

  discovered(peerId: string, name: string, host: string, port: number): void {
    if (peerId === this.options.peerId) return;
    const existing = this.peersById.get(peerId);
    if (existing) {
      existing.lastSeenAt = Date.now();
      if (endpointKey(existing.endpoint) !== endpointKey({ address: host, port })) {
        this.peersByEndpoint.delete(endpointKey(existing.endpoint));
        existing.endpoint = { address: host, port };
        existing.ready = false;
        this.peersByEndpoint.set(endpointKey(existing.endpoint), existing);
      }
      if (!existing.ready) this.sendHello(existing.endpoint, PacketType.Hello);
      return;
    }
    const peer = this.registerPeer(peerId, name, { address: host, port }, false);
    this.sendHello(peer.endpoint, PacketType.Hello);
  }

  documentChanged(): void {
    for (const peer of this.peersById.values()) this.pump(peer);
    this.options.onStatus();
  }

  peers(): PeerSummary[] {
    return [...this.peersById.values()].filter((peer) => peer.ready).map((peer) => ({
      peerId: peer.peerId,
      name: peer.name,
      endpoint: endpointKey(peer.endpoint),
      transport: "udp",
      ourChangesAcknowledged: Automerge.hasOurChanges(this.options.getDocument(), peer.syncState),
      pendingDatagrams: [...peer.pending.values()].reduce((total, item) => total + item.packets.length, 0),
    }));
  }

  close(): void {
    clearInterval(this.maintenanceTimer);
    if (this.started) this.socket.close();
    this.started = false;
  }

  private receivePacket(packet: Buffer, remote: RemoteInfo): void {
    const parsed = parsePacket(packet);
    if (!parsed) return;
    const endpoint = { address: remote.address, port: remote.port };
    this.options.wireLog.log({
      channel: "peer", direction: "rx", event: `udp.${packetTypeName(parsed.type)}`,
      remote: endpointKey(endpoint), bytes: packet.byteLength, payload: packetMetadata(parsed), raw: packet,
    });
    if (parsed.type === PacketType.Hello || parsed.type === PacketType.HelloAck) {
      this.receiveHello(parsed, endpoint);
      return;
    }
    const peer = this.peersByEndpoint.get(endpointKey(endpoint));
    if (!peer) return;
    peer.lastSeenAt = Date.now();
    if (parsed.type === PacketType.Ack) {
      if (parsed.ackSession === peer.localSession && peer.pending.delete(parsed.ackSequence)) {
        this.options.wireLog.log({
          channel: "peer", direction: "event", event: "udp.message-acknowledged", remote: peer.peerId,
          payload: { sequence: parsed.ackSequence }, outcome: "received-by-peer",
        });
        this.options.onStatus();
      }
      return;
    }
    if (parsed.type !== PacketType.Data) return;
    if (peer.remoteSession !== parsed.senderSession) {
      if (peer.remoteSession !== undefined) this.resetOutgoingSession(peer);
      this.resetIncomingSession(peer, parsed.senderSession);
    }
    if (parsed.sequence < peer.expectedSequence) {
      this.sendAck(peer, parsed.senderSession, parsed.sequence);
      return;
    }
    if (parsed.fragmentCount < 1 || parsed.fragmentIndex >= parsed.fragmentCount || parsed.fragmentCount > MAX_FRAGMENTS) return;
    let assembly = peer.assemblies.get(parsed.sequence);
    if (!assembly) {
      assembly = { fragments: new Array(parsed.fragmentCount), received: 0 };
      peer.assemblies.set(parsed.sequence, assembly);
    }
    if (assembly.fragments.length !== parsed.fragmentCount) return;
    if (!assembly.fragments[parsed.fragmentIndex]) {
      assembly.fragments[parsed.fragmentIndex] = parsed.payload;
      assembly.received += 1;
    }
    if (assembly.received === parsed.fragmentCount) {
      peer.assemblies.delete(parsed.sequence);
      peer.complete.set(parsed.sequence, Buffer.concat(assembly.fragments as Buffer[]));
      this.sendAck(peer, parsed.senderSession, parsed.sequence);
      this.drain(peer);
    }
  }

  private receiveHello(packet: ParsedPacket, endpoint: Endpoint): void {
    try {
      const hello = JSON.parse(packet.payload.toString("utf8")) as Partial<Hello>;
      if (hello.protocol !== "fieldmesh-udp-v1" || hello.room !== this.options.room ||
          typeof hello.peerId !== "string" || typeof hello.name !== "string" || hello.peerId === this.options.peerId) return;
      let peer = this.peersById.get(hello.peerId);
      const isNewPeer = !peer;
      if (!peer) peer = this.registerPeer(hello.peerId, hello.name, endpoint, true);
      if (endpointKey(peer.endpoint) !== endpointKey(endpoint)) {
        this.peersByEndpoint.delete(endpointKey(peer.endpoint));
        peer.endpoint = endpoint;
        this.peersByEndpoint.set(endpointKey(endpoint), peer);
      }
      const becameReady = isNewPeer || !peer.ready;
      peer.ready = true;
      peer.name = hello.name;
      peer.lastSeenAt = Date.now();
      if (peer.remoteSession !== packet.senderSession) {
        if (peer.remoteSession !== undefined) this.resetOutgoingSession(peer);
        this.resetIncomingSession(peer, packet.senderSession);
      }
      if (packet.type === PacketType.Hello) this.sendHello(endpoint, PacketType.HelloAck);
      if (becameReady) console.log(`Connected to ${peer.name} (${peer.peerId.slice(0, 8)}) over UDP`);
      this.options.onStatus();
      this.pump(peer);
    } catch {
      // Ignore malformed or unrelated UDP payloads.
    }
  }

  private registerPeer(peerId: string, name: string, endpoint: Endpoint, ready: boolean): PeerState {
    const peer: PeerState = {
      peerId, name, endpoint, ready, localSession: this.sessionId, nextSequence: 1, expectedSequence: 1,
      syncState: Automerge.initSyncState(), pending: new Map(), assemblies: new Map(), complete: new Map(),
      lastSeenAt: Date.now(),
    };
    this.peersById.set(peerId, peer);
    this.peersByEndpoint.set(endpointKey(endpoint), peer);
    return peer;
  }

  private sendHello(endpoint: Endpoint, type: PacketType.Hello | PacketType.HelloAck): void {
    const hello: Hello = {
      protocol: "fieldmesh-udp-v1", peerId: this.options.peerId, name: this.options.name,
      room: this.options.room, port: this.options.port,
    };
    const peer = this.peersByEndpoint.get(endpointKey(endpoint));
    this.sendPacket(endpoint, encodePacket({ type, senderSession: peer?.localSession ?? this.sessionId, payload: Buffer.from(JSON.stringify(hello)) }));
  }

  private pump(peer: PeerState): void {
    if (!peer.ready) return;
    const [syncState, message] = Automerge.generateSyncMessage(this.options.getDocument(), peer.syncState);
    peer.syncState = syncState;
    if (!message) return;
    this.options.wireLog.sync("tx", peer.peerId, message);
    this.sendReliable(peer, Buffer.from(message));
  }

  private sendReliable(peer: PeerState, payload: Buffer): void {
    const sequence = peer.nextSequence++;
    const fragmentCount = Math.ceil(payload.byteLength / MAX_FRAGMENT_PAYLOAD);
    if (fragmentCount > MAX_FRAGMENTS) throw new Error("Automerge sync message exceeds UDP fragmentation limit");
    const packets: Buffer[] = [];
    for (let index = 0; index < fragmentCount; index += 1) {
      packets.push(encodePacket({
        type: PacketType.Data, senderSession: peer.localSession, sequence, fragmentIndex: index, fragmentCount,
        payload: payload.subarray(index * MAX_FRAGMENT_PAYLOAD, (index + 1) * MAX_FRAGMENT_PAYLOAD),
      }));
    }
    const pending = { sequence, packets, lastSentAt: Date.now(), attempts: 1 };
    peer.pending.set(sequence, pending);
    for (const packet of packets) this.sendPacket(peer.endpoint, packet);
  }

  private sendAck(peer: PeerState, remoteSession: number, sequence: number): void {
    this.sendPacket(peer.endpoint, encodePacket({
      type: PacketType.Ack, senderSession: peer.localSession, ackSession: remoteSession, ackSequence: sequence,
    }));
  }

  private drain(peer: PeerState): void {
    while (peer.complete.has(peer.expectedSequence)) {
      const message = peer.complete.get(peer.expectedSequence)!;
      peer.complete.delete(peer.expectedSequence++);
      try {
        this.options.wireLog.sync("rx", peer.peerId, message);
        const before = Automerge.getHeads(this.options.getDocument()).slice().sort().join(",");
        const [document, syncState] = Automerge.receiveSyncMessage(this.options.getDocument(), peer.syncState, message);
        peer.syncState = syncState;
        this.options.setDocument(document);
        this.pump(peer);
        this.options.onStatus();
        if (Automerge.getHeads(document).slice().sort().join(",") !== before) {
          for (const other of this.peersById.values()) if (other !== peer) this.pump(other);
        }
      } catch (error) {
        console.warn(`Rejected sync message from ${peer.name}:`, error);
        this.dropPeer(peer, "invalid-sync-message");
        return;
      }
    }
  }

  private sendPacket(endpoint: Endpoint, packet: Buffer): void {
    if (!this.started) return;
    const parsed = parsePacket(packet)!;
    const remote = endpointKey(endpoint);
    this.options.wireLog.log({
      channel: "peer", direction: "tx", event: `udp.${packetTypeName(parsed.type)}`,
      remote, bytes: packet.byteLength, payload: packetMetadata(parsed), raw: packet,
    });
    this.socket.send(packet, endpoint.port, endpoint.address, (error) => {
      this.options.wireLog.log({
        channel: "peer", direction: "event", event: "udp.socket-send", remote, bytes: packet.byteLength,
        outcome: error ? `error: ${error.message}` : "accepted-by-os",
      });
    });
  }

  private maintain(): void {
    const now = Date.now();
    for (const peer of [...this.peersById.values()]) {
      for (const pending of peer.pending.values()) {
        if (now - pending.lastSentAt < RETRANSMIT_AFTER_MS) continue;
        if (pending.attempts >= MAX_RETRIES) {
          this.dropPeer(peer, `delivery-timeout-sequence-${pending.sequence}`);
          break;
        }
        pending.attempts += 1;
        pending.lastSentAt = now;
        this.options.wireLog.log({
          channel: "peer", direction: "event", event: "udp.retransmit", remote: peer.peerId,
          payload: { sequence: pending.sequence, attempt: pending.attempts, fragments: pending.packets.length },
        });
        for (const packet of pending.packets) this.sendPacket(peer.endpoint, packet);
      }
    }
  }

  private resetIncomingSession(peer: PeerState, session: number): void {
    peer.remoteSession = session;
    peer.expectedSequence = 1;
    peer.assemblies.clear();
    peer.complete.clear();
    peer.syncState = Automerge.initSyncState();
  }

  private resetOutgoingSession(peer: PeerState): void {
    peer.localSession = randomBytes(4).readUInt32BE();
    peer.nextSequence = 1;
    peer.pending.clear();
  }

  private dropPeer(peer: PeerState, reason: string): void {
    if (!this.peersById.delete(peer.peerId)) return;
    this.peersByEndpoint.delete(endpointKey(peer.endpoint));
    console.log(`Disconnected from ${peer.name} (${reason})`);
    this.options.wireLog.log({ channel: "peer", direction: "event", event: "udp.peer-removed", remote: peer.peerId, outcome: reason });
    this.options.onStatus();
    this.options.onPeerUnavailable?.();
  }
}

function endpointKey(endpoint: Endpoint): string { return `${endpoint.address}:${endpoint.port}` }
function packetTypeName(type: PacketType): string {
  return ({ [PacketType.Hello]: "hello", [PacketType.HelloAck]: "hello-ack", [PacketType.Data]: "data", [PacketType.Ack]: "ack" })[type] ?? "unknown";
}
function packetMetadata(packet: ParsedPacket): Record<string, number> {
  return {
    senderSession: packet.senderSession, sequence: packet.sequence, ackSession: packet.ackSession,
    ackSequence: packet.ackSequence, fragmentIndex: packet.fragmentIndex,
    fragmentCount: packet.fragmentCount, payloadBytes: packet.payload.byteLength,
  };
}
function encodePacket(input: {
  type: PacketType; senderSession: number; sequence?: number; ackSession?: number; ackSequence?: number;
  fragmentIndex?: number; fragmentCount?: number; payload?: Buffer;
}): Buffer {
  const payload = input.payload ?? Buffer.alloc(0);
  const packet = Buffer.allocUnsafe(HEADER_BYTES + payload.byteLength);
  MAGIC.copy(packet, 0);
  packet.writeUInt8(1, 4);
  packet.writeUInt8(input.type, 5);
  packet.writeUInt16BE(input.fragmentIndex ?? 0, 6);
  packet.writeUInt32BE(input.senderSession, 8);
  packet.writeUInt32BE(input.sequence ?? 0, 12);
  packet.writeUInt32BE(input.ackSession ?? 0, 16);
  packet.writeUInt32BE(input.ackSequence ?? 0, 20);
  packet.writeUInt16BE(input.fragmentCount ?? 0, 24);
  packet.writeUInt16BE(payload.byteLength, 26);
  payload.copy(packet, HEADER_BYTES);
  return packet;
}
function parsePacket(packet: Buffer): ParsedPacket | undefined {
  if (packet.byteLength < HEADER_BYTES || !packet.subarray(0, 4).equals(MAGIC) || packet.readUInt8(4) !== 1) return;
  const type = packet.readUInt8(5);
  if (type < PacketType.Hello || type > PacketType.Ack) return;
  const payloadLength = packet.readUInt16BE(26);
  if (packet.byteLength !== HEADER_BYTES + payloadLength) return;
  return {
    type, senderSession: packet.readUInt32BE(8), sequence: packet.readUInt32BE(12),
    ackSession: packet.readUInt32BE(16), ackSequence: packet.readUInt32BE(20),
    fragmentIndex: packet.readUInt16BE(6), fragmentCount: packet.readUInt16BE(24),
    payload: packet.subarray(HEADER_BYTES),
  };
}
