import * as Automerge from "@automerge/automerge";
import WebSocket, { type RawData } from "ws";
import type { FieldDocument } from "./types.js";
import type { WireLogger } from "./wire-log.js";

interface Hello {
  type: "hello";
  protocol: "fieldmesh-sync-v1";
  peerId: string;
  name: string;
  room: string;
  port: number;
}

interface PeerConnection {
  socket: WebSocket;
  peerId?: string;
  name?: string;
  syncState: Automerge.SyncState;
  outbound: boolean;
  endpoint?: string;
}

export interface PeerSummary {
  peerId: string;
  name: string;
  endpoint?: string;
  ourChangesAcknowledged: boolean;
}

export interface MeshOptions {
  peerId: string;
  name: string;
  room: string;
  port: number;
  getDocument: () => Automerge.Doc<FieldDocument>;
  setDocument: (document: Automerge.Doc<FieldDocument>) => void;
  onStatus: () => void;
  wireLog: WireLogger;
}

export class Mesh {
  private readonly connections = new Set<PeerConnection>();
  private readonly dialing = new Set<string>();

  constructor(private readonly options: MeshOptions) {}

  accept(socket: WebSocket): void {
    const connection: PeerConnection = {
      socket,
      syncState: Automerge.initSyncState(),
      outbound: false,
    };
    this.attach(connection);
  }

  connect(host: string, port: number): void {
    const endpoint = `${host}:${port}`;
    if (this.dialing.has(endpoint) || this.hasEndpoint(endpoint)) return;
    this.dialing.add(endpoint);
    const socket = new WebSocket(`ws://${endpoint}/peer`, { handshakeTimeout: 3_000 });
    const connection: PeerConnection = {
      socket,
      syncState: Automerge.initSyncState(),
      outbound: true,
      endpoint,
    };
    socket.once("open", () => this.dialing.delete(endpoint));
    socket.once("close", () => this.dialing.delete(endpoint));
    socket.once("error", () => this.dialing.delete(endpoint));
    this.attach(connection);
  }

  discovered(peerId: string, host: string, port: number): void {
    if (peerId === this.options.peerId || this.hasPeer(peerId)) return;
    // One deterministic dialer prevents a connection storm when every node hears every beacon.
    if (this.options.peerId < peerId) this.connect(host, port);
  }

  documentChanged(): void {
    for (const connection of this.connections) this.pump(connection);
  }

  peers(): PeerSummary[] {
    return [...this.connections]
      .filter((connection): connection is PeerConnection & { peerId: string; name: string } => Boolean(connection.peerId && connection.name))
      .map(({ peerId, name, endpoint, syncState }) => ({
        peerId, name, endpoint,
        ourChangesAcknowledged: Automerge.hasOurChanges(this.options.getDocument(), syncState),
      }));
  }

  close(): void {
    for (const connection of this.connections) connection.socket.close();
  }

  private attach(connection: PeerConnection): void {
    this.connections.add(connection);
    connection.socket.on("open", () => this.sendHello(connection));
    connection.socket.on("message", (data, isBinary) => this.onMessage(connection, data, isBinary));
    connection.socket.on("close", () => this.remove(connection));
    connection.socket.on("error", (error) => {
      if (connection.peerId) console.warn(`Peer ${connection.name ?? connection.peerId} disconnected: ${error.message}`);
    });
    if (connection.socket.readyState === WebSocket.OPEN) this.sendHello(connection);
  }

  private sendHello(connection: PeerConnection): void {
    const hello: Hello = {
      type: "hello",
      protocol: "fieldmesh-sync-v1",
      peerId: this.options.peerId,
      name: this.options.name,
      room: this.options.room,
      port: this.options.port,
    };
    const payload = JSON.stringify(hello);
    this.options.wireLog.log({ channel: "peer", direction: "tx", event: "handshake.hello", remote: connection.endpoint, bytes: Buffer.byteLength(payload), payload, raw: payload });
    connection.socket.send(payload, (error) => this.logSendOutcome(connection, "handshake.hello", Buffer.byteLength(payload), error));
  }

  private onMessage(connection: PeerConnection, data: RawData, isBinary: boolean): void {
    if (!connection.peerId) {
      if (isBinary) return connection.socket.close(1002, "hello required");
      try {
        const hello = JSON.parse(data.toString()) as Partial<Hello>;
        this.options.wireLog.log({ channel: "peer", direction: "rx", event: "handshake.hello", remote: connection.endpoint, bytes: data.toString().length, payload: hello, raw: data.toString() });
        if (
          hello.type !== "hello" ||
          hello.protocol !== "fieldmesh-sync-v1" ||
          hello.room !== this.options.room ||
          typeof hello.peerId !== "string" ||
          typeof hello.name !== "string"
        ) {
          return connection.socket.close(1008, "incompatible peer");
        }
        if (hello.peerId === this.options.peerId) return connection.socket.close(1008, "self connection");
        const duplicate = [...this.connections].find((item) => item !== connection && item.peerId === hello.peerId);
        if (duplicate) {
          const preferOutbound = this.options.peerId < hello.peerId;
          if (connection.outbound !== preferOutbound) return connection.socket.close(1000, "duplicate");
          duplicate.socket.close(1000, "duplicate");
        }
        connection.peerId = hello.peerId;
        connection.name = hello.name;
        console.log(`Connected to ${hello.name} (${hello.peerId.slice(0, 8)})`);
        this.options.onStatus();
        this.pump(connection);
      } catch {
        connection.socket.close(1002, "invalid hello");
      }
      return;
    }

    if (!isBinary) return;
    try {
      this.options.wireLog.sync("rx", connection.peerId, new Uint8Array(data as Buffer));
      const before = Automerge.getHeads(this.options.getDocument()).join(",");
      const [document, syncState] = Automerge.receiveSyncMessage(
        this.options.getDocument(),
        connection.syncState,
        new Uint8Array(data as Buffer),
      );
      connection.syncState = syncState;
      this.options.setDocument(document);
      this.pump(connection);
      this.options.onStatus();
      if (Automerge.getHeads(document).join(",") !== before) {
        for (const other of this.connections) {
          if (other !== connection) this.pump(other);
        }
      }
    } catch (error) {
      console.warn(`Rejected sync message from ${connection.name}:`, error);
      connection.socket.close(1007, "invalid sync message");
    }
  }

  private pump(connection: PeerConnection): void {
    if (!connection.peerId || connection.socket.readyState !== WebSocket.OPEN) return;
    const [syncState, message] = Automerge.generateSyncMessage(this.options.getDocument(), connection.syncState);
    connection.syncState = syncState;
    if (message) {
      this.options.wireLog.sync("tx", connection.peerId, message);
      connection.socket.send(message, (error) => this.logSendOutcome(connection, "automerge.sync", message.byteLength, error));
    }
    this.options.onStatus();
  }

  private hasPeer(peerId: string): boolean {
    return [...this.connections].some((connection) => connection.peerId === peerId);
  }

  private hasEndpoint(endpoint: string): boolean {
    return [...this.connections].some(
      (connection) => connection.endpoint === endpoint && connection.socket.readyState !== WebSocket.CLOSED,
    );
  }

  private remove(connection: PeerConnection): void {
    if (!this.connections.delete(connection)) return;
    if (connection.peerId) console.log(`Disconnected from ${connection.name ?? connection.peerId}`);
    this.options.onStatus();
  }

  private logSendOutcome(connection: PeerConnection, event: string, bytes: number, error?: Error): void {
    this.options.wireLog.log({
      channel: "peer", direction: "event", event: `${event}.socket-send`, remote: connection.peerId ?? connection.endpoint,
      bytes, outcome: error ? `error: ${error.message}` : "accepted-by-os",
    });
  }
}
