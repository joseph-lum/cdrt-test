import dgram from "node:dgram";
import type { WireLogger } from "./wire-log.js";

const PROTOCOL = "fieldmesh-discovery-v1";

export interface PeerAnnouncement {
  protocol: typeof PROTOCOL;
  peerId: string;
  name: string;
  room: string;
  port: number;
}

export class Discovery {
  private readonly socket = dgram.createSocket({ type: "udp4", reuseAddr: true });
  private timer: NodeJS.Timeout | undefined;

  constructor(
    private readonly announcement: PeerAnnouncement,
    private readonly discoveryPort: number,
    private readonly multicastAddress: string,
    private readonly onPeer: (peer: PeerAnnouncement, address: string) => void,
    private readonly wireLog: WireLogger,
  ) {}

  async start(): Promise<void> {
    this.socket.on("message", (data, remote) => {
      this.wireLog.log({
        channel: "discovery", direction: "rx", event: "udp.datagram", remote: `${remote.address}:${remote.port}`,
        bytes: data.byteLength, raw: data,
      });
      try {
        const message = JSON.parse(data.toString("utf8")) as Partial<PeerAnnouncement>;
        this.wireLog.log({ channel: "discovery", direction: "event", event: "beacon.decoded", remote: remote.address, payload: message });
        if (
          message.protocol === PROTOCOL &&
          message.room === this.announcement.room &&
          message.peerId !== this.announcement.peerId &&
          typeof message.peerId === "string" &&
          typeof message.name === "string" &&
          typeof message.port === "number"
        ) {
          this.onPeer(message as PeerAnnouncement, remote.address);
        }
      } catch {
        // Ignore unrelated traffic on the multicast group.
      }
    });
    this.socket.on("error", (error) => console.error("Discovery socket error", error.message));

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      this.socket.once("error", onError);
      this.socket.bind(this.discoveryPort, "0.0.0.0", () => {
        this.socket.off("error", onError);
        try {
          this.socket.addMembership(this.multicastAddress);
          this.socket.setMulticastTTL(1);
          this.socket.setMulticastLoopback(true);
          this.socket.setBroadcast(true);
          resolve();
        } catch (error) {
          reject(error);
        }
      });
    });

    this.announce();
    this.timer = setInterval(() => this.announce(), 2_000);
  }

  private announce(): void {
    const payload = Buffer.from(JSON.stringify(this.announcement));
    this.sendBeacon(payload, this.multicastAddress, "multicast");
    // Limited broadcast helps on simple field routers that suppress multicast.
    this.sendBeacon(payload, "255.255.255.255", "broadcast");
  }

  private sendBeacon(payload: Buffer, address: string, kind: string): void {
    const remote = `${address}:${this.discoveryPort}`;
    this.wireLog.log({ channel: "discovery", direction: "tx", event: `udp.${kind}`, remote, bytes: payload.byteLength, payload: this.announcement, raw: payload });
    this.socket.send(payload, this.discoveryPort, address, (error) => {
      this.wireLog.log({ channel: "discovery", direction: "event", event: "socket.send", remote, bytes: payload.byteLength, outcome: error ? `error: ${error.message}` : "accepted-by-os" });
    });
  }

  close(): void {
    clearInterval(this.timer);
    this.socket.close();
  }
}

export function createAnnouncement(peerId: string, name: string, room: string, port: number): PeerAnnouncement {
  return { protocol: PROTOCOL, peerId, name, room, port };
}
