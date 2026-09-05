import os from "node:os";
import path from "node:path";
import type { WireLogLevel } from "./wire-log.js";

export interface Config {
  name: string;
  room: string;
  host: string;
  port: number;
  discoveryPort: number;
  multicastAddress: string;
  dataDir: string;
  discoveryDisabled: boolean;
  peers: string[];
  wireLog: WireLogLevel;
}

function numberArg(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 && parsed <= 65535 ? parsed : fallback;
}

export function readConfig(argv = process.argv.slice(2), env = process.env): Config {
  const args = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 1) {
    const item = argv[index];
    if (!item?.startsWith("--")) continue;
    const [rawKey, inlineValue] = item.slice(2).split("=", 2);
    const next = argv[index + 1];
    const value = inlineValue ?? (next && !next.startsWith("--") ? argv[++index] : "true");
    args.set(rawKey!, value!);
  }

  const peerList = args.get("peer") ?? env.FIELDMESH_PEERS ?? "";
  const requestedWireLog = args.get("wire-log") ?? env.FIELDMESH_WIRE_LOG ?? "off";
  const wireLog: WireLogLevel = requestedWireLog === "true" ? "full" : requestedWireLog === "full" || requestedWireLog === "summary" ? requestedWireLog : "off";
  return {
    name: args.get("name") ?? env.FIELDMESH_NAME ?? os.hostname(),
    room: args.get("room") ?? env.FIELDMESH_ROOM ?? "demo",
    host: args.get("host") ?? env.FIELDMESH_HOST ?? "0.0.0.0",
    port: numberArg(args.get("port") ?? env.FIELDMESH_PORT, 4310),
    discoveryPort: numberArg(args.get("discovery-port") ?? env.FIELDMESH_DISCOVERY_PORT, 4311),
    multicastAddress: args.get("multicast") ?? env.FIELDMESH_MULTICAST ?? "239.255.42.99",
    dataDir: path.resolve(args.get("data-dir") ?? env.FIELDMESH_DATA_DIR ?? ".fieldmesh"),
    discoveryDisabled: args.get("no-discovery") === "true" || env.FIELDMESH_NO_DISCOVERY === "true",
    peers: peerList.split(",").map((peer) => peer.trim()).filter(Boolean),
    wireLog,
  };
}
