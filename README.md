# FieldMesh Automerge POC

An offline-first, serverless LAN mesh for experimenting with C2-style shared data. Every device is an equal peer: it stores an Automerge document locally, discovers other nodes by UDP multicast/limited broadcast, and exchanges Automerge sync messages over reliable UDP unicast.

The included web console demonstrates:

- append-only tactical chat;
- shared force tracks;
- shared freehand map annotations;
- concurrent edits while disconnected, followed by automatic convergence;
- persistence across node restarts.

No internet connection or central backend is used. The Wi-Fi router/access point only provides a local IP network.

## Quick start

Requirements: Node.js 20 or newer. On each computer connected to the same isolated router:

```sh
npm install
npm start -- --name ALPHA --room exercise-1
```

Open `http://localhost:4310` on that computer. Start the same command on a second computer, using another name but the **same room**. The nodes should appear under **Mesh nodes** within a few seconds.

To simulate two or more devices on one computer, use different web/UDP data ports and data directories while sharing the discovery port:

```sh
npm start -- --name ALPHA --room demo --port 4310 --data-dir .fieldmesh-alpha
npm start -- --name BRAVO --room demo --port 4320 --data-dir .fieldmesh-bravo
npm start -- --name CHARLIE --room demo --port 4330 --data-dir .fieldmesh-charlie
```

Windows does not reliably fan a multicast datagram out to several Node processes sharing one UDP port. For same-host simulation only, nodes register their ephemeral discovery reply ports in the operating-system temporary directory and send one `udp.loopback-discover` datagram to each already-running local node. Discovery offers and all Automerge synchronization still use UDP. This local registry is not used to connect separate computers and is not a backend service.

Then open `http://localhost:4310` and `http://localhost:4320` in separate browser windows.

### Manual connection fallback

Some access points disable multicast or isolate wireless clients. Disable “AP/client isolation” in the router. If multicast is still unavailable, connect one node directly:

```sh
npm start -- --name BRAVO --room demo --peer 192.168.10.20:4310
```

Room names must match. Allow inbound TCP and UDP `4310`, plus UDP `4311`, in the device firewall. TCP `4310` serves the local browser UI; UDP `4310` carries peer sync. Ports and multicast group can be changed:

```text
--name NAME
--room ROOM
--host 0.0.0.0
--port 4310
--discovery-port 4311
--multicast 239.255.42.99
--data-dir .fieldmesh
--peer host:port[,host:port]
--no-discovery
--wire-log[=summary|full]
```

Equivalent environment variables use the `FIELDMESH_` prefix, for example `FIELDMESH_ROOM` and `FIELDMESH_PEERS`.

### Inspect network traffic

Add `--wire-log` (equivalent to `--wire-log=full`) to print newline-delimited structured records for UDP discovery, UDP peer handshakes/fragments/ACKs, Automerge sync messages, socket write outcomes, browser commands, and browser state updates:

```sh
npm start -- --name ALPHA --room demo --wire-log=full
```

Actual UDP sends and receives start with `[comms]`; all decoded views and local events start with `[trace]`. A `[comms]` record always has `recordKind: "udp-datagram"`. Its `direction` is `tx` or `rx`, while `networkScope` distinguishes physical LAN traffic from same-host loopback traffic. Trace-only records include Automerge decoded views, socket completion results, browser messages, HTTP requests, and local decisions; they must not be counted as additional mesh packets. Full mode includes complete payloads and raw bytes encoded as Base64; it can be very verbose and can expose all operational data in terminal logs. `--wire-log=summary` retains decoded event metadata and byte counts but replaces nested binary values with their sizes. Set `FIELDMESH_WIRE_LOG=full` to enable it through the environment.

Example event names include `udp.multicast-discover`, `udp.broadcast-discover`, `udp.loopback-discover` (same-host simulation), `udp.unicast-offer`, `udp.hello`, `udp.data`, `udp.ack`, `udp.retransmit`, and `automerge.sync`. An `accepted-by-os` socket outcome only means the local operating system accepted the write. `udp.message-acknowledged` means the remote peer reassembled the complete message.

The Mesh Nodes panel shows **SYNCED** when Automerge's sync state reports that the peer has acknowledged all changes currently held by this node. **SYNCING** means it has not yet done so. This is a document replication acknowledgement, not proof that an operator viewed a chat message. Human read receipts are not implemented.

## How it works

```text
┌──────────────┐    UDP multicast beacon     ┌──────────────┐
│ Device Alpha │ <--------------------------> │ Device Bravo │
│ Automerge DB │ === reliable UDP unicast ===>│ Automerge DB │
│ Local web UI │                              │ Local web UI │
└──────────────┘                              └──────────────┘
```

A joining node sends one multicast discovery request. Existing nodes each reply once by unicast after an independently randomized 0-750 ms delay. Only when multicast produces no offers after 1.75 seconds does the joining node send one limited-broadcast fallback request. There is no periodic discovery or idle peer heartbeat. Discovery runs again only after a real reliable-message delivery failure; configured manual endpoints retry every five seconds only while disconnected. Document traffic uses UDP unicast with a small reliability layer: a 28-byte header, 1,000-byte fragment payloads, message sequence numbers, reassembly, acknowledgements, duplicate suppression, in-order delivery, and retransmission after 750 ms. A message is retried up to ten times before the peer is dropped and discovery can re-establish it.

The complete discovery, packet format, acknowledgement, retry, restart, and wire-log design is recorded in [`docs/PROTOCOL.md`](docs/PROTOCOL.md).

The browser UI still uses a local `/client` WebSocket to its own Node process. WebSockets are no longer used between nodes.

Local data is stored at `.fieldmesh/<room>.automerge`; `.fieldmesh/identity` is the stable device/Automerge actor ID.

## Verify

```sh
npm test
npm run typecheck
npm run build
npm run serve
```

The automated test creates concurrent changes on two isolated replicas, runs the real Automerge sync protocol, and checks that their heads and content converge.

## Flutter direction

Automerge is a sound format/protocol choice, but there is no first-party, mature Dart package comparable to the JavaScript and Rust implementations today. For a production Flutter client, keep the document schema and transport boundary from this POC and bind the official `automerge-rs` core through Dart FFI (for example with `flutter_rust_bridge`). That retains binary document and sync-protocol compatibility rather than reimplementing Automerge in Dart.

On Android, multicast discovery requires multicast/Wi-Fi permissions and a `MulticastLock`; iOS requires local-network and Bonjour/multicast entitlements. mDNS/Bonjour may be a better native discovery layer while retaining UDP unicast for peer synchronization.

## Deliberate POC limitations

This is **not deployment-ready for military or safety-critical use**:

- no authentication, authorization, encryption, signing, or device revocation;
- no protection against a malicious peer, replay, traffic analysis, or radio/network disruption;
- one whole-room Automerge document, which will eventually need bounded documents, retention, snapshots, and compaction strategy;
- wall-clock timestamps are display metadata, not trusted ordering;
- UDP peer discovery works only within the multicast/broadcast domain; it is not an RF mesh routing protocol;
- the reliable UDP layer is experimental and does not yet implement congestion control, bandwidth shaping, priority queues, authentication, or encryption;
- drawings use a normalized demonstration grid, not GIS coordinates or a tactical symbology standard;
- concurrent updates of the exact same track field resolve deterministically but are not surfaced as operator-visible conflicts.

Before field use, add authenticated membership, transport encryption, signed changes, key rotation, data-at-rest protection, audit policy, schema migrations, resource limits, fuzzing, and loss/partition testing on representative radios and devices.
