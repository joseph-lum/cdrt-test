const $ = (selector) => document.querySelector(selector);
const state = { document: { messages: [], tracks: {}, drawings: [] }, peers: [], self: null };
let socket;
let reconnectTimer;
let currentStroke = null;

function connect() {
  socket = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/client`);
  socket.addEventListener("open", () => { $("#network-dot").classList.add("online"); $("#network-label").textContent = "NODE ONLINE"; });
  socket.addEventListener("close", () => { $("#network-dot").classList.remove("online"); $("#network-label").textContent = "RECONNECTING"; clearTimeout(reconnectTimer); reconnectTimer = setTimeout(connect, 1000); });
  socket.addEventListener("message", ({ data }) => {
    const message = JSON.parse(data);
    if (message.type === "state") { Object.assign(state, message); render(); }
    if (message.type === "error") toast(message.message);
  });
}

function send(command) {
  if (socket?.readyState !== WebSocket.OPEN) return toast("Local node is disconnected");
  socket.send(JSON.stringify(command));
}

function render() {
  $("#identity").textContent = `${state.self.name} / ${state.self.room} / ${state.self.peerId.slice(0, 8)}`;
  const messages = [...state.document.messages].sort((a,b) => a.createdAt-b.createdAt || a.id.localeCompare(b.id));
  $("#message-count").textContent = messages.length;
  $("#messages").classList.toggle("empty-state", !messages.length);
  $("#messages").innerHTML = messages.length ? messages.map((item) => `<div class="message"><div class="message-meta">${escapeHtml(item.author)} · ${formatTime(item.createdAt)}</div><div class="message-text">${escapeHtml(item.text)}</div></div>`).join("") : "NO TRAFFIC";

  const tracks = Object.values(state.document.tracks).sort((a,b) => a.callsign.localeCompare(b.callsign));
  $("#track-count").textContent = tracks.length;
  $("#tracks").classList.toggle("empty-state", !tracks.length);
  $("#tracks").innerHTML = tracks.length ? tracks.map((item) => `<div class="track-row ${item.affiliation}"><strong>${escapeHtml(item.callsign)}</strong><small>${item.latitude.toFixed(4)}, ${item.longitude.toFixed(4)}</small><small>${item.affiliation.toUpperCase()}</small><small>${escapeHtml(item.updatedBy)} · ${formatTime(item.updatedAt)}</small></div>`).join("") : "NO TRACKS REPORTED";
  $("#track-layer").innerHTML = tracks.map((item) => `<div class="track ${item.affiliation}" style="left:${((item.longitude+180)/360)*100}%;top:${((90-item.latitude)/180)*100}%"><div class="track-symbol"></div><span>${escapeHtml(item.callsign)}</span></div>`).join("");

  $("#peer-count").textContent = state.peers.length;
  $("#peers").classList.toggle("empty-state", !state.peers.length);
  $("#peers").innerHTML = state.peers.length ? state.peers.map((peer) => `<div class="peer-row ${peer.ourChangesAcknowledged ? "friendly" : "unknown"}"><strong>${escapeHtml(peer.name)}</strong><small>${peer.ourChangesAcknowledged ? "SYNCED" : "SYNCING"}</small><small>${peer.peerId.slice(0,16)}</small></div>`).join("") : "SEARCHING LOCAL NETWORK…";
  drawCanvas();
}

$("#chat-form").addEventListener("submit", (event) => { event.preventDefault(); const input=event.target.elements.text; send({type:"chat.add",text:input.value}); input.value=""; });
$("#track-form").addEventListener("submit", (event) => { event.preventDefault(); const data=new FormData(event.target); send({type:"track.upsert",callsign:data.get("callsign"),affiliation:data.get("affiliation"),latitude:Number(data.get("latitude")),longitude:Number(data.get("longitude"))}); event.target.reset(); });
$("#clear-drawings").addEventListener("click", () => send({type:"drawing.clear"}));

const canvas = $("#drawing-canvas");
canvas.addEventListener("pointerdown", (event) => { canvas.setPointerCapture(event.pointerId); currentStroke=[normalizedPoint(event)]; drawCanvas(); });
canvas.addEventListener("pointermove", (event) => { if (!currentStroke) return; const point=normalizedPoint(event); const last=currentStroke.at(-1); if (Math.hypot(point.x-last.x,point.y-last.y)>.003) currentStroke.push(point); drawCanvas(); });
canvas.addEventListener("pointerup", () => { if (currentStroke?.length > 1) send({type:"drawing.add",color:$("#draw-color").value,points:currentStroke}); currentStroke=null; });
window.addEventListener("resize", drawCanvas);

function normalizedPoint(event) { const box=canvas.getBoundingClientRect(); return {x:(event.clientX-box.left)/box.width,y:(event.clientY-box.top)/box.height}; }
function drawCanvas() { const ratio=devicePixelRatio||1; const box=canvas.getBoundingClientRect(); canvas.width=Math.round(box.width*ratio);canvas.height=Math.round(box.height*ratio);const context=canvas.getContext("2d");context.scale(ratio,ratio);context.lineWidth=2;context.lineCap="round";context.lineJoin="round";for(const drawing of [...state.document.drawings,{color:$("#draw-color").value,points:currentStroke||[]}]){if(drawing.points.length<2)continue;context.strokeStyle=drawing.color;context.beginPath();drawing.points.forEach((point,index)=>index?context.lineTo(point.x*box.width,point.y*box.height):context.moveTo(point.x*box.width,point.y*box.height));context.stroke();} }
function escapeHtml(value) { const element=document.createElement("span");element.textContent=String(value);return element.innerHTML; }
function formatTime(value) { return new Date(value).toLocaleTimeString([], {hour:"2-digit",minute:"2-digit"}); }
function toast(message) { const element=$("#toast");element.textContent=message;element.classList.add("show");setTimeout(()=>element.classList.remove("show"),2500); }
connect();
