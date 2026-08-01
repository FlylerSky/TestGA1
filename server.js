/**
 * server.js - Chay tren Github Actions Runner
 * 
 * Ho tro:
 *   - Interactive shell (PTY that qua pty_wrapper.py)
 *   - Port forwarding: tunnel cac port tren runner ve may local
 *
 * npm install ws
 */
"use strict";

const { execSync, spawn } = require("child_process");
const net     = require("net");
const path    = require("path");
const WebSocket = require("ws");

const RELAY_URL   = process.env.RELAY_URL;
const SECRET      = process.env.SSH_SECRET;
const SHELL       = process.env.SHELL || "/bin/bash";
const PTY_WRAPPER = path.join(__dirname, "pty_wrapper.py");

function ts() { return new Date().toISOString().replace("T"," ").slice(0,23); }
const log  = (...a) => console.log (`[${ts()}] [server]`, ...a);
const warn = (...a) => console.warn (`[${ts()}] [server] WARN`, ...a);
const err  = (...a) => console.error(`[${ts()}] [server] ERR `, ...a);

if (!RELAY_URL || !SECRET) { err("Thieu RELAY_URL hoac SSH_SECRET"); process.exit(1); }

log("============================================================");
log(`Relay  : ${RELAY_URL}`);
log(`Shell  : ${SHELL}`);
log(`Node   : ${process.version}`);
try { log(`IP     : ${execSync("curl -sf --max-time 5 https://ipinfo.io/ip").toString().trim()}`); } catch(_){}
log("============================================================");

let retryCount = 0;

function connect() {
  log(`Ket noi relay... (retry #${retryCount})`);

  const ws = new WebSocket(RELAY_URL, {
    headers: { "x-secret": SECRET, "x-role": "server" },
    handshakeTimeout: 15000,
  });

  const connectTimeout = setTimeout(() => {
    warn("Handshake timeout 15s"); ws.terminate();
  }, 16000);

  // ── Port forward channel map ─────────────────────────────────────────────
  // ch=0 → reserved cho shell (PTY)
  // ch=1,2,... → TCP connections toi cac port tren runner
  const pfChannels = new Map(); // ch -> net.Socket

  function wsSend(obj) {
    if (ws.readyState === WebSocket.OPEN)
      ws.send(JSON.stringify(obj));
  }

  ws.on("open", () => {
    clearTimeout(connectTimeout);
    retryCount = 0;
    log("Relay OK — spawn PTY");

    // ── PTY shell ─────────────────────────────────────────────────────────
    const pty = spawn("python3", [PTY_WRAPPER], {
      env: { ...process.env, SHELL, PTY_COLS:"220", PTY_ROWS:"50" },
      cwd: process.env.GITHUB_WORKSPACE || process.env.HOME || "/",
      stdio: ["pipe","pipe","pipe"],
    });
    log(`PTY PID=${pty.pid}`);

    pty.stdout.on("data", (data) => {
      wsSend({ type:"data", ch:0, payload: data.toString("base64") });
    });
    pty.stderr.on("data", (d) => log(`[pty] ${d.toString().trim()}`));
    pty.on("error", (e) => err(`PTY error: ${e.message}`));
    pty.on("close", (code) => {
      log(`PTY closed code=${code}`);
      // Dong het cac port forward
      for (const [ch, sock] of pfChannels) {
        sock.destroy();
        pfChannels.delete(ch);
      }
      if (ws.readyState === WebSocket.OPEN) ws.close(1000, "shell exited");
      process.exit(0);
    });

    // ── WebSocket message handler ─────────────────────────────────────────
    ws.on("message", (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch(e) { return; }

      const ch = msg.ch ?? 0;

      switch (msg.type) {

        // ── Shell data / resize / ping ──────────────────────────────────
        case "data":
          if (ch === 0) {
            // → PTY stdin
            const buf = Buffer.from(msg.payload, "base64");
            pty.stdin.write(JSON.stringify({ t:"d", p: buf.toString("base64") }) + "\n");
          } else {
            // → Port forward socket
            const sock = pfChannels.get(ch);
            if (sock && !sock.destroyed) {
              sock.write(Buffer.from(msg.payload, "base64"));
            } else {
              warn(`pf data drop: ch=${ch} not found`);
            }
          }
          break;

        case "resize":
          pty.stdin.write(JSON.stringify({ t:"r", c: msg.cols, r: msg.rows }) + "\n");
          break;

        case "ping":
          wsSend({ type:"pong" });
          break;

        // ── Port forward: mo ket noi TCP moi tren runner ────────────────
        case "pf_open":
          // Client yeu cau mo tunnel toi remotePort tren runner
          const remotePort = msg.remotePort;
          const newCh      = ch; // client da chon ch

          log(`pf_open ch=${newCh} → localhost:${remotePort}`);

          const socket = net.connect(remotePort, "127.0.0.1", () => {
            log(`pf ch=${newCh} connected → :${remotePort}`);
            wsSend({ type:"pf_connected", ch: newCh });
          });

          socket.on("data", (data) => {
            wsSend({ type:"data", ch: newCh, payload: data.toString("base64") });
          });

          socket.on("close", () => {
            log(`pf ch=${newCh} socket closed`);
            pfChannels.delete(newCh);
            wsSend({ type:"pf_closed", ch: newCh });
          });

          socket.on("error", (e) => {
            err(`pf ch=${newCh} error: ${e.message}`);
            pfChannels.delete(newCh);
            wsSend({ type:"pf_error", ch: newCh, message: e.message });
          });

          pfChannels.set(newCh, socket);
          break;

        // ── Port forward: dong ket noi ───────────────────────────────────
        case "pf_close":
          const closeSock = pfChannels.get(ch);
          if (closeSock) {
            closeSock.destroy();
            pfChannels.delete(ch);
            log(`pf ch=${ch} closed by client`);
          }
          break;

        default:
          warn(`Unknown msg type: ${msg.type}`);
      }
    });

    ws.on("close", (code, reason) => {
      log(`WS closed code=${code} reason=${reason?.toString()||"(none)"}`);
      pty.kill("SIGTERM");
      setTimeout(() => pty.kill("SIGKILL"), 3000);
      for (const [, sock] of pfChannels) sock.destroy();
    });
  });

  ws.on("error", (e) => { clearTimeout(connectTimeout); err(e.message); scheduleRetry(); });
  ws.on("close", (code) => {
    clearTimeout(connectTimeout);
    if (code !== 1000) scheduleRetry();
  });
  ws.on("unexpected-response", (_req, res) => {
    clearTimeout(connectTimeout);
    err(`HTTP ${res.statusCode} tu relay`);
    let body = "";
    res.on("data", (d) => body += d);
    res.on("end", () => { err(`Body: ${body.slice(0,200)}`); scheduleRetry(); });
  });
}

function scheduleRetry() {
  retryCount++;
  const delay = Math.min(5000 * retryCount, 30000);
  warn(`Thu lai sau ${delay/1000}s...`);
  setTimeout(connect, delay);
}

connect();
process.on("uncaughtException", (e) => err("uncaught:", e.message));
process.on("unhandledRejection", (e) => err("unhandled:", e));
