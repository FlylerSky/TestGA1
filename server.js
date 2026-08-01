/**
 * server.js - Chay tren Github Actions Runner
 * npm install ws
 */

"use strict";

const { execSync, spawn } = require("child_process");
const WebSocket = require("ws");

const RELAY_URL = process.env.RELAY_URL;
const SECRET    = process.env.SSH_SECRET;
const SHELL     = process.env.SHELL || "/bin/bash";

// ── Logger co timestamp ──────────────────────────────────────────────────────
function ts() {
  return new Date().toISOString().replace("T", " ").slice(0, 23);
}
const log  = (...a) => console.log (`[${ts()}] [server]`, ...a);
const warn = (...a) => console.warn (`[${ts()}] [server] WARN`, ...a);
const err  = (...a) => console.error(`[${ts()}] [server] ERR `, ...a);

if (!RELAY_URL || !SECRET) {
  err("Thieu RELAY_URL hoac SSH_SECRET");
  process.exit(1);
}

// ── Thong tin runner ─────────────────────────────────────────────────────────
log("============================================================");
log("Github Actions SSH Server khoi dong");
log(`  RELAY_URL : ${RELAY_URL}`);
log(`  Runner    : ${process.env.RUNNER_NAME    || "unknown"}`);
log(`  Repo      : ${process.env.GITHUB_REPOSITORY || "unknown"}`);
log(`  Ref       : ${process.env.GITHUB_REF     || "unknown"}`);
log(`  Run ID    : ${process.env.GITHUB_RUN_ID  || "unknown"}`);
log(`  Workspace : ${process.env.GITHUB_WORKSPACE || process.cwd()}`);
log(`  Shell     : ${SHELL}`);
log(`  Node      : ${process.version}`);
try {
  const ip = execSync("curl -sf --max-time 5 https://ipinfo.io/ip").toString().trim();
  log(`  Public IP : ${ip}`);
} catch (_) {
  warn("Khong lay duoc public IP");
}
log("============================================================");

// ── Stats ────────────────────────────────────────────────────────────────────
const stats = {
  connectAttempts : 0,
  msgSent         : 0,
  msgRecv         : 0,
  bytesSent       : 0,
  bytesRecv       : 0,
  shellStartTime  : null,
};

let retryCount = 0;

function connect() {
  stats.connectAttempts++;
  log(`Ket noi toi relay... (lan ${stats.connectAttempts}, retry #${retryCount})`);
  log(`  URL: ${RELAY_URL}`);

  const ws = new WebSocket(RELAY_URL, {
    headers: {
      "x-secret": SECRET,
      "x-role"  : "server",
    },
    handshakeTimeout: 15000,
  });

  // ── Timeout neu relay khong phan hoi ──────────────────────────────────────
  const connectTimeout = setTimeout(() => {
    warn("WebSocket handshake timeout (15s) — huy va thu lai");
    ws.terminate();
  }, 16000);

  ws.on("open", () => {
    clearTimeout(connectTimeout);
    retryCount = 0;
    log("Ket noi relay THANH CONG");
    log("Dang cho client SSH ket noi...");

    // ── Spawn shell ──────────────────────────────────────────────────────────
    stats.shellStartTime = Date.now();
    log(`Spawn shell: ${SHELL}`);

    const shell = spawn(SHELL, [], {
      env: {
        ...process.env,
        TERM      : "xterm-256color",
        COLORTERM : "truecolor",
        FORCE_COLOR: "1",
      },
      cwd: process.env.GITHUB_WORKSPACE || process.env.HOME || "/",
    });

    log(`Shell spawned PID=${shell.pid}`);

    // ── Heartbeat log moi 30 giac ────────────────────────────────────────────
    const heartbeat = setInterval(() => {
      const upSec = Math.floor((Date.now() - stats.shellStartTime) / 1000);
      log(`HEARTBEAT up=${upSec}s ws=${ws.readyState} sent=${stats.msgSent}msgs/${(stats.bytesSent/1024).toFixed(1)}KB recv=${stats.msgRecv}msgs/${(stats.bytesRecv/1024).toFixed(1)}KB`);
    }, 30000);

    // ── shell stdout → WebSocket ─────────────────────────────────────────────
    shell.stdout.on("data", (data) => {
      if (ws.readyState !== WebSocket.OPEN) {
        warn(`stdout DROP ${data.length}B — ws not OPEN (state=${ws.readyState})`);
        return;
      }
      const payload = data.toString("base64");
      ws.send(JSON.stringify({ type: "data", payload }));
      stats.msgSent++;
      stats.bytesSent += data.length;
      if (stats.msgSent <= 5) {
        log(`stdout->ws msg#${stats.msgSent} ${data.length}B`);
      }
    });

    // ── shell stderr → WebSocket ─────────────────────────────────────────────
    shell.stderr.on("data", (data) => {
      if (ws.readyState !== WebSocket.OPEN) return;
      const payload = data.toString("base64");
      ws.send(JSON.stringify({ type: "data", payload }));
      stats.msgSent++;
      stats.bytesSent += data.length;
      warn(`stderr->ws ${data.length}B: ${data.toString().slice(0, 120)}`);
    });

    shell.on("error", (e) => {
      err(`Shell spawn error: ${e.message}`);
    });

    shell.on("close", (code, signal) => {
      clearInterval(heartbeat);
      const dur = stats.shellStartTime
        ? ((Date.now() - stats.shellStartTime) / 1000).toFixed(1)
        : "?";
      log("============================================================");
      log(`Shell dong: code=${code} signal=${signal} duration=${dur}s`);
      log(`  Tong sent: ${stats.msgSent} msgs, ${(stats.bytesSent/1024).toFixed(1)} KB`);
      log(`  Tong recv: ${stats.msgRecv} msgs, ${(stats.bytesRecv/1024).toFixed(1)} KB`);
      log("============================================================");
      if (ws.readyState === WebSocket.OPEN) ws.close(1000, "shell exited");
      process.exit(0);
    });

    // ── WebSocket → shell stdin ──────────────────────────────────────────────
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        stats.msgRecv++;

        if (msg.type === "data") {
          const buf = Buffer.from(msg.payload, "base64");
          stats.bytesRecv += buf.length;
          if (stats.msgRecv <= 5) {
            log(`ws->stdin msg#${stats.msgRecv} ${buf.length}B`);
          }
          if (!shell.stdin.writable) {
            warn("stdin khong con writable — bo qua");
            return;
          }
          shell.stdin.write(buf);

        } else if (msg.type === "resize") {
          log(`Resize terminal: cols=${msg.cols} rows=${msg.rows} (ignored — no PTY)`);

        } else if (msg.type === "ping") {
          ws.send(JSON.stringify({ type: "pong" }));

        } else {
          warn(`Message type la: "${msg.type}"`);
        }
      } catch (e) {
        err(`Parse message that bai: ${e.message} raw=${raw.toString().slice(0, 80)}`);
      }
    });

    ws.on("close", (code, reason) => {
      clearInterval(heartbeat);
      log(`WebSocket dong: code=${code} reason=${reason?.toString() || "(none)"}`);
      log("Kill shell...");
      shell.kill("SIGTERM");
      setTimeout(() => shell.kill("SIGKILL"), 3000);
    });
  });

  ws.on("error", (e) => {
    clearTimeout(connectTimeout);
    err(`WebSocket error: ${e.message}`);
    scheduleRetry();
  });

  ws.on("close", (code, reason) => {
    clearTimeout(connectTimeout);
    if (code !== 1000) {
      warn(`WebSocket dong ngoai y muon: code=${code} reason=${reason?.toString() || "(none)"}`);
      scheduleRetry();
    }
  });

  ws.on("unexpected-response", (req, res) => {
    clearTimeout(connectTimeout);
    err(`Relay phan hoi HTTP ${res.statusCode} — kiem tra RELAY_URL va SSH_SECRET`);
    let body = "";
    res.on("data", (d) => (body += d));
    res.on("end", () => {
      err(`Response body: ${body.slice(0, 200)}`);
      scheduleRetry();
    });
  });
}

function scheduleRetry() {
  retryCount++;
  const delay = Math.min(5000 * retryCount, 30000); // backoff toi da 30s
  warn(`Thu lai lan ${retryCount} sau ${delay / 1000}s...`);
  setTimeout(connect, delay);
}

connect();

process.on("uncaughtException", (e) => {
  err("uncaughtException:", e.message, e.stack);
});

process.on("unhandledRejection", (e) => {
  err("unhandledRejection:", e);
});
