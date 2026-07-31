/**
 * server.js — Chạy trên Github Actions Runner
 *
 * Cách hoạt động:
 *  1. Kết nối WebSocket tới Relay Server (ví dụ: ngrok hoặc một VPS public)
 *  2. Spawn shell (`bash`) và pipe stdin/stdout qua WebSocket
 *  3. Client.js kết nối tới Relay để nhận shell session
 *
 * Cài đặt: npm install ws
 */

const { execSync, spawn } = require("child_process");
const WebSocket = require("ws");

// ─── Cấu hình ────────────────────────────────────────────────────────────────
const RELAY_URL = process.env.RELAY_URL; // wss://your-relay-server/session
const SECRET    = process.env.SSH_SECRET;  // Shared secret để xác thực
const SHELL     = process.env.SHELL || "/bin/bash";
// ─────────────────────────────────────────────────────────────────────────────

if (!RELAY_URL || !SECRET) {
  console.error("[server] ❌  Thiếu biến môi trường RELAY_URL hoặc SSH_SECRET");
  process.exit(1);
}

console.log("[server] 🚀  Github Actions runner đang kết nối tới relay...");
console.log(`[server]     Runner: ${process.env.RUNNER_NAME || "unknown"}`);
console.log(`[server]     Repo  : ${process.env.GITHUB_REPOSITORY || "unknown"}`);
console.log(`[server]     Ref   : ${process.env.GITHUB_REF || "unknown"}`);

// In ra một số thông tin debug hữu ích
try {
  console.log(`[server]     IP    : ${execSync("curl -sf https://ipinfo.io/ip").toString().trim()}`);
} catch (_) {}

function connect() {
  const ws = new WebSocket(RELAY_URL, {
    headers: {
      "x-secret": SECRET,
      "x-role"  : "server",
    },
  });

  ws.on("open", () => {
    console.log("[server] ✅  Đã kết nối relay. Đang chờ client SSH...");

    // Spawn shell với PTY environment
    const shell = spawn(SHELL, [], {
      env : {
        ...process.env,
        TERM       : "xterm-256color",
        COLORTERM  : "truecolor",
        FORCE_COLOR: "1",
      },
      cwd  : process.env.GITHUB_WORKSPACE || process.env.HOME || "/",
    });

    // shell stdout/stderr → WebSocket
    shell.stdout.on("data", (data) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "data", payload: data.toString("base64") }));
      }
    });

    shell.stderr.on("data", (data) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "data", payload: data.toString("base64") }));
      }
    });

    shell.on("close", (code) => {
      console.log(`[server] Shell đã thoát với code ${code}`);
      ws.close();
      process.exit(0);
    });

    // WebSocket → shell stdin
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());

        if (msg.type === "data") {
          shell.stdin.write(Buffer.from(msg.payload, "base64"));
        } else if (msg.type === "resize") {
          // node-pty nếu muốn hỗ trợ resize thực sự
          // shell.resize(msg.cols, msg.rows);
        } else if (msg.type === "ping") {
          ws.send(JSON.stringify({ type: "pong" }));
        }
      } catch (err) {
        console.error("[server] Lỗi parse message:", err.message);
      }
    });

    ws.on("close", () => {
      console.log("[server] Relay đóng kết nối — kill shell");
      shell.kill("SIGTERM");
    });
  });

  ws.on("error", (err) => {
    console.error("[server] ❌  WebSocket error:", err.message);
    console.log("[server] Thử lại sau 5 giây...");
    setTimeout(connect, 5000);
  });

  ws.on("close", (code, reason) => {
    if (code !== 1000) {
      console.warn(`[server] Kết nối đóng (${code}): ${reason} — thử lại...`);
      setTimeout(connect, 5000);
    }
  });
}

connect();    setTimeout(connect, 5000);
  });

  ws.on("close", (code, reason) => {
    if (code !== 1000) {
      console.warn(`[server] Kết nối đóng (${code}): ${reason} — thử lại...`);
      setTimeout(connect, 5000);
    }
  });
}

connect();
