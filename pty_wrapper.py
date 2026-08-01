#!/usr/bin/env python3
"""
pty_wrapper.py - Tao PTY that cho bash, nhan lenh resize tu stdin JSON
Giao thuc:
  stdin  <- JSON lines tu Node.js:
              {"t":"d","p":"<base64>"}   # data -> pty
              {"t":"r","c":80,"r":24}    # resize pty
  stdout -> tat ca output tu PTY, raw bytes (khong encode)
"""
import sys, os, pty, signal, select, struct, fcntl, termios, json, base64, threading

SHELL = os.environ.get("SHELL", "/bin/bash")
COLS  = int(os.environ.get("PTY_COLS", "220"))
ROWS  = int(os.environ.get("PTY_ROWS", "50"))

def set_pty_size(fd, cols, rows):
    size = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, size)

# Fork voi PTY that
pid, master_fd = pty.fork()

if pid == 0:
    # Child: exec shell
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"
    env["COLORTERM"] = "truecolor"
    os.execvpe(SHELL, [SHELL], env)
    sys.exit(1)

# Parent: set kich thuoc ban dau
set_pty_size(master_fd, COLS, ROWS)
sys.stderr.write(f"[pty] PTY created pid={pid} fd={master_fd} size={COLS}x{ROWS}\n")
sys.stderr.flush()

# Thread doc stdin (JSON commands tu Node.js)
def stdin_reader():
    buf = b""
    while True:
        try:
            ch = sys.stdin.buffer.read(1)
            if not ch:
                break
            if ch == b"\n":
                if buf.strip():
                    try:
                        msg = json.loads(buf.decode())
                        if msg.get("t") == "d":
                            data = base64.b64decode(msg["p"])
                            os.write(master_fd, data)
                        elif msg.get("t") == "r":
                            cols = int(msg.get("c", 80))
                            rows = int(msg.get("r", 24))
                            set_pty_size(master_fd, cols, rows)
                            os.kill(pid, signal.SIGWINCH)
                            sys.stderr.write(f"[pty] resize {cols}x{rows}\n")
                            sys.stderr.flush()
                    except Exception as e:
                        sys.stderr.write(f"[pty] stdin parse err: {e} buf={buf}\n")
                buf = b""
            else:
                buf += ch
        except Exception:
            break

t = threading.Thread(target=stdin_reader, daemon=True)
t.start()

# Main: doc output tu PTY → stdout raw
try:
    while True:
        r, _, _ = select.select([master_fd], [], [], 1.0)
        if r:
            try:
                data = os.read(master_fd, 4096)
                if not data:
                    break
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
            except OSError:
                break
        # Check neu child da chet
        result = os.waitpid(pid, os.WNOHANG)
        if result[0] != 0:
            break
except Exception as e:
    sys.stderr.write(f"[pty] main loop err: {e}\n")

sys.stderr.write(f"[pty] shell exited\n")
sys.stderr.flush()
