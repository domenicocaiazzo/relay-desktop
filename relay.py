"""
relay.py  —  Gira su Render.com (o qualsiasi VPS pubblico).
Mette in comunicazione il SERVER e il CLIENT che sono entrambi
dietro NAT (router diversi, reti diverse).

Dipendenze: nessuna (solo libreria standard Python)
"""

import socket
import threading
import time
import logging
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("relay")

# ── Configurazione ────────────────────────────────────────────────
# Render assegna la porta via variabile d'ambiente PORT
HTTP_PORT  = int(os.environ.get("PORT", 8080))   # porta HTTP per Render
RELAY_PORT = 5900                                  # porta TCP per il relay
HOST = "0.0.0.0"
CONNECT_TIMEOUT = 120
BUFFER = 65536
# ─────────────────────────────────────────────────────────────────

rooms: dict = {}
rooms_lock = threading.Lock()


# ── Health check HTTP (richiesto da Render) ───────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Relay OK")

    def log_message(self, *args):
        pass  # silenzia i log HTTP


def start_http_server():
    srv = HTTPServer((HOST, HTTP_PORT), HealthHandler)
    log.info(f"Health check HTTP su porta {HTTP_PORT}")
    srv.serve_forever()


# ── Relay TCP ─────────────────────────────────────────────────────

def recv_line(conn: socket.socket, timeout=30) -> str:
    conn.settimeout(timeout)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = conn.recv(256)
            if not chunk:
                return ""
            buf += chunk
        return buf.split(b"\n", 1)[0].decode("utf-8").strip()
    except Exception:
        return ""


def send_line(conn: socket.socket, msg: str):
    try:
        conn.sendall((msg + "\n").encode("utf-8"))
    except Exception:
        pass


def pipe(src: socket.socket, dst: socket.socket, label: str):
    try:
        while True:
            data = src.recv(BUFFER)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    log.info(f"[{label}] canale chiuso")


def handle_connection(conn: socket.socket, addr):
    log.info(f"Nuova connessione da {addr}")

    line = recv_line(conn, timeout=CONNECT_TIMEOUT)
    if not line:
        conn.close()
        return

    parts = line.split(":", 1)
    if len(parts) != 2 or parts[0] not in ("SERVER", "CLIENT"):
        log.warning(f"{addr}: handshake non valido: {line!r}")
        send_line(conn, "ERR handshake non valido")
        conn.close()
        return

    role, room_id = parts[0], parts[1].strip()
    log.info(f"{addr} -> ruolo={role}  stanza={room_id!r}")

    with rooms_lock:
        if room_id not in rooms:
            rooms[room_id] = {}
        room = rooms[room_id]
        if role in room:
            send_line(conn, f"ERR ruolo {role} gia occupato")
            conn.close()
            return
        room[role] = conn
        send_line(conn, "OK attendo controparte...")

    partner_role = "CLIENT" if role == "SERVER" else "SERVER"
    deadline = time.time() + CONNECT_TIMEOUT

    while time.time() < deadline:
        with rooms_lock:
            partner = rooms.get(room_id, {}).get(partner_role)
        if partner:
            break
        time.sleep(0.5)
    else:
        log.warning(f"Timeout: {partner_role} non arrivato per stanza {room_id!r}")
        send_line(conn, "ERR timeout")
        with rooms_lock:
            rooms.get(room_id, {}).pop(role, None)
            if not rooms.get(room_id):
                rooms.pop(room_id, None)
        conn.close()
        return

    with rooms_lock:
        server_conn = rooms[room_id].get("SERVER")
        client_conn = rooms[room_id].get("CLIENT")

    send_line(server_conn, "GO")
    send_line(client_conn, "GO")
    log.info(f"Stanza {room_id!r}: tunnel aperto!")

    t1 = threading.Thread(target=pipe, args=(server_conn, client_conn, f"{room_id} SRV->CLI"), daemon=True)
    t2 = threading.Thread(target=pipe, args=(client_conn, server_conn, f"{room_id} CLI->SRV"), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    with rooms_lock:
        rooms.pop(room_id, None)
    log.info(f"Stanza {room_id!r}: sessione terminata")
    for c in (server_conn, client_conn):
        try:
            c.close()
        except Exception:
            pass


def start_relay():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, RELAY_PORT))
    srv.listen(20)
    log.info(f"Relay TCP in ascolto su {HOST}:{RELAY_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


def main():
    # Avvia health check HTTP in background (richiesto da Render)
    t_http = threading.Thread(target=start_http_server, daemon=True)
    t_http.start()

    # Avvia relay TCP (bloccante)
    start_relay()


if __name__ == "__main__":
    main()
