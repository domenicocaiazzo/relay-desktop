"""
relay.py  —  Gira sul VPS pubblico.
Mette in comunicazione il SERVER e il CLIENT che sono entrambi
dietro NAT (router diversi, reti diverse).

Dipendenze: nessuna (solo libreria standard Python)
Avvio:      python relay.py
Porta:      5900 (modificabile sotto)
"""

import socket
import threading
import struct
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("relay")

# ── Configurazione ────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = 5900
PING_INTERVAL = 10      # secondi tra keepalive
CONNECT_TIMEOUT = 120   # secondi di attesa per la coppia
BUFFER = 65536
# ─────────────────────────────────────────────────────────────────

# Stanze in attesa: room_id → {"server": conn, "client": conn}
rooms: dict[str, dict] = {}
rooms_lock = threading.Lock()


def recv_line(conn: socket.socket, timeout=30) -> str | None:
    """Legge una riga terminata da \\n con timeout."""
    conn.settimeout(timeout)
    buf = b""
    try:
        while b"\n" not in buf:
            chunk = conn.recv(256)
            if not chunk:
                return None
            buf += chunk
        return buf.split(b"\n", 1)[0].decode("utf-8").strip()
    except Exception:
        return None


def send_line(conn: socket.socket, msg: str):
    try:
        conn.sendall((msg + "\n").encode("utf-8"))
    except Exception:
        pass


def pipe(src: socket.socket, dst: socket.socket, label: str):
    """Copia dati da src a dst finché uno dei due chiude."""
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

    # Handshake: il peer manda "SERVER:<room_id>" o "CLIENT:<room_id>"
    line = recv_line(conn, timeout=CONNECT_TIMEOUT)
    if not line:
        log.warning(f"{addr}: nessun handshake, chiudo")
        conn.close()
        return

    parts = line.split(":", 1)
    if len(parts) != 2 or parts[0] not in ("SERVER", "CLIENT"):
        log.warning(f"{addr}: handshake non valido: {line!r}")
        send_line(conn, "ERR handshake non valido")
        conn.close()
        return

    role, room_id = parts[0], parts[1].strip()
    log.info(f"{addr} → ruolo={role}  stanza={room_id!r}")

    with rooms_lock:
        if room_id not in rooms:
            rooms[room_id] = {}
        room = rooms[room_id]

        if role in room:
            send_line(conn, f"ERR ruolo {role} già occupato in questa stanza")
            conn.close()
            return

        room[role] = conn
        send_line(conn, "OK attendo controparte…")

    # Aspetta che arrivi anche l'altro peer
    deadline = time.time() + CONNECT_TIMEOUT
    partner_role = "CLIENT" if role == "SERVER" else "SERVER"

    while time.time() < deadline:
        with rooms_lock:
            partner = rooms.get(room_id, {}).get(partner_role)
        if partner:
            break
        time.sleep(0.5)
    else:
        log.warning(f"Timeout: {partner_role} non arrivato per stanza {room_id!r}")
        send_line(conn, "ERR timeout: controparte non connessa")
        with rooms_lock:
            rooms.get(room_id, {}).pop(role, None)
            if not rooms.get(room_id):
                rooms.pop(room_id, None)
        conn.close()
        return

    # Notifica entrambi
    with rooms_lock:
        server_conn = rooms[room_id].get("SERVER")
        client_conn = rooms[room_id].get("CLIENT")

    send_line(server_conn, "GO")
    send_line(client_conn, "GO")
    log.info(f"Stanza {room_id!r}: tunnel aperto!")

    # Avvia due thread di pipe bidirezionale
    t1 = threading.Thread(
        target=pipe, args=(server_conn, client_conn, f"{room_id} SRV→CLI"),
        daemon=True
    )
    t2 = threading.Thread(
        target=pipe, args=(client_conn, server_conn, f"{room_id} CLI→SRV"),
        daemon=True
    )
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Pulizia
    with rooms_lock:
        rooms.pop(room_id, None)
    log.info(f"Stanza {room_id!r}: sessione terminata")
    try:
        server_conn.close()
    except Exception:
        pass
    try:
        client_conn.close()
    except Exception:
        pass


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(20)
    log.info(f"Relay in ascolto su {HOST}:{PORT}")
    log.info("Premi Ctrl+C per fermare.")
    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        log.info("Relay fermato.")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
