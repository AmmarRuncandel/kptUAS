"""
common/protocol.py
==================
Komponen c – Distributed Processing: Format pesan + framing TCP (length-prefixed 8-byte).

Semua komunikasi Master-Worker menggunakan protokol berikut:
  [8-byte header: panjang payload (big-endian uint64)] [payload JSON/binary]

Pesan kontrol berformat JSON; payload data biner dikirim terpisah (bukan base64).
"""

from __future__ import annotations
import json
import socket
import struct
import logging
from enum import Enum
from typing import Optional, Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tipe Pesan (Komponen c)
# ---------------------------------------------------------------------------

class MsgType(str, Enum):
    """Semua tipe pesan yang valid dalam protokol Master-Worker."""
    REGISTER        = "REGISTER"        # Worker → Master: daftar & info node
    REGISTER_ACK    = "REGISTER_ACK"    # Master → Worker: konfirmasi pendaftaran
    REQUEST_TASK    = "REQUEST_TASK"    # Worker → Master: minta task baru
    TASK_ASSIGN     = "TASK_ASSIGN"     # Master → Worker: kirim task + data chunk
    TASK_RESULT     = "TASK_RESULT"     # Worker → Master: kirim hasil kompresi
    HEARTBEAT       = "HEARTBEAT"       # Worker → Master: tanda hidup + status CPU/mem
    HEARTBEAT_ACK   = "HEARTBEAT_ACK"   # Master → Worker: konfirmasi heartbeat
    NO_MORE_TASKS   = "NO_MORE_TASKS"   # Master → Worker: tidak ada task lagi
    SHUTDOWN        = "SHUTDOWN"        # Master → Worker: perintah berhenti
    ERROR           = "ERROR"           # bidirectional: laporkan error
    STATUS_QUERY    = "STATUS_QUERY"    # Master → Worker: tanya status
    STATUS_REPLY    = "STATUS_REPLY"    # Worker → Master: balas status


HEADER_SIZE = 8   # 8-byte length prefix, big-endian uint64
MAX_MSG_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB batas aman


# ---------------------------------------------------------------------------
# Fungsi kirim / terima pesan (Komponen c)
# ---------------------------------------------------------------------------

def send_message(sock: socket.socket, msg_type: str, payload: dict[str, Any],
                 binary_data: Optional[bytes] = None) -> None:
    """
    Kirim satu pesan ke socket.

    Format kawat:
        [8B: panjang JSON frame] [JSON bytes]
        jika binary_data:
            [8B: panjang biner] [binary bytes]

    Args:
        sock: socket TCP aktif.
        msg_type: nilai MsgType.
        payload: dict yang akan di-encode ke JSON.
        binary_data: bytes opsional (payload data chunk / hasil kompresi).
    """
    payload["type"] = msg_type
    has_binary = binary_data is not None
    payload["has_binary"] = has_binary
    if has_binary:
        payload["binary_length"] = len(binary_data)

    json_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if len(json_bytes) > MAX_MSG_SIZE:
        raise ValueError(f"JSON payload terlalu besar: {len(json_bytes)} bytes")

    # Kirim JSON frame
    _send_raw(sock, json_bytes)

    # Kirim binary frame (jika ada)
    if has_binary:
        _send_raw(sock, binary_data)


def recv_message(sock: socket.socket) -> tuple[dict[str, Any], Optional[bytes]]:
    """
    Terima satu pesan dari socket.

    Returns:
        (header_dict, binary_data_or_None)
    """
    # Terima JSON frame
    json_bytes = _recv_raw(sock)
    header = json.loads(json_bytes.decode("utf-8"))

    # Terima binary frame jika ada
    binary_data: Optional[bytes] = None
    if header.get("has_binary"):
        binary_data = _recv_raw(sock)

    return header, binary_data


def _send_raw(sock: socket.socket, data: bytes) -> None:
    """Kirim [8B length][data] ke socket, menangani partial send."""
    length_header = struct.pack(">Q", len(data))
    sock.sendall(length_header)
    sock.sendall(data)


def _recv_raw(sock: socket.socket) -> bytes:
    """Terima [8B length][data] dari socket, menangani partial recv."""
    # Baca 8-byte header
    header_buf = _recv_exact(sock, HEADER_SIZE)
    length = struct.unpack(">Q", header_buf)[0]
    if length > MAX_MSG_SIZE:
        raise ValueError(f"Pesan terlalu besar: {length} bytes")
    # Baca payload
    return _recv_exact(sock, length)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Baca tepat n byte dari socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(65536, n - len(buf)))
        if not chunk:
            raise ConnectionError("Koneksi terputus saat menerima data")
        buf.extend(chunk)
    return bytes(buf)
