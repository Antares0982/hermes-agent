"""PWA WebSocket JSON message protocol formatters.

All messages are JSON strings.  This module provides helper functions
that return pre-serialised JSON so the adapter can send them directly
over the WebSocket.

Protocol defined in design/hermes-mobile-pwa.md §3.3.
"""

from __future__ import annotations

import json
from typing import Any


def _json(obj: dict[str, Any]) -> str:
    """Serialize a dict to a compact JSON string."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# ── Server → Client messages ──


def auth_ok() -> str:
    """Authentication successful."""
    return _json({"type": "auth_ok"})


def auth_fail(reason: str = "") -> str:
    """Authentication failed."""
    return _json({"type": "auth_fail", "reason": reason})


def ready(messages: list[dict[str, Any]]) -> str:
    """Send the current hot-buffer message list after auth or reconnect."""
    return _json({"type": "ready", "messages": messages})


def stream_start(msg_id: str) -> str:
    """Signal that streaming has begun for a message."""
    return _json({"type": "stream_start", "msg_id": msg_id})


def stream_delta(msg_id: str, token: str) -> str:
    """Push a single token during streaming."""
    return _json({"type": "stream_delta", "msg_id": msg_id, "token": token})


def stream_done(msg_id: str, full_text: str) -> str:
    """Streaming complete — send the full text."""
    return _json({"type": "stream_done", "msg_id": msg_id, "full_text": full_text})


def search_results(results: list[dict[str, Any]]) -> str:
    """Search results from FTS5 query."""
    return _json({"type": "search_results", "results": results})


def status_msg(connected: bool) -> str:
    """Connection status indicator."""
    return _json({"type": "status", "connected": connected})
