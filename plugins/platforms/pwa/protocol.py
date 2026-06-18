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


def ready(
    messages: list[dict[str, Any]],
    streaming_msg_id: str | None = None,
    streaming_text: str = "",
) -> str:
    """Send the current message list and optional mid-stream state.

    When *streaming_msg_id* is set, the client is reconnecting while an
    agent response is still being generated — it should resume streaming
    by prepending a partial assistant bubble and listening for further
    ``stream_delta`` events.
    """
    payload: dict[str, Any] = {"type": "ready", "messages": messages}
    if streaming_msg_id:
        payload["streaming"] = {
            "msg_id": streaming_msg_id,
            "text_so_far": streaming_text,
        }
    return _json(payload)


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


def history(messages: list[dict[str, Any]], has_more: bool = False) -> str:
    """Cold-data history loaded from SessionDB."""
    return _json({"type": "history", "messages": messages, "has_more": has_more})


def session_info(session_id: str, started_at: float = 0.0) -> str:
    """Notify client that a new session has begun (6h auto-split)."""
    return _json({
        "type": "session_info",
        "session_id": session_id,
        "started_at": started_at,
    })


def status_msg(connected: bool) -> str:
    """Connection status indicator."""
    return _json({"type": "status", "connected": connected})
