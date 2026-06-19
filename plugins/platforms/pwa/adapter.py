"""PWA WebSocket platform adapter.

Single-user, password-authenticated WebSocket server that a PWA frontend
connects to.  The WebSocket server binds to 127.0.0.1 only — nginx proxies
WSS from the public internet.

Streaming: the adapter implements ``edit_message`` so the gateway's
``GatewayStreamConsumer`` can push deltas during agent processing.
``send`` delivers the initial chunk (stream_start) or the final response.
Mid-stream reconnect resumes streaming via the ``ready`` payload.

Message persistence: hot buffer (last 50 messages) kept in memory;
cold data stored in SessionDB (SQLite FTS5).  ``load_history`` queries
the DB for scroll-up and search-anchored history.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource
from gateway.config import Platform, PlatformConfig

from .protocol import (
    auth_fail,
    auth_ok,
    history as _history_msg,
    ready,
    search_results,
    stream_delta,
    stream_done,
    stream_start,
    status_msg,
)

logger = logging.getLogger("hermes.gateway.platforms.pwa")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HOT_BUFFER_SIZE = 50
SESSION_SPLIT_SECONDS = 6 * 3600  # 6 hours
PWA_CHAT_ID = "pwa"
PWA_USER_ID = "pwa-user"

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------

_WEBSOCKETS_AVAILABLE = False

try:
    from websockets.asyncio.server import serve as ws_serve
    from websockets.exceptions import ConnectionClosed

    _WEBSOCKETS_AVAILABLE = True
except ImportError:
    pass


def check_requirements() -> bool:
    """Check that websockets package is installed and PWA_PASSWORD is set."""
    if not _WEBSOCKETS_AVAILABLE:
        logger.warning("websockets not installed — PWA platform disabled")
        return False
    if not os.getenv("PWA_PASSWORD"):
        logger.warning("PWA_PASSWORD not set — PWA platform disabled")
        return False
    return True


def is_connected(config: Any = None) -> bool:
    """Return whether PWA credentials are configured."""
    return bool(os.getenv("PWA_PASSWORD"))


_instance: Optional["PWAAdapter"] = None

# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class PWAAdapter(BasePlatformAdapter):
    """WebSocket server adapter for PWA mobile client."""

    def __init__(self, config: PlatformConfig) -> None:
        platform = Platform("pwa")
        super().__init__(config, platform)

        self._password = os.getenv("PWA_PASSWORD", "")
        self._ws_host = os.getenv("PWA_WS_HOST", "127.0.0.1")
        try:
            self._ws_port = int(os.getenv("PWA_WS_PORT", "61234"))
        except ValueError:
            self._ws_port = 61234

        # Single-user: one active WebSocket at a time.
        self._ws: Optional[Any] = None
        self._ws_server: Optional[Any] = None
        self._authenticated = False

        # In-memory hot buffer: last N messages.
        self._hot_messages: list[dict[str, Any]] = []

        # Track the currently-streaming message for mid-stream reconnect.
        self._streaming_msg_id: Optional[str] = None
        self._streaming_text: str = ""

        # Current session tracking (6h auto-split).
        self._current_session_id: str = ""
        self._last_message_time: float = 0.0

        # SessionDB (lazy init to keep adapter construction light).
        self._session_db: Optional[Any] = None

        global _instance
        _instance = self

    # ------------------------------------------------------------------
    # SessionDB access
    # ------------------------------------------------------------------

    def _get_session_db(self):
        """Lazy-init SessionDB, sharing the same state.db as the gateway."""
        if self._session_db is None:
            try:
                from hermes_state import SessionDB
                self._session_db = SessionDB()
            except Exception:
                logger.exception("Failed to open SessionDB for PWA")
        return self._session_db

    def _get_or_create_session(self) -> str:
        """Return the current session id, creating one if needed.

        If >6h elapsed since the last message, create a new session.
        A divider message (role="divider") is persisted to the DB so the
        session boundary shares the auto-increment message ordering.
        """
        db = self._get_session_db()
        now = time.time()

        if (
            self._current_session_id
            and self._last_message_time > 0
            and (now - self._last_message_time) <= SESSION_SPLIT_SECONDS
        ):
            return self._current_session_id

        # Store old session id for divider label.
        old_sid = self._current_session_id

        # New session needed.
        new_id = str(uuid.uuid4())[:8]
        self._current_session_id = new_id
        self._last_message_time = now

        if db:
            try:
                db.create_session(new_id, source="pwa")
            except Exception:
                logger.exception("Failed to create session in SessionDB")
                self._current_session_id = ""
                return ""

        # Persist a divider message so it shares message ordering.
        # Label includes the old session's short id for context.
        if old_sid:
            divider_text = f"新对话开始 (上次: {old_sid})"
        else:
            divider_text = "新对话开始"
        self._persist_message("divider", divider_text)

        return new_id

    def _persist_message(self, role: str, text: str) -> Optional[int]:
        """Write a message to SessionDB (cold data). Returns the row id."""
        db = self._get_session_db()
        if not db or not self._current_session_id:
            return None
        try:
            return db.append_message(
                session_id=self._current_session_id,
                role=role,
                content=text,
            )
        except Exception:
            logger.exception("Failed to persist message to SessionDB")
            return None

    def _append_hot(self, role: str, text: str, msg_id: Optional[int] = None) -> None:
        """Append a message to the in-memory hot buffer, trimming to size."""
        entry: dict[str, Any] = {
            "role": role,
            "text": text,
        }
        if msg_id is not None:
            entry["msg_id"] = msg_id
        self._hot_messages.append(entry)
        if len(self._hot_messages) > HOT_BUFFER_SIZE:
            self._hot_messages = self._hot_messages[-HOT_BUFFER_SIZE:]

    def _load_recent_messages(self) -> list[dict[str, Any]]:
        """Load recent messages from SessionDB and populate the hot buffer.

        Returns the message list in insertion order, including divider
        messages.  Used on first auth so reconnects see persisted history.
        """
        db = self._get_session_db()
        if not db or not self._current_session_id:
            return list(self._hot_messages)

        try:
            all_msgs = db.get_messages(self._current_session_id)
        except Exception:
            logger.exception("Failed to load messages from SessionDB")
            return list(self._hot_messages)

        # Populate hot buffer from DB.
        recent = all_msgs[-HOT_BUFFER_SIZE:] if len(all_msgs) > HOT_BUFFER_SIZE else all_msgs
        self._hot_messages = [
            {
                "role": m.get("role", "unknown"),
                "text": m.get("content", "") or "",
                "msg_id": m.get("id"),
            }
            for m in recent
        ]
        return list(self._hot_messages)

    @property
    def enforces_own_access_policy(self) -> bool:
        """PWA gates access at the WebSocket level via password auth."""
        return True

    # ------------------------------------------------------------------
    # BasePlatformAdapter abstract methods
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Start the WebSocket server on the gateway's asyncio event loop."""
        if not check_requirements():
            return False

        try:
            self._ws_server = await ws_serve(
                self._handle_connection,
                host=self._ws_host,
                port=self._ws_port,
            )
            self._running = True
            self._mark_connected()
            logger.info(
                "PWA WebSocket server listening on ws://%s:%d",
                self._ws_host,
                self._ws_port,
            )
            return True
        except Exception:
            logger.exception("Failed to start PWA WebSocket server")
            return False

    async def disconnect(self) -> None:
        """Stop the WebSocket server and close any active connection."""
        self._running = False

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._ws_server is not None:
            self._ws_server.close()
            try:
                await asyncio.wait_for(self._ws_server.wait_closed(), timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                pass
            self._ws_server = None

        self._mark_disconnected()
        logger.info("PWA WebSocket server stopped")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to the connected PWA client.

        PWA uses a single streaming message per turn.  The first call
        starts streaming; subsequent calls (tool-progress from the stream
        consumer) only accumulate text silently.  The gateway's final
        delivery sends ``stream_done`` to the client.
        """
        if not self._ws or not self._authenticated:
            return SendResult(success=False, error="No authenticated client connected")

        text = content or ""

        if self._streaming_msg_id is not None:
            # We already have an active streaming session.
            # Tool-progress / fallback chunks: silently accumulate.
            self._streaming_text += text
            return SendResult(success=True, message_id=self._streaming_msg_id)

        # First send of this turn — start streaming.
        msg_id = str(int(time.time() * 1000))
        self._streaming_msg_id = msg_id
        self._streaming_text = text

        try:
            await self._ws.send(stream_start(msg_id))
            if text:
                await self._ws.send(stream_delta(msg_id, text))
        except ConnectionClosed:
            self._streaming_msg_id = None
            self._streaming_text = ""
            self._ws = None
            self._authenticated = False
            return SendResult(success=False, error="WebSocket closed")

        return SendResult(success=True, message_id=msg_id)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Push a streaming delta to the PWA client.

        Called repeatedly by the gateway's GatewayStreamConsumer during
        agent processing.  ``finalize=True`` at tool boundaries and at
        the turn end.  We only close the stream when the gateway delivers
        the final response via a separate ``send()`` call, so
        ``finalize=True`` here is a no-op — it just keeps accumulating.
        """
        if not self._ws or not self._authenticated:
            return SendResult(success=False, error="No authenticated client connected")

        text = content or ""

        if finalize:
            # Tool boundary or turn end — accumulate but don't close the
            # stream.  The gateway will send the final answer via
            # ``send()`` which triggers ``stream_done``.
            if text:
                self._streaming_text = text
            return SendResult(success=True)

        # Streaming delta.
        if text and self._streaming_msg_id:
            delta = text[len(self._streaming_text):]
            if delta:
                self._streaming_text = text
                try:
                    await self._ws.send(
                        stream_delta(self._streaming_msg_id, delta)
                    )
                except ConnectionClosed:
                    self._streaming_text = ""
                    self._ws = None
                    self._authenticated = False
                    return SendResult(success=False, error="WebSocket closed")

        return SendResult(success=True)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": "PWA Mobile", "type": "dm", "chat_id": PWA_CHAT_ID}

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def _authenticate(self, ws: Any, password: str) -> bool:
        """Validate password against PWA_PASSWORD env var."""
        if not self._password:
            logger.error("PWA_PASSWORD not configured on server")
            await ws.send(auth_fail("Server misconfigured"))
            await ws.close()
            return False

        if password != self._password:
            logger.warning("PWA auth failed: incorrect password")
            await ws.send(auth_fail("Incorrect password"))
            await ws.close()
            return False

        logger.info("PWA client authenticated")
        await ws.send(auth_ok())

        # Load recent messages from SessionDB (or hot buffer if populated).
        msgs = self._load_recent_messages()

        # Send current state with loaded messages + mid-stream info.
        await ws.send(ready(
            msgs,
            streaming_msg_id=self._streaming_msg_id,
            streaming_text=self._streaming_text,
        ))
        self._authenticated = True
        return True

    # ------------------------------------------------------------------
    # WebSocket connection handler
    # ------------------------------------------------------------------

    async def _handle_connection(self, ws: Any) -> None:
        """Handle a single WebSocket connection lifecycle.

        Only one client is allowed at a time (single-user design).
        """
        peer = ws.remote_address
        logger.info("PWA WebSocket connection from %s", peer)

        # Single-user: kick the old connection.
        if self._ws is not None:
            logger.info("New PWA connection — closing previous")
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
            self._authenticated = False

        self._ws = ws
        self._authenticated = False

        try:
            async for raw in ws:
                try:
                    await self._dispatch(json.loads(raw))
                except json.JSONDecodeError:
                    logger.warning("PWA client sent invalid JSON: %.100r", raw)
                    await ws.send(json.dumps({"type": "error", "message": "Invalid JSON"}))
                except Exception:
                    logger.exception("Error handling PWA message")
        except ConnectionClosed:
            logger.info("PWA WebSocket connection closed by client")
        except Exception:
            logger.exception("PWA WebSocket error")
        finally:
            if self._ws is ws:
                self._ws = None
                self._authenticated = False

    async def _dispatch(self, data: dict[str, Any]) -> None:
        """Route an incoming JSON message to the appropriate handler."""
        ws = self._ws
        if ws is None:
            return

        msg_type = data.get("type", "")

        if msg_type == "auth":
            await self._authenticate(ws, str(data.get("password", "")))

        elif msg_type == "message":
            if not self._authenticated:
                await ws.send(json.dumps({"type": "error", "message": "Not authenticated"}))
                return
            await self._handle_user_message(data.get("text", ""))

        elif msg_type == "search":
            if not self._authenticated:
                await ws.send(json.dumps({"type": "error", "message": "Not authenticated"}))
                return
            await self._handle_search(data.get("query", ""))

        elif msg_type == "load_history":
            if not self._authenticated:
                await ws.send(json.dumps({"type": "error", "message": "Not authenticated"}))
                return
            await self._handle_load_history(
                data.get("before_msg_id"),
                data.get("around_msg_id"),
                int(data.get("limit", 20)),
            )

        elif msg_type == "load_sessions":
            if not self._authenticated:
                await ws.send(json.dumps({"type": "error", "message": "Not authenticated"}))
                return
            await self._handle_load_sessions()

        else:
            logger.debug("PWA unknown message type: %s", msg_type)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def _handle_user_message(self, text: str) -> None:
        """Handle a user text message: create MessageEvent and route to agent."""
        if not text.strip():
            return

        # 6h session auto-split.
        self._get_or_create_session()

        source = SessionSource(
            platform=self.platform,
            chat_id=PWA_CHAT_ID,
            chat_name="PWA Mobile",
            chat_type="dm",
            user_id=PWA_USER_ID,
            user_name="User",
        )

        event = MessageEvent(
            text=text.strip(),
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(int(time.time() * 1000)),
            timestamp=datetime.now(timezone.utc),
        )

        # Persist user message.
        user_msg_id = self._persist_message("user", text.strip())
        self._append_hot("user", text.strip(), msg_id=user_msg_id)

        # Route to agent — adapter.send() / edit_message() will be called
        # during processing for streaming, then send() for the final response.
        await self.handle_message(event)

        # After agent finishes, finalize the stream.  The stream consumer
        # has accumulated the final text via send() calls during processing.
        if self._streaming_msg_id and self._ws and self._authenticated:
            try:
                final_text = self._streaming_text or ""
                await self._ws.send(stream_done(self._streaming_msg_id, final_text))
                self._streaming_msg_id = None
                self._streaming_text = ""
                # Persist the final assistant response.
                if final_text:
                    asst_msg_id = self._persist_message("assistant", final_text)
                    self._append_hot("assistant", final_text, msg_id=asst_msg_id)
            except Exception:
                logger.exception("Failed to finalize PWA stream")
                self._streaming_msg_id = None
                self._streaming_text = ""

    # ------------------------------------------------------------------
    # Search (FTS5)
    # ------------------------------------------------------------------

    async def _handle_search(self, query: str) -> None:
        """Search messages using SessionDB FTS5."""
        ws = self._ws
        if ws is None:
            return

        if not query.strip():
            await ws.send(search_results([]))
            return

        db = self._get_session_db()
        results: list[dict[str, Any]] = []

        if db:
            try:
                # FTS5 search across all sessions.
                db_results = db.search_messages(
                    query.strip(),
                    source_filter=["pwa"],
                    limit=20,
                    sort="newest",
                )
                for row in db_results:
                    results.append({
                        "msg_id": row.get("id"),
                        "session_id": row.get("session_id"),
                        "role": row.get("role"),
                        "snippet": row.get("snippet", ""),
                        "text": row.get("content", "")[:200],
                        "timestamp": row.get("timestamp"),
                    })
            except Exception:
                logger.exception("FTS5 search failed")

        # Fallback: also search hot buffer.
        q = query.lower()
        hot_ids = {r.get("msg_id") for r in results}
        for msg in self._hot_messages:
            if q in msg.get("text", "").lower():
                results.append({
                    "role": msg["role"],
                    "snippet": msg["text"][:200],
                    "text": msg["text"][:200],
                    "timestamp": msg.get("timestamp"),
                    "hot": True,
                })

        await ws.send(search_results(results))

    # ------------------------------------------------------------------
    # History loading
    # ------------------------------------------------------------------

    async def _handle_load_history(
        self,
        before_msg_id: Optional[str],
        around_msg_id: Optional[str],
        limit: int,
    ) -> None:
        """Load history from SessionDB (cold data) or hot buffer."""
        ws = self._ws
        if ws is None:
            return

        # limit sanity
        limit = max(1, min(limit, 100))

        if around_msg_id:
            # Anchored load (search result click).
            db = self._get_session_db()
            if db:
                try:
                    # Try to resolve the anchor message.
                    # We don't know the session_id, so try current session first.
                    sess = self._current_session_id
                    if sess:
                        try:
                            msg_id_int = int(around_msg_id)
                            window = db.get_messages_around(sess, msg_id_int, limit)
                            if window and window.get("window"):
                                msgs = window["window"]
                                await ws.send(_history_msg(
                                    [self._db_row_to_msg(r) for r in msgs],
                                    has_more=(window.get("messages_before", 0) >= limit),
                                ))
                                return
                        except (ValueError, TypeError, Exception):
                            pass
                except Exception:
                    logger.exception("Failed anchored history load")
            await ws.send(_history_msg([], has_more=False))
            return

        if before_msg_id:
            # Scrolling up: load messages older than before_msg_id.
            db = self._get_session_db()
            if db and self._current_session_id:
                try:
                    all_msgs = db.get_messages(self._current_session_id)
                    # Find messages before the anchor.
                    before_idx = None
                    for i, m in enumerate(all_msgs):
                        if str(m.get("id")) == str(before_msg_id):
                            before_idx = i
                            break
                    if before_idx is not None and before_idx > 0:
                        start = max(0, before_idx - limit)
                        chunk = all_msgs[start:before_idx]
                        await ws.send(_history_msg(
                            [self._db_row_to_msg(r) for r in chunk],
                            has_more=(start > 0),
                        ))
                        return
                except Exception:
                    logger.exception("Failed history load")
            await ws.send(_history_msg([], has_more=False))
            return

        # No pagination: return hot buffer.
        await ws.send(_history_msg(
            list(self._hot_messages),
            has_more=False,
        ))

    async def _handle_load_sessions(self) -> None:
        """Return a list of past sessions for the client."""
        ws = self._ws
        if ws is None:
            return

        db = self._get_session_db()
        sessions: list[dict[str, Any]] = []
        if db:
            try:
                from hermes_state import SessionDB
                # Query sessions with source=pwa.
                with db._lock:
                    rows = db._conn.execute(
                        "SELECT id, started_at, message_count, source "
                        "FROM sessions WHERE source = ? "
                        "ORDER BY started_at DESC LIMIT 50",
                        ("pwa",),
                    ).fetchall()
                for row in rows:
                    sessions.append({
                        "session_id": row["id"],
                        "started_at": row["started_at"],
                        "message_count": row["message_count"],
                    })
            except Exception:
                logger.exception("Failed to load sessions")

        await ws.send(json.dumps({
            "type": "sessions",
            "sessions": sessions,
            "current": self._current_session_id,
        }))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _db_row_to_msg(row: Any) -> dict[str, Any]:
        """Convert a SessionDB row to the PWA message format."""
        msg = dict(row) if not isinstance(row, dict) else row
        return {
            "role": msg.get("role", "unknown"),
            "text": msg.get("content", "") or "",
            "timestamp": msg.get("timestamp", ""),
            "msg_id": msg.get("id"),
            "session_id": msg.get("session_id"),
        }

# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------


def register(ctx: Any) -> None:
    """Register the PWA platform with the Hermes plugin system."""
    ctx.register_platform(
        name="pwa",
        label="PWA Mobile",
        adapter_factory=lambda cfg: PWAAdapter(cfg),
        check_fn=check_requirements,
        is_connected=is_connected,
        required_env=["PWA_PASSWORD"],
        install_hint="pip install websockets",
        max_message_length=0,  # no limit for PWA
        emoji="📱",
        platform_hint=(
            "You are chatting via PWA mobile app. "
            "The user is likely an elderly person. "
            "Keep responses warm, patient, and use simple language. "
            "Support MarkdownV2 formatting. "
            "Messages are limited to ~4096 chars."
        ),
    )
