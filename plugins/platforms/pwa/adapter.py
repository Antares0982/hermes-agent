"""PWA WebSocket platform adapter.

Single-user, password-authenticated WebSocket server that a PWA frontend
connects to.  The WebSocket server binds to 127.0.0.1 only — nginx proxies
WSS from the public internet.

Streaming: PWA does NOT use the gateway's draft/edit-based streaming
(send_draft / edit_message).  Instead, the adapter maintains a reference
to the active WebSocket and pushes stream deltas directly during
agent processing.  The adapter's send() delivers the final complete
message when the agent finishes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
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
    ready,
    search_results,
    stream_delta,
    stream_done,
    stream_start,
    status_msg,
)

logger = logging.getLogger("hermes.gateway.platforms.pwa")

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


def is_connected() -> bool:
    """Return whether PWA WebSocket server is currently running."""
    return _instance is not None and _instance._running


_instance: Optional["PWAAdapter"] = None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class PWAAdapter(BasePlatformAdapter):
    """WebSocket server adapter for PWA mobile client."""

    def __init__(self, config: PlatformConfig) -> None:
        # The PWA platform isn't in upstream's Platform enum, so we
        # construct a Platform value dynamically.  Platform._missing_()
        # (in gateway/config.py) creates member values on the fly for
        # plugin-registered platform names.
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
        self._ws_server_task: Optional[asyncio.Task] = None
        self._authenticated = False

        # In-memory hot buffer: last 50 messages (mirrors the design doc
        # data-layering scheme).  Keyed by (message_id, role, text, timestamp).
        self._hot_messages: list[dict[str, Any]] = []

        # Track the currently-streaming message so we can resume if the
        # client disconnects mid-stream and reconnects.
        self._streaming_msg_id: Optional[str] = None
        self._streaming_text: str = ""

        global _instance
        _instance = self

    @property
    def enforces_own_access_policy(self) -> bool:
        """PWA gates access at the WebSocket level via password auth."""
        return True

    # ------------------------------------------------------------------
    # BasePlatformAdapter abstract methods
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Start the WebSocket server on the gateway's asyncio event loop.

        Important (pitfall #1 from design doc): do NOT create a separate
        event loop.  websockets.serve() must run inside the gateway's
        existing asyncio loop.
        """
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

        # Close active client connection.
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        # Close the server.
        if self._ws_server is not None:
            self._ws_server.close()
            # Wait for the server to finish closing.
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
        """Send a message to the connected PWA client."""
        if not self._ws or not self._authenticated:
            return SendResult(success=False, error="No authenticated client connected")

        # When called as a final send after streaming, this is the
        # stream_done message.  When called without prior streaming,
        # this is a standalone response.
        if self._streaming_msg_id is not None:
            msg = stream_done(self._streaming_msg_id, content)
            self._streaming_msg_id = None
            self._streaming_text = ""
        else:
            # Standalone (non-streaming) response.
            msg_id = str(int(time.time() * 1000))
            msg = stream_done(msg_id, content)

        try:
            await self._ws.send(msg)
            return SendResult(success=True)
        except ConnectionClosed:
            self._ws = None
            self._authenticated = False
            return SendResult(success=False, error="WebSocket closed")

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": "PWA Mobile", "type": "dm", "chat_id": "pwa"}

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

        # Send current state: ready with hot message buffer.
        await ws.send(ready(self._hot_messages))
        self._authenticated = True
        return True

    # ------------------------------------------------------------------
    # WebSocket connection handler
    # ------------------------------------------------------------------

    async def _handle_connection(self, ws: Any) -> None:
        """Handle a single WebSocket connection lifecycle.

        Only one client is allowed at a time (single-user design).
        If a new client connects while one is active, the old one is
        closed.
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
                data.get("limit", 20),
            )

        else:
            logger.debug("PWA unknown message type: %s", msg_type)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def _handle_user_message(self, text: str) -> None:
        """Handle a user text message: create MessageEvent and route to agent."""
        if not text.strip():
            return

        source = SessionSource(
            platform=self.platform,
            chat_id="pwa",
            chat_name="PWA Mobile",
            chat_type="dm",
            user_id="pwa-user",
            user_name="User",
        )

        event = MessageEvent(
            text=text.strip(),
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(int(time.time() * 1000)),
            timestamp=datetime.now(timezone.utc),
        )

        # Store user message in hot buffer.
        self._hot_messages.append({
            "role": "user",
            "text": text.strip(),
            "timestamp": event.timestamp.isoformat(),
        })
        if len(self._hot_messages) > 50:
            self._hot_messages = self._hot_messages[-50:]

        # The gateway will call back into adapter.send() when the agent
        # produces a response.  The base class stores the final response
        # in the hot buffer via _add_assistant_message() which we need
        # to hook.
        await self.handle_message(event)

    async def _handle_search(self, query: str) -> None:
        """Search messages using the gateway's SessionDB (FTS5).

        For now this is a stub — full search implementation requires
        access to the SessionDB which the adapter gets via the gateway
        context.
        """
        if not query.strip():
            return
        ws = self._ws
        if ws is None:
            return

        # TODO: wire SessionDB access through gateway hooks.
        # For now, search only the in-memory hot buffer.
        results: list[dict[str, Any]] = []
        q = query.lower()
        for msg in self._hot_messages:
            if q in msg.get("text", "").lower():
                results.append(msg)

        await ws.send(search_results(results))

    async def _handle_load_history(
        self,
        before_msg_id: Optional[str],
        around_msg_id: Optional[str],
        limit: int,
    ) -> None:
        """Load history from hot buffer. SessionDB cold-data loading
        will be added when the SessionDB access hook is available.
        """
        ws = self._ws
        if ws is None:
            return

        # For now, return the hot buffer.
        await ws.send(json.dumps({
            "type": "history",
            "messages": self._hot_messages[-limit:] if limit > 0 else self._hot_messages,
        }))

    # ------------------------------------------------------------------
    # Streaming (stub — Phase 8)
    # ------------------------------------------------------------------

    async def _on_stream_delta(self, text: str) -> None:
        """Called by the gateway stream consumer to push a token to PWA."""
        ws = self._ws
        if ws is None or not self._authenticated:
            return

        if self._streaming_msg_id is None:
            self._streaming_msg_id = str(int(time.time() * 1000))
            self._streaming_text = ""
            await ws.send(stream_start(self._streaming_msg_id))

        self._streaming_text += text
        try:
            await ws.send(stream_delta(self._streaming_msg_id, text))
        except ConnectionClosed:
            self._streaming_text = ""  # Will be recovered on reconnect
            self._ws = None
            self._authenticated = False

    # ------------------------------------------------------------------
    # Gateway hooks
    # ------------------------------------------------------------------

    async def _after_turn(self, response_text: str) -> None:
        """Hook called after an agent turn completes.
        Stashes the assistant message in the hot buffer.
        """
        self._hot_messages.append({
            "role": "assistant",
            "text": response_text,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        if len(self._hot_messages) > 50:
            self._hot_messages = self._hot_messages[-50:]


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
