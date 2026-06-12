"""
Antares Bridge Adapter for Hermes Agent.

Connects Hermes to the Antares Telegram bot via public RabbitMQ, enabling
remote Telegram messaging without direct Telegram API access on the Hermes
host.

RabbitMQ exchanges:
  - alice (topic) — incoming Bot -> Hermes messages on routing key "alice.hermes"
  - hermes (topic) — outgoing Hermes -> Bot messages on routing key "hermes.alice"

Configuration via config.yaml ``extra`` or environment variables:

    gateway:
      platforms:
        antares:
          enabled: true
          extra:
            rabbitmq_host: "mq.example.com"
            rabbitmq_port: 5671
            rabbitmq_user: "hermes"
            rabbitmq_pass: "secret"
            rabbitmq_vhost: "/"
            rabbitmq_cafile: "/path/to/ca.pem"
            rabbitmq_certfile: "/path/to/client.crt"
            rabbitmq_keyfile: "/path/to/client.key"

Env vars (override config.yaml): ANTARES_RABBITMQ_HOST, ANTARES_RABBITMQ_PORT,
ANTARES_RABBITMQ_USER, ANTARES_RABBITMQ_PASS, ANTARES_RABBITMQ_VHOST,
ANTARES_RABBITMQ_CAFILE, ANTARES_RABBITMQ_CERTFILE, ANTARES_RABBITMQ_KEYFILE.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import uuid
from typing import Any, Dict, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource
from gateway.config import PlatformConfig, Platform

from .protocol import (
    build_send_payload,
    build_typing_payload,
    build_image_payload,
    build_document_payload,
    build_edit_payload,
    build_delete_payload,
    build_clarify_payload,
    extract_media_urls,
    extract_media_types,
    parse_incoming_message,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    """Verify aio_pika is installed."""
    try:
        import aio_pika  # noqa: F401

        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Antares Bridge Adapter
# ---------------------------------------------------------------------------


class AntaresBridgeAdapter(BasePlatformAdapter):
    """Async RabbitMQ bridge adapter for the Antares Telegram bot."""

    def __init__(self, config: PlatformConfig, **kwargs):
        platform = Platform("antares")
        super().__init__(config=config, platform=platform)

        self._connection = None  # aio_pika.RobustConnection
        self._channel = None  # aio_pika.RobustChannel
        self._consumer_tag = None  # Tag for unsubscribing

        # Known chat ids tracked from incoming messages
        self._known_chat_ids: set[str] = set()
        # Cached chat info: chat_id -> {name, type}
        self._chat_info_cache: Dict[str, dict] = {}

        # Pending correlation → Future for message_ack responses from Alice.
        # Used to retrieve the real Telegram message_id for stream editing
        # and tool-progress editing to work.
        self._pending_acks: Dict[str, asyncio.Future] = {}
        self._ack_timeout: float = 15.0

    # -----------------------------------------------------------------------
    # Connection lifecycle
    # -----------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to RabbitMQ and set up topic exchanges."""
        import aio_pika
        from aio_pika import ExchangeType

        extra = self.config.extra or {}

        host = extra.get("rabbitmq_host") or os.getenv(
            "ANTARES_RABBITMQ_HOST", "127.0.0.1"
        )
        port = int(
            extra.get("rabbitmq_port") or os.getenv("ANTARES_RABBITMQ_PORT", "5671")
        )
        user = extra.get("rabbitmq_user") or os.getenv("ANTARES_RABBITMQ_USER", "")
        password = extra.get("rabbitmq_pass") or os.getenv("ANTARES_RABBITMQ_PASS", "")
        vhost = extra.get("rabbitmq_vhost") or os.getenv("ANTARES_RABBITMQ_VHOST", "/")
        cafile = extra.get("rabbitmq_cafile") or os.getenv(
            "ANTARES_RABBITMQ_CAFILE", ""
        )
        certfile = extra.get("rabbitmq_certfile") or os.getenv(
            "ANTARES_RABBITMQ_CERTFILE", ""
        )
        keyfile = extra.get("rabbitmq_keyfile") or os.getenv(
            "ANTARES_RABBITMQ_KEYFILE", ""
        )

        is_tls = bool(cafile and certfile and keyfile)

        connect_kw: dict = {"host": host, "port": port}
        if vhost and vhost != "/":
            connect_kw["virtualhost"] = vhost
        if user:
            connect_kw["login"] = user
            connect_kw["password"] = password

        if is_tls:
            context = ssl.create_default_context(cafile=cafile)
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
            context.verify_flags &= ~ssl.VERIFY_X509_STRICT
            connect_kw["ssl"] = True
            connect_kw["ssl_context"] = context

        self._connection = await aio_pika.connect_robust(**connect_kw)
        self._channel = await self._connection.channel()

        # Incoming: subscribe to alice.hermes
        alice_exchange = await self._channel.declare_exchange(
            "alice", ExchangeType.TOPIC
        )
        queue = await self._channel.declare_queue("", exclusive=True)
        await queue.bind(alice_exchange, routing_key="alice.hermes")
        self._consumer_tag = await queue.consume(self._on_rabbitmq_message)

        # Outgoing: declare hermes exchange
        await self._channel.declare_exchange("hermes", ExchangeType.TOPIC)

        self._running = True
        logger.info("[antares] Connected to RabbitMQ at %s:%s", host, port)
        return True

    async def disconnect(self) -> None:
        """Close RabbitMQ channel and connection."""
        self._running = False

        # Cancel all pending acks so no _publish() caller hangs
        # waiting for a message_ack that will never arrive.
        for future in self._pending_acks.values():
            if not future.done():
                future.cancel()
        self._pending_acks.clear()

        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                logger.debug("[antares] Error closing channel", exc_info=True)

        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                logger.debug("[antares] Error closing connection", exc_info=True)

        self._channel = None
        self._connection = None

    # -----------------------------------------------------------------------
    # Incoming message handler
    # -----------------------------------------------------------------------

    async def _on_rabbitmq_message(self, message: "aio_pika.IncomingMessage") -> None:
        """Process an incoming message from RabbitMQ."""
        import aio_pika

        async with message.process():
            try:
                data = json.loads(message.body.decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning("[antares] Invalid JSON from RabbitMQ")
                return

            action = data.get("action")

            # Handle clarify callback responses from Alice
            if action == "clarify_response":
                await self._handle_clarify_response(data)
                return

            # Handle message_ack: Alice confirms a send/edit with the
            # real Telegram message_id. Resolves the pending Future so
            # _publish() returns a proper SendResult with message_id.
            if action == "message_ack":
                correlation_id = data.get("correlation_id")
                message_id = data.get("message_id")
                if correlation_id and correlation_id in self._pending_acks:
                    future = self._pending_acks.pop(correlation_id)
                    if not future.done():
                        future.set_result(str(message_id))
                return

            if action != "new_message":
                return

            parsed = parse_incoming_message(data)
            if not parsed["chat_id"] or not parsed["user_id"]:
                logger.warning("[antares] Missing chat_id or user_id in message")
                return

            # Build session source
            source = self.build_source(
                chat_id=parsed["chat_id"],
                chat_type=parsed["chat_type"],
                chat_name=parsed["chat_name"],
                user_id=parsed["user_id"],
                user_name=parsed["user_name"],
                message_id=parsed["message_id"],
            )

            # Detect message type
            text = parsed["text"]
            if text.startswith("/"):
                message_type = MessageType.COMMAND
            elif parsed["media"]:
                first_media = parsed["media"][0]
                media_type = first_media.get("type", "")
                if media_type == "photo":
                    message_type = MessageType.PHOTO
                elif media_type in ("video", "animation"):
                    message_type = MessageType.VIDEO
                elif media_type in ("audio", "voice"):
                    message_type = MessageType.VOICE
                elif media_type == "document":
                    message_type = MessageType.DOCUMENT
                elif media_type == "sticker":
                    message_type = MessageType.STICKER
                else:
                    message_type = MessageType.TEXT
            else:
                message_type = MessageType.TEXT

            # Build MessageEvent
            event = MessageEvent(
                text=text,
                message_type=message_type,
                source=source,
                message_id=parsed["message_id"],
                reply_to_message_id=parsed["reply_to_message_id"],
                media_urls=extract_media_urls(parsed["media"]),
                media_types=extract_media_types(parsed["media"]),
            )

            # Cache chat info
            chat_id_key = parsed["chat_id"]
            self._known_chat_ids.add(chat_id_key)
            self._chat_info_cache[chat_id_key] = {
                "name": parsed["chat_name"],
                "type": parsed["chat_type"],
            }

            # Dispatch to Hermes agent
            await self.handle_message(event)

    # -----------------------------------------------------------------------
    # Sending messages
    # -----------------------------------------------------------------------

    async def _publish(
        self, payload: dict, *, action_name: str = "message"
    ) -> SendResult:
        """Publish a JSON payload to the hermes.alice routing key.

        For ``send`` actions, appends a correlation_id and awaits a
        ``message_ack`` response from Alice to retrieve the real Telegram
        message_id. Other actions (typing, delete, ...) are fire-and-forget.
        """
        if not self._channel:
            return SendResult(success=False, error="Not connected")

        # Only ``send`` needs a message_id back from Alice so the gateway
        # can track it for progressive editing (streaming, tool-progress).
        if action_name in ("send",):
            return await self._publish_with_ack(payload, action_name)
        return await self._publish_fire_and_forget(payload, action_name)

    async def _publish_fire_and_forget(
        self, payload: dict, action_name: str
    ) -> SendResult:
        """Publish without waiting for a response (typing, delete, etc.)."""
        try:
            import aio_pika

            exchange = await self._channel.get_exchange("hermes")
            await exchange.publish(
                aio_pika.Message(body=json.dumps(payload).encode()),
                routing_key="hermes.alice",
            )
            return SendResult(success=True)
        except Exception as e:
            logger.error("[antares] Failed to publish %s: %s", action_name, e)
            return SendResult(success=False, error=str(e))

    async def _publish_with_ack(
        self, payload: dict, action_name: str
    ) -> SendResult:
        """Publish and await a ``message_ack`` for the real Telegram message_id."""
        correlation_id = uuid.uuid4().hex
        payload["correlation_id"] = correlation_id
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_acks[correlation_id] = future

        try:
            import aio_pika

            exchange = await self._channel.get_exchange("hermes")
            await exchange.publish(
                aio_pika.Message(body=json.dumps(payload).encode()),
                routing_key="hermes.alice",
            )

            message_id = await asyncio.wait_for(future, timeout=self._ack_timeout)
            return SendResult(success=True, message_id=message_id)
        except asyncio.TimeoutError:
            logger.debug(
                "[antares] message_ack timeout for %s (cid=%s), falling back",
                action_name, correlation_id,
            )
            self._pending_acks.pop(correlation_id, None)
            return SendResult(success=True)  # graceful: edit disabled but msg sent
        except asyncio.CancelledError:
            self._pending_acks.pop(correlation_id, None)
            if not future.done():
                future.cancel()
            raise
        except Exception as e:
            logger.error("[antares] Failed to publish %s: %s", action_name, e)
            self._pending_acks.pop(correlation_id, None)
            if not future.done():
                future.cancel()
            return SendResult(success=False, error=str(e))

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a chat via RabbitMQ."""
        payload = build_send_payload(chat_id, content, reply_to)
        return await self._publish(payload, action_name="send")

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a typing indicator."""
        payload = build_typing_payload(chat_id, typing=True)
        await self._publish(payload, action_name="typing")

    async def stop_typing(self, chat_id: str) -> None:
        """Stop the typing indicator."""
        if not self._channel:
            return
        try:
            import aio_pika

            payload = build_typing_payload(chat_id, typing=False)
            exchange = await self._channel.get_exchange("hermes")
            await exchange.publish(
                aio_pika.Message(body=json.dumps(payload).encode()),
                routing_key="hermes.alice",
            )
        except Exception:
            pass

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image via the bridge."""
        payload = build_image_payload(chat_id, image_url, caption, reply_to)
        return await self._publish(payload, action_name="image")

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Send a document via the bridge.

        Note: file_path should be a publicly accessible URL for the remote
        bot to download. The bridge itself does not handle file uploads.
        """
        payload = build_document_payload(
            chat_id, file_path, file_name, caption, reply_to
        )
        return await self._publish(payload, action_name="document")

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent message."""
        if not self._channel:
            return SendResult(success=False, error="Not connected")
        payload = build_edit_payload(chat_id, message_id, content)
        return await self._publish(payload, action_name="edit")

    async def delete_message(
        self,
        chat_id: str,
        message_id: str,
    ) -> bool:
        """Delete a message."""
        if not self._channel:
            return False
        try:
            import aio_pika

            payload = build_delete_payload(chat_id, message_id)
            exchange = await self._channel.get_exchange("hermes")
            await exchange.publish(
                aio_pika.Message(body=json.dumps(payload).encode()),
                routing_key="hermes.alice",
            )
            return True
        except Exception:
            logger.debug("[antares] Failed to delete message", exc_info=True)
            return False

    # -----------------------------------------------------------------------
    # Clarify tool support (inline keyboard via bridge protocol)
    # -----------------------------------------------------------------------

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices,
        clarify_id: str,
        session_key: str,
        metadata=None,
    ) -> SendResult:
        """Render a clarify prompt.

        Multi-choice mode (choices non-empty): sends a structured
        ``action: clarify`` payload so Alice can render inline keyboard
        buttons.  Open-ended mode (choices empty): falls back to the base
        adapter's plain-text + text-capture path.
        """
        if choices:
            payload = build_clarify_payload(
                chat_id=chat_id,
                clarify_id=clarify_id,
                question=question,
                choices=list(choices),
                reply_to=None,
            )
            return await self._publish(payload, action_name="clarify")
        else:
            return await super().send_clarify(
                chat_id=chat_id,
                question=question,
                choices=choices,
                clarify_id=clarify_id,
                session_key=session_key,
                metadata=metadata,
            )

    async def _handle_clarify_response(self, data: dict) -> None:
        """Resolve a pending clarify from an Alice callback.

        Alice sends ``action: clarify_response`` when the user taps an
        inline keyboard button (``choice`` = 0-based index) or types a
        free-form answer after the \"Other\" button (``choice`` = \"other\",
        ``text`` = the typed answer).

        Text-capture mode (open-ended clarifies) is handled by the gateway's
        text-intercept via ``mark_awaiting_text`` and does not use this path.
        """
        from tools.clarify_gateway import resolve_gateway_clarify, mark_awaiting_text

        clarify_id = data.get("clarify_id")
        if not clarify_id:
            logger.warning("[antares] clarify_response missing clarify_id")
            return

        choice = data.get("choice")
        text = (data.get("text") or "").strip()

        if choice is not None and choice != "other":
            # Button press: Alice resolves the choice index to the label
            # text and passes it back as ``text``.  Use it directly.
            response = text if text else str(choice)
        elif choice == "other":
            # "Other" button tapped — flip into text-capture mode so the
            # next user message in this session is intercepted.
            mark_awaiting_text(clarify_id)
            return
        else:
            # Plain text response (open-ended fallback via bridge)
            response = text

        if response:
            resolved = resolve_gateway_clarify(clarify_id, response)
            if resolved:
                logger.info(
                    "[antares] Resolved clarify %s via bridge response",
                    clarify_id,
                )

    # -----------------------------------------------------------------------
    # Chat info
    # -----------------------------------------------------------------------

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return cached chat info or a sensible default.

        Since this bridge does not connect directly to Telegram, chat info
        comes from metadata forwarded by the bot with each message.
        """
        if chat_id in self._chat_info_cache:
            return {
                **self._chat_info_cache[chat_id],
                "chat_id": chat_id,
            }

        is_group = chat_id.startswith("-")
        return {
            "name": chat_id,
            "type": "group" if is_group else "dm",
            "chat_id": chat_id,
        }


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="antares",
        label="Antares Bridge",
        adapter_factory=lambda cfg: AntaresBridgeAdapter(cfg),
        check_fn=check_requirements,
        required_env=[
            "ANTARES_RABBITMQ_HOST",
            "ANTARES_RABBITMQ_CAFILE",
            "ANTARES_RABBITMQ_CERTFILE",
            "ANTARES_RABBITMQ_KEYFILE",
        ],
        install_hint="pip install aio-pika",
        allowed_users_env="ANTARES_ALLOWED_USERS",
        allow_all_env="ANTARES_ALLOW_ALL_USERS",
        max_message_length=4096,
        emoji="🔗",
        platform_hint=(
            "You are chatting via Telegram (through the Antares Bridge). "
            "You receive messages forwarded from a Telegram bot. "
            "Support MarkdownV2 formatting. Messages are limited to ~4096 chars."
        ),
    )
