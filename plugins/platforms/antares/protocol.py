"""
Message protocol helpers for Hermes <-> Antares Bot communication.

All messages are JSON bodies sent over RabbitMQ topic exchanges.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def build_send_payload(
    chat_id: str,
    text: str,
    reply_to: Optional[str] = None,
    parse_mode: str = "markdown",
    correlation_id: Optional[str] = None,
) -> dict:
    payload = {
        "action": "send",
        "chat_id": chat_id,
        "text": text,
        "reply_to_message_id": reply_to,
        "parse_mode": parse_mode,
    }
    if correlation_id:
        payload["correlation_id"] = correlation_id
    return payload


def build_typing_payload(chat_id: str, typing: bool = True) -> dict:
    return {
        "action": "typing",
        "chat_id": chat_id,
        "typing": typing,
    }


def build_image_payload(
    chat_id: str,
    image_url: str,
    caption: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> dict:
    return {
        "action": "image",
        "chat_id": chat_id,
        "image_url": image_url,
        "caption": caption,
        "reply_to_message_id": reply_to,
    }


def build_document_payload(
    chat_id: str,
    url: str,
    file_name: Optional[str] = None,
    caption: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> dict:
    return {
        "action": "document",
        "chat_id": chat_id,
        "url": url,
        "file_name": file_name,
        "caption": caption,
        "reply_to_message_id": reply_to,
    }


def build_clarify_payload(
    chat_id: str,
    clarify_id: str,
    question: str,
    choices: List[str],
    reply_to: Optional[str] = None,
) -> dict:
    """Build a clarify prompt for inline keyboard rendering by Alice.

    When *choices* is non-empty, Alice should render Telegram
    ``InlineKeyboardMarkup`` buttons — one per choice (numbered 0..N-1)
    plus a final ``✏️ Other (type answer)`` button that enters text-capture
    mode.  Open-ended clarifies (empty choices) are rendered by the base
    adapter as plain text with text-capture, and do not use this payload.
    """
    return {
        "action": "clarify",
        "chat_id": chat_id,
        "clarify_id": clarify_id,
        "question": question,
        "choices": list(choices) if choices else [],
        "reply_to_message_id": reply_to,
    }


def build_edit_payload(
    chat_id: str,
    message_id: str,
    text: str,
) -> dict:
    return {
        "action": "edit",
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }


def build_delete_payload(
    chat_id: str,
    message_id: str,
) -> dict:
    return {
        "action": "delete",
        "chat_id": chat_id,
        "message_id": message_id,
    }


def parse_incoming_message(data: dict) -> Dict[str, Any]:
    """Parse a raw JSON dict from RabbitMQ into a normalized message dict.

    Returns dict with keys: chat_id, chat_type, chat_name, user_id,
    user_name, message_id, reply_to_message_id, text, media.
    """
    return {
        "chat_id": str(data.get("chat_id", "")),
        "chat_type": data.get("chat_type", "dm"),
        "chat_name": data.get("chat_name", ""),
        "user_id": str(data.get("user_id", "")),
        "user_name": data.get("user_name", ""),
        "message_id": str(data.get("message_id", "")),
        "reply_to_message_id": data.get("reply_to_message_id"),
        "text": data.get("text", ""),
        "media": data.get("media", []),
    }


def extract_media_urls(media: List[dict]) -> list[str]:
    return [m["url"] for m in media if "url" in m]


def extract_media_types(media: List[dict]) -> list[str]:
    return [m["type"] for m in media if "type" in m]
