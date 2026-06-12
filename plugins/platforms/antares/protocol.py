"""
Message protocol helpers for Hermes <-> Antares Bot communication.

All messages are JSON bodies sent over RabbitMQ topic exchanges.
"""

from __future__ import annotations

import base64
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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
    urls = []
    for m in media:
        if "data" in m:
            # base64-encoded media — caller should use process_incoming_media()
            # to decode and cache; here we return a placeholder that gets
            # replaced later.  Keep for backward compat with URL-based media.
            urls.append("")
        elif "url" in m:
            urls.append(m["url"])
        else:
            urls.append("")
    return urls


def extract_media_types(media: List[dict]) -> list[str]:
    return [m["type"] for m in media if "type" in m]


_MIME_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/mp4": ".m4a",
    "audio/wav": ".wav",
    "application/pdf": ".pdf",
    "application/zip": ".zip",
    "application/json": ".json",
    "text/plain": ".txt",
    "text/csv": ".csv",
    "text/html": ".html",
}


def _ext_for_mime(mime_type: str) -> str:
    return _MIME_TO_EXT.get(mime_type, "")


def process_incoming_media(
    media: List[dict], cache_dir: str
) -> tuple:
    """Decode base64-encoded media and save to cache files.

    Returns (urls, types) where urls are local file paths usable by
    the hermes agent's tools (vision_analyze, read_file, etc).
    """
    urls = []
    types = []
    os.makedirs(cache_dir, exist_ok=True)

    for m in media:
        media_type = m.get("type", "")
        types.append(media_type)

        if "data" in m:
            try:
                data_bytes = base64.b64decode(m["data"])
            except Exception as e:
                logger.warning(
                    "[antares] Failed to decode incoming base64 media: %s", e
                )
                urls.append("")
                continue

            filename = m.get("filename", "media")
            ext = os.path.splitext(filename)[1]
            if not ext:
                mime_type = m.get("mime_type", "")
                ext = _ext_for_mime(mime_type)
            if not ext:
                ext = ".bin"
            try:
                fd, path = tempfile.mkstemp(
                    suffix=ext, prefix="antares_", dir=cache_dir
                )
                with os.fdopen(fd, "wb") as f:
                    f.write(data_bytes)
                urls.append(path)
                logger.debug(
                    "[antares] Cached incoming media: type=%s size=%d path=%s",
                    media_type, len(data_bytes), path,
                )
            except OSError as e:
                logger.error(
                    "[antares] Failed to write incoming media to cache: %s", e
                )
                urls.append("")
        elif "url" in m:
            urls.append(m["url"])
        else:
            urls.append("")

    return urls, types
