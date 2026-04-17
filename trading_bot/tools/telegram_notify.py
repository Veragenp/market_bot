"""
Универсальная отправка сообщений в Telegram (одна точка входа для бота и джобов).

Токен и чат: TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID (или TELEGRAM_TOKEN + TELEGRAM_CHAT_ID).

Паттерн как в tutorial_v3 (TelegramClient-синглтон): один экземпляр на процесс и общий
HTTP Session; без python-telegram-bot и polling — только sendMessage для уведомлений.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_API = "https://api.telegram.org"

_notifier_lock = threading.Lock()
_notifier_instance: Optional["TelegramNotifier"] = None


def _resolve_token() -> str:
    t = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or "").strip()
    return t


def _resolve_chat_id() -> str:
    c = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()
    return c


def _post_send(
    tok: str,
    cid: str,
    text: str,
    *,
    parse_mode: Optional[str],
    timeout: float,
) -> bool:
    if not tok or not cid:
        logger.warning("Telegram: skip send (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
        return False
    if not text or not str(text).strip():
        logger.warning("Telegram: skip send (empty text)")
        return False
    url = f"{DEFAULT_API}/bot{tok}/sendMessage"
    payload: dict = {"chat_id": cid, "text": str(text)}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        data = r.json() if r.content else {}
        if r.status_code == 200 and data.get("ok"):
            return True
        logger.error("Telegram sendMessage failed: status=%s body=%s", r.status_code, data)
        return False
    except Exception:
        logger.exception("Telegram sendMessage request failed")
        return False


class TelegramNotifier:
    """
    Ленивый синглтон: кэш токена/чата из env и один requests.Session на процесс.
    Для ops-уведомлений (structural и др.) без интерактивного бота.
    
    Важно: токены считываются при первом вызове send_message(), а не при инициализации,
    чтобы дать время settings.py загрузить .env файл.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._token_cached: Optional[str] = None
        self._chat_id_cached: Optional[str] = None

    def _ensure_tokens(self) -> None:
        """Считать токены из env (лениво, при первом использовании)."""
        if self._token_cached is None:
            self._token_cached = _resolve_token()
        if self._chat_id_cached is None:
            self._chat_id_cached = _resolve_chat_id()

    def send_message(
        self,
        text: str,
        *,
        parse_mode: Optional[str] = "HTML",
        timeout: float = 30.0,
    ) -> bool:
        # Считываем токены лениво - при первом вызове send_message
        self._ensure_tokens()
        
        if not self._token_cached or not self._chat_id_cached:
            logger.warning("Telegram: skip send (singleton: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
            return False
        if not text or not str(text).strip():
            logger.warning("Telegram: skip send (empty text)")
            return False
        url = f"{DEFAULT_API}/bot{self._token_cached}/sendMessage"
        payload: dict = {"chat_id": self._chat_id_cached, "text": str(text)}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            r = self._session.post(url, json=payload, timeout=timeout)
            data = r.json() if r.content else {}
            if r.status_code == 200 and data.get("ok"):
                return True
            logger.error("Telegram sendMessage failed: status=%s body=%s", r.status_code, data)
            return False
        except Exception:
            logger.exception("Telegram sendMessage request failed")
            return False


def get_telegram_notifier() -> TelegramNotifier:
    """Возвращает общий экземпляр TelegramNotifier (как TelegramClient в tutorial_v3)."""
    global _notifier_instance
    with _notifier_lock:
        if _notifier_instance is None:
            _notifier_instance = TelegramNotifier()
        return _notifier_instance


def send_telegram_message(
    text: str,
    *,
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = "HTML",
    timeout: float = 30.0,
) -> bool:
    """
    Отправляет текст в чат. Возвращает True при HTTP 200 и ok=true в ответе API.
    Если token/chat_id не переданы — используется синглтон с env.
    Явная пара token/chat_id (хотя бы одно не None) — разовый запрос без синглтона.
    """
    if token is not None or chat_id is not None:
        tok = (token or "").strip() or _resolve_token()
        cid = (chat_id or "").strip() or _resolve_chat_id()
        return _post_send(tok, cid, text, parse_mode=parse_mode, timeout=timeout)
    return get_telegram_notifier().send_message(text, parse_mode=parse_mode, timeout=timeout)


def escape_html_telegram(text: str) -> str:
    """Минимальное экранирование для parse_mode=HTML."""
    s = str(text)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


__all__ = ["TelegramNotifier", "escape_html_telegram", "get_telegram_notifier", "send_telegram_message"]
