from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    openai_api_key: str | None
    openai_model: str
    database_path: str
    reminder_lookahead_minutes: int
    allowed_chat_ids: frozenset[int]
    allowed_user_ids: frozenset[int]


def load_settings() -> Settings:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required in .env")

    lookahead_raw = os.getenv("REMINDER_LOOKAHEAD_MINUTES", "10").strip()
    try:
        lookahead = max(1, int(lookahead_raw))
    except ValueError:
        lookahead = 10

    return Settings(
        telegram_bot_token=token,
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip() or None,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.2").strip() or "gpt-5.2",
        database_path=os.getenv("DATABASE_PATH", "family_planner.sqlite3").strip()
        or "family_planner.sqlite3",
        reminder_lookahead_minutes=lookahead,
        allowed_chat_ids=_parse_int_set(os.getenv("ALLOWED_CHAT_IDS", "")),
        allowed_user_ids=_parse_int_set(os.getenv("ALLOWED_USER_IDS", "")),
    )


def _parse_int_set(raw: str) -> frozenset[int]:
    values: set[int] = set()
    for part in raw.replace(";", ",").replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.add(int(part))
        except ValueError:
            raise RuntimeError(f"Invalid numeric id in allowlist: {part}") from None
    return frozenset(values)
