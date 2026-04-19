from __future__ import annotations

from datetime import datetime

import dateparser


RECURRENCES = {"daily", "weekly", "monthly"}


def parse_datetime(text: str) -> datetime | None:
    return dateparser.parse(
        text.strip(),
        languages=["ru", "en"],
        settings={
            "PREFER_DATES_FROM": "future",
            "RELATIVE_BASE": datetime.now(),
            "RETURN_AS_TIMEZONE_AWARE": False,
        },
    )


def split_parts(text: str, min_parts: int) -> list[str]:
    parts = [part.strip() for part in text.split("|")]
    if len(parts) < min_parts or any(not part for part in parts[:min_parts]):
        raise ValueError
    return parts


def parse_amount(text: str) -> float:
    normalized = text.replace(" ", "").replace(",", ".")
    return float(normalized)


def parse_recurrence(parts: list[str]) -> tuple[list[str], str | None]:
    if parts and parts[-1].lower() in RECURRENCES:
        return parts[:-1], parts[-1].lower()
    return parts, None
