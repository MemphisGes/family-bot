from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from .db import Item


KIND_LABELS = {
    "event": "🗓️ Календарь",
    "booking": "📌 Брони",
    "task": "✅ Задачи",
    "shopping": "🛒 Покупки",
    "marketplace": "📦 Маркетплейсы",
    "wishlist": "🎁 Вишлисты",
    "expense": "💳 Финансы",
    "menu": "🍽️ Меню",
    "note": "📝 Заметки",
}


def format_dt(value: str | None) -> str:
    if not value:
        return "без даты"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    return dt.strftime("%d.%m %H:%M")


def format_item(item: Item) -> str:
    when = format_dt(item.starts_at or item.due_at)
    person = f" [{item.person}]" if item.person else ""
    recurrence = f" ({item.recurrence})" if item.recurrence else ""
    amount = f" - {item.amount:g}" if item.amount is not None else ""
    category = f" #{item.category}" if item.category else ""
    return f"#{item.id} {when}{person}: {item.title}{amount}{category}{recurrence}"


def render_items(title: str, items: list[Item]) -> str:
    if not items:
        return f"{title}\nПока ничего нет."

    groups: dict[str, list[Item]] = defaultdict(list)
    for item in items:
        groups[item.kind].append(item)

    lines = [title]
    for kind, kind_items in groups.items():
        lines.append("")
        lines.append(KIND_LABELS.get(kind, kind.title()))
        lines.extend(format_item(item) for item in kind_items)
    return "\n".join(lines)


def render_context(items: list[Item]) -> str:
    if not items:
        return "В семейном планере пока нет ближайших записей."
    return "\n".join(format_item(item) for item in items[:60])
