from __future__ import annotations

import json
import re
from typing import Any

from openai import OpenAI


class FamilyAI:
    def __init__(self, api_key: str | None, model: str) -> None:
        self.api_key = api_key
        self.model = model
        self.client = OpenAI(api_key=api_key) if api_key else None

    def is_enabled(self) -> bool:
        return self.client is not None

    def answer(self, question: str, planner_context: str) -> str:
        if not self.client:
            return "AI не настроен. Добавьте OPENAI_API_KEY в .env и перезапустите бота."

        response = self.client.responses.create(
            model=self.model,
            instructions=(
                "Ты семейный помощник в Telegram. Отвечай по-русски, кратко и практично. "
                "Используй только данные из семейного планера, если вопрос касается расписания, "
                "дел, покупок, финансов, меню или напоминаний. Если данных не хватает, скажи что именно уточнить."
            ),
            input=f"Семейный планер:\n{planner_context}\n\nВопрос:\n{question}",
        )
        return response.output_text.strip()

    def parse_family_entry(
        self,
        text: str,
        members: list[dict[str, str | None]],
        today: str,
    ) -> dict[str, Any]:
        if not self.client:
            return {
                "flow": None,
                "clarification": "AI не настроен. Добавьте OPENAI_API_KEY в .env и перезапустите бота.",
            }

        member_lines = []
        for member in members:
            parts = [member.get("name")]
            if member.get("username"):
                parts.append(f"@{member['username']}")
            if member.get("role"):
                parts.append(member["role"])
            member_lines.append(" / ".join(part for part in parts if part))
        members_text = "\n".join(f"- {line}" for line in member_lines) or "- семья"

        response = self.client.responses.create(
            model=self.model,
            instructions=(
                "Ты парсер семейного Telegram-планера. Верни только JSON без Markdown. "
                "Определи тип записи и поля для конструктора. "
                "Поддержанные flow: event, task, shopping, marketplace, wishlist, reminder. "
                "date заполняй в формате YYYY-MM-DD HH:MM, если дата нужна. "
                "person выбирай из списка членов семьи по смыслу; если человек не указан, используй семья. "
                "recurrence допустим только daily, weekly, monthly или null. "
                "reminder допустим только 10m, 1h, 1d или null. "
                "Если записи не хватает обязательных данных, верни flow null и clarification на русском. "
                "Не выдумывай ссылки, цены и детали, которых нет в тексте."
            ),
            input=(
                f"Сегодня: {today}\n"
                f"Члены семьи:\n{members_text}\n\n"
                "Схема ответа:\n"
                "{\"flow\":\"event|task|shopping|marketplace|wishlist|reminder|null\","
                "\"date\":\"YYYY-MM-DD HH:MM|null\","
                "\"person\":\"имя|null\","
                "\"title\":\"что добавить|null\","
                "\"category\":\"категория|null\","
                "\"recurrence\":\"daily|weekly|monthly|null\","
                "\"reminder\":\"10m|1h|1d|null\","
                "\"notes\":\"комментарий|null\","
                "\"clarification\":\"вопрос|null\"}\n\n"
                f"Текст пользователя: {text}"
            ),
        )
        return self._extract_json(response.output_text)

    @staticmethod
    def _extract_json(text: str) -> dict[str, Any]:
        cleaned = text.strip()
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", cleaned, re.DOTALL)
        if fenced:
            cleaned = fenced.group(1)
        else:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start >= 0 and end > start:
                cleaned = cleaned[start : end + 1]
        data = json.loads(cleaned)
        if not isinstance(data, dict):
            raise ValueError("AI response is not a JSON object")
        return data
