from __future__ import annotations

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
