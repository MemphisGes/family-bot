from __future__ import annotations

import logging
import asyncio
import tempfile
from io import BytesIO
from html import escape
from collections.abc import Awaitable, Callable
from datetime import datetime, time, timedelta
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import CallbackQueryHandler, Application, CommandHandler, ContextTypes, MessageHandler, filters

from .ai import FamilyAI
from .config import Settings, load_settings
from .db import Database
from .parsing import parse_amount, parse_datetime, parse_recurrence, split_parts
from .db import Item
from .rendering import KIND_LABELS, format_dt, render_context, render_items


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

NOTIFICATION_TITLES = {
    "event": "Новое событие",
    "booking": "Новая бронь",
    "task": "Новая задача",
}


MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["☀️ Сегодня", "📅 Неделя"],
        ["👤 Мои задачи", "✅ Все задачи"],
        ["🗓️ Событие", "✅ Задача", "🛒 Покупки"],
        ["🎁 Вишлист", "🔔 Напоминание"],
    ],
    resize_keyboard=True,
    input_field_placeholder="Выберите действие",
)

FLOW_PROMPTS = {
    "member": "Введите: Имя роль цвет\nПример: Мама parent #ff6b6b",
    "event": "Введите: дата | кто | что | категория | daily/weekly/monthly\nПример: завтра 18:30 | Мама | врач | health",
    "booking": "Введите: дата | кто | что забронировали | детали\nПример: суббота 15:00 | семья | столик в кафе | 4 человека",
    "task": "Введите: дата | кто | что | категория | daily/weekly/monthly\nПример: пятница 19:00 | Папа | оплатить ЖКУ | finance",
    "reminder": "Введите: дата | кто | текст\nПример: завтра 09:00 | Мама | взять документы",
    "shopping": "Введите покупку.\nПример: молоко 2 л",
    "marketplace": "Введите покупку на маркетплейсе.\nПример: WB | кроссовки ребенку | 3200 | ссылка или комментарий",
    "wishlist": "Введите желание.\nПример: Мама | массажер для спины | день рождения | ссылка или комментарий",
    "expense": "Введите: дата | кто | сумма | категория | комментарий\nПример: завтра | семья | 4500 | ЖКУ | коммунальные платежи",
    "menu": "Введите: дата | прием пищи | блюдо\nПример: понедельник | ужин | паста и салат",
    "note": "Введите текст заметки.",
    "ai": "Введите вопрос AI-помощнику.\nПример: Что у нас завтра и что подготовить вечером?",
    "done": "Введите ID записи, которую нужно закрыть.\nПример: 12",
}

CONSTRUCTORS = {
    "event": [
        ("date", "Когда событие?\nПример: завтра 18:30"),
        ("person", "Для кого событие?\nПример: Мама"),
        ("title", "Что за событие?\nПример: врач"),
        ("category", "Раздел или категория? Напишите '-' если не нужно.\nПример: health"),
        ("recurrence", "Повторять? Напишите daily, weekly, monthly или '-'."),
        ("reminder", "Напомнить заранее? Напишите 10m, 1h, 1d или '-'."),
    ],
    "task": [
        ("date", "К какому сроку задача?\nПример: пятница 19:00"),
        ("person", "Кому задача?\nПример: Папа"),
        ("title", "Что нужно сделать?\nПример: оплатить ЖКУ"),
        ("category", "Раздел или категория? Напишите '-' если не нужно.\nПример: finance"),
        ("recurrence", "Повторять? Напишите daily, weekly, monthly или '-'."),
        ("reminder", "Напомнить заранее? Напишите 10m, 1h, 1d или '-'."),
    ],
    "shopping": [
        ("title", "Что купить?\nПример: молоко"),
        ("notes", "Количество, магазин или комментарий? Напишите '-' если не нужно.\nПример: 2 л"),
    ],
    "marketplace": [
        ("title", "Что купить на маркетплейсе?\nПример: кроссовки ребенку"),
        ("category", "Маркетплейс или категория? Напишите '-' если не нужно.\nПример: WB"),
        ("notes", "Ссылка, цена или комментарий? Напишите '-' если не нужно."),
    ],
    "wishlist": [
        ("person", "Чей вишлист?\nПример: Мама"),
        ("title", "Что добавить в вишлист?\nПример: массажер для спины"),
        ("category", "Повод или категория? Напишите '-' если не нужно.\nПример: день рождения"),
        ("notes", "Ссылка или комментарий? Напишите '-' если не нужно."),
    ],
    "reminder": [
        ("date", "Когда напомнить?\nПример: завтра 09:00"),
        ("person", "Кому напомнить?\nПример: Мама"),
        ("title", "О чем напомнить?\nПример: взять документы в школу"),
    ],
}

MENU_TO_FLOW = {
    "Семья": "member",
    "Событие": "event",
    "🗓️ Событие": "event",
    "Бронь": "booking",
    "Задача": "task",
    "✅ Задача": "task",
    "Покупки": "shopping",
    "🛒 Покупки": "shopping",
    "Напоминание": "reminder",
    "🔔 Напоминание": "reminder",
    "Покупка": "shopping",
    "Маркетплейс": "marketplace",
    "Вишлист": "wishlist",
    "🎁 Вишлист": "wishlist",
    "Финансы": "expense",
    "Меню": "menu",
    "Заметка": "note",
    "AI": "ai",
    "Закрыть ID": "done",
}


class FamilyPlannerBot:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.database_path)
        self.ai = FamilyAI(settings.openai_api_key, settings.openai_model)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "Семейный планер готов. Выберите действие в меню быстрого доступа.",
            reply_markup=MENU_KEYBOARD,
        )

    async def show_ids(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        user = update.effective_user
        lines = ["ID для настройки доступа:"]
        if chat:
            lines.append(f"chat_id: {chat.id}")
        if user:
            lines.append(f"user_id: {user.id}")
        lines.append("")
        lines.append("Добавьте нужные значения в ALLOWED_CHAT_IDS или ALLOWED_USER_IDS в .env.")
        await update.message.reply_text("\n".join(lines))

    async def menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data.pop("flow", None)
        context.user_data.pop("constructor", None)
        await update.message.reply_text(
            "Меню обновлено.",
            reply_markup=MENU_KEYBOARD,
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "Основной ввод теперь через меню быстрого доступа.\n\n"
            "Нажмите кнопку, затем отправьте данные по подсказке. "
            "Можно также написать фразу обычным текстом, например: завтра в 18:00 у Маши стоматолог, "
            "и AI соберет запись на подтверждение. "
            "Когда кто-то добавляет событие, бронь, задачу, покупку, маркетплейс, вишлист, финансы, меню или напоминание, бот отправляет уведомление в семейный чат.\n\n"
            "Команды оставлены как запасной режим: /today, /week, /digest, /export, /backup, /restore, /add, /ask, /done.",
            reply_markup=MENU_KEYBOARD,
        )

    async def add_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        args = context.args
        if not args:
            await update.message.reply_text("Формат: /member Мама parent #ff6b6b\nИли /join Мама")
            return
        name = args[0]
        role = args[1] if len(args) > 1 else None
        color = args[2] if len(args) > 2 else None
        await self._save_member(update, chat_id, name, role, color)

    async def join_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        user = update.effective_user
        if not user:
            await update.message.reply_text("Не вижу Telegram-пользователя в сообщении.")
            return
        name = " ".join(context.args).strip() or user.full_name or user.username or str(user.id)
        self.db.add_member(
            chat_id,
            name,
            role=None,
            color=None,
            telegram_user_id=user.id,
            username=user.username,
            mention=user.mention_html(),
        )
        await update.message.reply_html(
            f"Добавлен член семьи: {user.mention_html()} как {name}",
            reply_markup=MENU_KEYBOARD,
        )

    async def members(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        rows = self.db.list_members(update.effective_chat.id)
        if not rows:
            await update.message.reply_text("Члены семьи пока не добавлены. Пример: /member Мама parent #ff6b6b")
            return
        lines = ["Семья:"]
        for row in rows:
            tg = f"@{row['username']}" if row["username"] else None
            details = " ".join(part for part in [tg, row["role"], row["color"]] if part)
            lines.append(f"- {row['name']} {details}".strip())
        await update.message.reply_text("\n".join(lines))

    async def add_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._add_dated_item(update, " ".join(context.args), "event", "событие")

    async def add_booking(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._add_dated_item(update, " ".join(context.args), "booking", "бронь")

    async def add_task(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._add_dated_item(update, " ".join(context.args), "task", "задача")

    async def _add_dated_item(
        self, update: Update, text: str, kind: str, label: str
    ) -> None:
        try:
            parts = split_parts(text, 3)
        except ValueError:
            await update.message.reply_text(
                f"Формат: /{kind} дата | кто | что | категория | daily/weekly/monthly"
            )
            return

        parts, recurrence = parse_recurrence(parts)
        when = parse_datetime(parts[0])
        if not when:
            await update.message.reply_text("Не понял дату. Пример: завтра 18:30")
            return

        person = parts[1]
        title = parts[2]
        category = parts[3] if len(parts) > 3 else None
        starts_at = when.isoformat(timespec="seconds") if kind in {"event", "booking"} else None
        due_at = when.isoformat(timespec="seconds") if kind == "task" else None
        item_id = self.db.add_item(
            update.effective_chat.id,
            kind,
            title,
            person=person,
            starts_at=starts_at,
            due_at=due_at,
            category=category,
            recurrence=recurrence,
        )
        await self._notify_family(
            update,
            self._build_notification(
                update,
                NOTIFICATION_TITLES.get(kind, f"Добавлено: {label}"),
                item_id,
                title,
                person,
                when,
                category,
            ),
        )
        await update.message.reply_text(f"Добавлено: {label} #{item_id}", reply_markup=MENU_KEYBOARD)

    async def add_buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text("Формат: /buy молоко 2 л")
            return
        await self._save_simple_item(
            update,
            "shopping",
            text,
            "Добавлено в покупки",
            notify_title="Новая покупка",
        )

    async def add_marketplace(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text("Формат: /market WB | кроссовки ребенку | 3200 | ссылка")
            return
        await self._save_simple_item(
            update,
            "marketplace",
            text,
            "Добавлено в покупки на маркетплейсах",
            notify_title="Новая покупка на маркетплейсе",
        )

    async def add_wishlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text("Формат: /wish Мама | массажер | день рождения | ссылка")
            return
        await self._save_simple_item(
            update,
            "wishlist",
            text,
            "Добавлено в вишлист",
            notify_title="Новое желание в семейном вишлисте",
        )

    async def _save_simple_item(
        self,
        update: Update,
        kind: str,
        text: str,
        reply_title: str,
        notify_title: str | None = None,
    ) -> int:
        item_id = self.db.add_item(update.effective_chat.id, kind, text)
        if notify_title:
            await self._notify_family(update, self._build_notification(update, notify_title, item_id, text))
        await update.effective_message.reply_text(f"{reply_title}: #{item_id}", reply_markup=MENU_KEYBOARD)
        return item_id

    async def add_expense(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._save_expense_from_text(update, " ".join(context.args))

    async def _save_expense_from_text(self, update: Update, text: str) -> None:
        try:
            parts = split_parts(text, 4)
            when = parse_datetime(parts[0])
            amount = parse_amount(parts[2])
        except (ValueError, TypeError):
            await update.message.reply_text("Формат: /expense дата | кто | сумма | категория | комментарий")
            return
        if not when:
            await update.message.reply_text("Не понял дату расхода или платежа.")
            return
        notes = parts[4] if len(parts) > 4 else None
        item_id = self.db.add_item(
            update.effective_chat.id,
            "expense",
            notes or parts[3],
            person=parts[1],
            due_at=when.isoformat(timespec="seconds"),
            amount=amount,
            category=parts[3],
        )
        await self._notify_family(
            update,
            self._build_notification(update, "Новая финансовая запись", item_id, notes or parts[3], parts[1], when, parts[3]),
        )
        await update.message.reply_text(f"Добавлено в финансы: #{item_id}", reply_markup=MENU_KEYBOARD)

    async def add_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._save_menu_from_text(update, " ".join(context.args))

    async def _save_menu_from_text(self, update: Update, text: str) -> None:
        try:
            parts = split_parts(text, 3)
            when = parse_datetime(parts[0])
        except ValueError:
            await update.message.reply_text("Формат: /menu дата | прием пищи | блюдо")
            return
        if not when:
            await update.message.reply_text("Не понял дату для меню.")
            return
        item_id = self.db.add_item(
            update.effective_chat.id,
            "menu",
            parts[2],
            starts_at=when.isoformat(timespec="seconds"),
            category=parts[1],
        )
        await self._notify_family(update, self._build_notification(update, "Новое меню", item_id, parts[2], None, when, parts[1]))
        await update.message.reply_text(f"Добавлено в меню: #{item_id}", reply_markup=MENU_KEYBOARD)

    async def add_note(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text("Формат: /note текст заметки")
            return
        await self._save_simple_item(
            update,
            "note",
            text,
            "Заметка сохранена",
            notify_title="Новая семейная заметка",
        )

    async def add_reminder(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = " ".join(context.args)
        try:
            parts = split_parts(text, 3)
            when = parse_datetime(parts[0])
        except ValueError:
            await update.message.reply_text("Формат: /remind завтра 09:00 | Мама | взять документы")
            return
        if not when:
            await update.message.reply_text("Не понял дату напоминания.")
            return
        reminder_id = self.db.add_reminder(
            update.effective_chat.id,
            when.isoformat(timespec="seconds"),
            parts[2],
            parts[1],
        )
        await self._notify_family(update, self._build_notification(update, "Новое напоминание", reminder_id, parts[2], parts[1], when))
        await update.message.reply_text(f"Напоминание создано: #{reminder_id}", reply_markup=MENU_KEYBOARD)

    async def done(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text("Формат: /done ID")
            return
        result = self.db.complete_item(update.effective_chat.id, int(context.args[0]))
        await update.message.reply_text(self._completion_text(result), reply_markup=MENU_KEYBOARD)

    async def today(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        now = datetime.now()
        start = datetime.combine(now.date(), time.min)
        end = start + timedelta(days=1)
        items = self.db.list_window(update.effective_chat.id, start, end)
        await self._send_items_with_actions(update, "Сегодня", items)

    async def week(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        now = datetime.now()
        end = now + timedelta(days=7)
        items = self.db.list_window(update.effective_chat.id, now - timedelta(hours=1), end)
        await self._send_items_with_actions(update, "Ближайшие 7 дней", items)

    async def digest(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        mode = (context.args[0].lower() if context.args else "").strip()
        now = datetime.now()
        if mode in {"week", "неделя", "7"}:
            start = now
            end = now + timedelta(days=7)
            title = "Семейный дайджест на неделю"
        else:
            start = datetime.combine(now.date(), time.min)
            end = start + timedelta(days=1)
            title = "Семейный дайджест на сегодня"

        answer = await self._build_digest_answer(update.effective_chat.id, title, start, end)
        await update.effective_message.reply_text(answer[:3900], reply_markup=MENU_KEYBOARD)

    async def export_calendar(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        mode = (context.args[0].lower() if context.args else "").strip()
        now = datetime.now()
        days = 30
        if mode in {"week", "неделя", "7"}:
            days = 7
        elif mode.isdigit():
            days = min(180, max(1, int(mode)))

        start = datetime.combine(now.date(), time.min)
        end = start + timedelta(days=days)
        items = self.db.list_window(update.effective_chat.id, start, end)
        reminders = self.db.list_reminders_window(update.effective_chat.id, start, end)
        content = self._build_ics(update.effective_chat.id, items, reminders)
        if not content:
            await update.effective_message.reply_text(
                "За выбранный период нет записей с датой для экспорта.",
                reply_markup=MENU_KEYBOARD,
            )
            return

        payload = BytesIO(content.encode("utf-8"))
        payload.name = f"family-calendar-{now.strftime('%Y%m%d')}.ics"
        await update.effective_message.reply_document(
            document=payload,
            filename=payload.name,
            caption=f"Календарь семьи на {days} дн.",
            reply_markup=MENU_KEYBOARD,
        )

    async def backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        filename = f"family-planner-backup-{timestamp}.sqlite3"
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_path = Path(temp_dir) / filename
            try:
                await asyncio.to_thread(self.db.backup_to, backup_path)
            except Exception:
                LOGGER.exception("Failed to create database backup")
                await update.effective_message.reply_text(
                    "Не смог создать резервную копию базы.",
                    reply_markup=MENU_KEYBOARD,
                )
                return

            with backup_path.open("rb") as file:
                await update.effective_message.reply_document(
                    document=file,
                    filename=filename,
                    caption="Резервная копия семейного планера.",
                    reply_markup=MENU_KEYBOARD,
                )

    async def restore(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        reply = update.effective_message.reply_to_message if update.effective_message else None
        document = reply.document if reply and reply.document else None
        if not document:
            await update.effective_message.reply_text(
                "Чтобы восстановить базу, ответьте командой /restore на файл backup `.sqlite3`.",
                reply_markup=MENU_KEYBOARD,
            )
            return

        filename = document.file_name or ""
        if not filename.endswith(".sqlite3"):
            await update.effective_message.reply_text(
                "Нужен файл `.sqlite3`, созданный командой /backup.",
                reply_markup=MENU_KEYBOARD,
            )
            return

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        safety_path = Path("restore-safety") / f"before-restore-{timestamp}.sqlite3"
        with tempfile.TemporaryDirectory() as temp_dir:
            restore_path = Path(temp_dir) / "restore.sqlite3"
            try:
                telegram_file = await document.get_file()
                await telegram_file.download_to_drive(custom_path=restore_path)
                await asyncio.to_thread(self.db.restore_from, restore_path, safety_path)
            except Exception:
                LOGGER.exception("Failed to restore database backup")
                await update.effective_message.reply_text(
                    "Не смог восстановить базу. Файл не похож на корректный backup семейного планера.",
                    reply_markup=MENU_KEYBOARD,
                )
                return

        await update.effective_message.reply_text(
            f"База восстановлена. Safety-копия предыдущей базы: {safety_path}",
            reply_markup=MENU_KEYBOARD,
        )

    async def my_tasks(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        if not user:
            await update.effective_message.reply_text("Не вижу Telegram-пользователя.", reply_markup=MENU_KEYBOARD)
            return
        member = self.db.get_member_by_user_id(update.effective_chat.id, user.id)
        if not member:
            await update.effective_message.reply_text(
                "Сначала зарегистрируйтесь как член семьи: /join Имя",
                reply_markup=MENU_KEYBOARD,
            )
            return
        people = self._member_person_values(member)
        items = self.db.list_tasks(update.effective_chat.id, people)
        await self._send_items_with_actions(update, "Мои задачи", items)

    async def all_tasks(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        items = self.db.list_tasks(update.effective_chat.id)
        await self._send_items_with_actions(update, "Все задачи", items)

    async def _send_items_with_actions(self, update: Update, title: str, items: list[Item]) -> None:
        if not items:
            await update.effective_message.reply_text(f"{title}\nПока ничего нет.", reply_markup=MENU_KEYBOARD)
            return
        await update.effective_message.reply_text(title, reply_markup=MENU_KEYBOARD)
        for item in items:
            await update.effective_message.reply_html(
                self._item_card_text(item),
                reply_markup=self._item_actions_keyboard(item),
            )

    def _item_card_text(self, item: Item) -> str:
        when = format_dt(item.starts_at or item.due_at)
        lines = [
            f"<b>{escape(KIND_LABELS.get(item.kind, item.kind.title()))}</b>",
            f"#{item.id} {escape(when)}",
            escape(item.title),
        ]
        if item.person:
            lines.append(f"Кому: {self._format_person(item.person)}")
        if item.category:
            lines.append(f"Раздел: {escape(item.category)}")
        if item.amount is not None:
            lines.append(f"Сумма: {item.amount:g}")
        if item.recurrence:
            lines.append(f"Повтор: {escape(item.recurrence)}")
        if item.notes:
            lines.append(f"Комментарий: {escape(item.notes)}")
        return "\n".join(lines)

    def _item_actions_keyboard(self, item: Item) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Выполнено", callback_data=f"item:done:{item.id}"),
                    InlineKeyboardButton("Изменить", callback_data=f"item:edit:{item.id}"),
                ],
                [
                    InlineKeyboardButton("Перенести", callback_data=f"item:move:{item.id}"),
                    InlineKeyboardButton("Удалить", callback_data=f"item:delete:{item.id}"),
                ],
            ]
        )

    def _completion_text(self, result, item: Item | None = None) -> str:
        if not result.found:
            return "Не нашел такую запись в этом чате."
        if result.advanced and result.next_at:
            when = format_dt(result.next_at)
            if item:
                return f"Выполнено: #{item.id} {item.title}\nСледующий повтор: {when}"
            return f"Готово. Следующий повтор: {when}"
        if item:
            return f"Выполнено: #{item.id} {item.title}"
        return "Готово."

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    async def _build_digest_answer(
        self, chat_id: int, title: str, start: datetime, end: datetime
    ) -> str:
        items = self.db.list_window(chat_id, start, end)
        reminders = self.db.list_reminders_window(chat_id, start, end)
        context_text = self._digest_context(title, items, reminders)

        if not self.ai.is_enabled():
            return self._fallback_digest(title, items, reminders)

        question = (
            "Составь короткий семейный дайджест на русском: сначала главное на сегодня или неделю, "
            "потом риски/что подготовить, потом покупки и напоминания. Без длинных вступлений."
        )
        try:
            return await asyncio.to_thread(self.ai.answer, question, context_text)
        except Exception:
            LOGGER.exception("AI digest failed")
            return self._fallback_digest(title, items, reminders)

    def _digest_context(self, title: str, items: list[Item], reminders: list) -> str:
        lines = [title, "", render_context(items)]
        if reminders:
            lines.append("")
            lines.append("Напоминания:")
            lines.extend(self._format_reminder_line(reminder) for reminder in reminders[:30])
        return "\n".join(lines)

    def _fallback_digest(self, title: str, items: list[Item], reminders: list) -> str:
        lines = [render_items(title, items)]
        if reminders:
            lines.append("")
            lines.append("🔔 Напоминания")
            lines.extend(self._format_reminder_line(reminder) for reminder in reminders)
        return "\n".join(lines)

    def _format_reminder_line(self, reminder) -> str:
        when = format_dt(reminder["remind_at"])
        person = f" [{reminder['person']}]" if reminder["person"] else ""
        return f"#{reminder['id']} {when}{person}: {reminder['text']}"

    def _build_ics(self, chat_id: int, items: list[Item], reminders: list) -> str:
        dated_items = [item for item in items if item.starts_at or item.due_at]
        if not dated_items and not reminders:
            return ""

        now_stamp = self._ics_dt(datetime.now())
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Family Planner Bot//RU",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:Family Planner",
        ]

        for item in dated_items:
            when_raw = item.starts_at or item.due_at
            try:
                starts_at = datetime.fromisoformat(when_raw or "")
            except ValueError:
                continue
            duration = timedelta(hours=1) if item.starts_at else timedelta(minutes=30)
            ends_at = starts_at + duration
            details = []
            if item.person:
                details.append(f"Кому: {self._plain_person(item.person)}")
            if item.category:
                details.append(f"Раздел: {item.category}")
            if item.notes:
                details.append(f"Комментарий: {item.notes}")
            lines.extend(
                [
                    "BEGIN:VEVENT",
                    f"UID:item-{chat_id}-{item.id}-{self._ics_dt(starts_at)}@family-planner-bot",
                    f"DTSTAMP:{now_stamp}",
                    f"DTSTART:{self._ics_dt(starts_at)}",
                    f"DTEND:{self._ics_dt(ends_at)}",
                    f"SUMMARY:{self._ics_escape(KIND_LABELS.get(item.kind, item.kind.title()))}: {self._ics_escape(item.title)}",
                ]
            )
            if details:
                lines.append(f"DESCRIPTION:{self._ics_escape(chr(10).join(details))}")
            lines.append("END:VEVENT")

        for reminder in reminders:
            try:
                remind_at = datetime.fromisoformat(reminder["remind_at"])
            except ValueError:
                continue
            details = f"Кому: {self._plain_person(reminder['person'])}" if reminder["person"] else ""
            lines.extend(
                [
                    "BEGIN:VEVENT",
                    f"UID:reminder-{chat_id}-{reminder['id']}-{self._ics_dt(remind_at)}@family-planner-bot",
                    f"DTSTAMP:{now_stamp}",
                    f"DTSTART:{self._ics_dt(remind_at)}",
                    f"DTEND:{self._ics_dt(remind_at + timedelta(minutes=15))}",
                    f"SUMMARY:{self._ics_escape('🔔 Напоминание')}: {self._ics_escape(reminder['text'])}",
                ]
            )
            if details:
                lines.append(f"DESCRIPTION:{self._ics_escape(details)}")
            lines.append("END:VEVENT")

        lines.append("END:VCALENDAR")
        return "\r\n".join(lines) + "\r\n"

    @staticmethod
    def _ics_dt(value: datetime) -> str:
        return value.strftime("%Y%m%dT%H%M%S")

    @staticmethod
    def _ics_escape(value: str) -> str:
        return (
            value.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "")
            .replace(";", "\\;")
            .replace(",", "\\,")
        )

    @staticmethod
    def _plain_person(person: str | None) -> str:
        if not person:
            return ""
        if person.startswith('<a href="tg://user?id=') and person.endswith("</a>"):
            return person.split('">', 1)[-1].removesuffix("</a>")
        return person

    async def ask(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        question = " ".join(context.args).strip()
        await self._answer_ai(update, question)

    async def add_from_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text(
                "Формат: /add завтра в 18:00 у Маши стоматолог",
                reply_markup=MENU_KEYBOARD,
            )
            return
        if not await self._create_ai_entry(update, context, text):
            await update.message.reply_text(
                "AI не настроен. Добавьте OPENAI_API_KEY в .env и перезапустите бота.",
                reply_markup=MENU_KEYBOARD,
            )

    async def _answer_ai(self, update: Update, question: str) -> None:
        if not question:
            await update.message.reply_text("Формат: /ask Что у нас завтра?")
            return
        items = self.db.list_context(update.effective_chat.id)
        try:
            answer = await asyncio.to_thread(self.ai.answer, question, render_context(items))
        except Exception:
            LOGGER.exception("AI request failed")
            answer = "AI-помощник сейчас не ответил. Проверьте OPENAI_API_KEY, модель и доступ к API."
        await update.message.reply_text(answer[:3900], reply_markup=MENU_KEYBOARD)

    async def _create_ai_entry(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
    ) -> bool:
        if not self.ai.is_enabled():
            return False

        members = [dict(row) for row in self.db.list_members(update.effective_chat.id)]
        try:
            parsed = await asyncio.to_thread(
                self.ai.parse_family_entry,
                text,
                members,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            )
        except Exception:
            LOGGER.exception("AI entry parsing failed")
            await update.message.reply_text(
                "AI не смог разобрать запись. Попробуйте через кнопку меню или повторите фразу проще.",
                reply_markup=MENU_KEYBOARD,
            )
            return True

        flow = str(parsed.get("flow") or "").strip().lower()
        if flow in {"", "null", "none"}:
            clarification = str(parsed.get("clarification") or "Не понял, какую запись создать.")
            await update.message.reply_text(clarification, reply_markup=MENU_KEYBOARD)
            return True
        if flow not in CONSTRUCTORS:
            await update.message.reply_text(
                "AI распознал неподдержанный тип записи. Используйте событие, задачу, покупку, маркетплейс, вишлист или напоминание.",
                reply_markup=MENU_KEYBOARD,
            )
            return True

        data = self._normalize_ai_constructor_data(flow, parsed, members)
        errors = [
            error
            for field, _prompt in CONSTRUCTORS[flow]
            if (error := self._validate_constructor_field(flow, field, data.get(field)))
        ]
        if errors:
            await update.message.reply_text(
                f"{errors[0]}\nМожно заполнить через кнопку меню или повторить фразу подробнее.",
                reply_markup=MENU_KEYBOARD,
            )
            return True

        context.user_data.pop("flow", None)
        context.user_data.pop("constructor", None)
        context.user_data.pop("pending_confirmation", None)
        await self._show_constructor_confirmation(update, context, flow, data)
        return True

    async def handle_menu_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (update.message.text or "").strip()
        if not text:
            return

        if text == "Отмена":
            context.user_data.pop("flow", None)
            context.user_data.pop("constructor", None)
            context.user_data.pop("pending_confirmation", None)
            context.user_data.pop("item_action", None)
            await update.message.reply_text("Ок, действие отменено.", reply_markup=MENU_KEYBOARD)
            return

        if text in {"Сегодня", "☀️ Сегодня"}:
            await self.today(update, context)
            return
        if text in {"Неделя", "📅 Неделя"}:
            await self.week(update, context)
            return
        if text in {"Мои задачи", "👤 Мои задачи"}:
            await self.my_tasks(update, context)
            return
        if text in {"Все задачи", "✅ Все задачи"}:
            await self.all_tasks(update, context)
            return
        if text == "Помощь":
            await self.help(update, context)
            return
        if text in MENU_TO_FLOW:
            flow = MENU_TO_FLOW[text]
            if flow in CONSTRUCTORS:
                await self._start_constructor(update, context, flow)
                return
            context.user_data["flow"] = flow
            await update.message.reply_text(FLOW_PROMPTS[flow], reply_markup=MENU_KEYBOARD)
            return

        if context.user_data.get("constructor"):
            await self._handle_constructor_input(update, context, text)
            return

        if context.user_data.get("item_action"):
            await self._handle_item_action_input(update, context, text)
            return

        flow = context.user_data.pop("flow", None)
        if not flow:
            if await self._create_ai_entry(update, context, text):
                return
            await update.message.reply_text(
                "Выберите действие в меню, затем отправьте данные. Если включен OPENAI_API_KEY, можно писать запись обычной фразой.",
                reply_markup=MENU_KEYBOARD,
            )
            return

        await self._handle_flow_input(update, context, flow, text)

    async def handle_item_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        if not self._is_access_allowed(update):
            await query.answer("Нет доступа", show_alert=True)
            return

        parts = (query.data or "").split(":")
        if len(parts) < 3:
            await query.answer("Не понял действие", show_alert=True)
            return
        action = parts[1]
        item_id = int(parts[2])
        chat_id = query.message.chat_id
        item = self.db.get_item(chat_id, item_id)
        if not item:
            await query.answer("Запись не найдена", show_alert=True)
            await query.edit_message_text("Запись уже удалена или недоступна.")
            return

        if action == "done":
            result = self.db.complete_item(chat_id, item_id)
            await query.answer("Готово")
            await query.edit_message_text(self._completion_text(result, item))
            notify_title = "Повтор перенесен" if result.advanced else "Запись выполнена"
            next_at = self._parse_iso_datetime(result.next_at) if result.next_at else None
            await self._notify_family(update, self._build_notification(update, notify_title, item_id, item.title, item.person, next_at))
            return

        if action == "delete":
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Да, удалить", callback_data=f"item:delete_confirm:{item_id}"),
                        InlineKeyboardButton("Отмена", callback_data=f"item:cancel:{item_id}"),
                    ]
                ]
            )
            await query.answer()
            await query.edit_message_text(
                f"Удалить запись #{item_id}?\n{item.title}",
                reply_markup=keyboard,
            )
            return

        if action == "delete_confirm":
            deleted = self.db.delete_item(chat_id, item_id)
            await query.answer("Удалено" if deleted else "Не найдено")
            await query.edit_message_text(f"Удалено: #{item_id} {item.title}" if deleted else "Запись не найдена.")
            if deleted:
                await self._notify_family(update, self._build_notification(update, "Запись удалена", item_id, item.title, item.person))
            return

        if action == "cancel":
            await query.answer()
            await query.edit_message_text(self._item_card_text(item), parse_mode="HTML", reply_markup=self._item_actions_keyboard(item))
            return

        if action == "edit":
            context.user_data["item_action"] = {"action": "edit", "item_id": item_id}
            await query.answer()
            await query.message.reply_text(
                f"Введите новое название для #{item_id}.\nТекущее: {item.title}",
                reply_markup=MENU_KEYBOARD,
            )
            return

        if action == "move":
            context.user_data["item_action"] = {"action": "move", "item_id": item_id}
            await query.answer()
            await query.message.reply_text(
                f"Введите новую дату для #{item_id}.\nПример: завтра 18:30",
                reply_markup=MENU_KEYBOARD,
            )
            return

        await query.answer("Неизвестное действие", show_alert=True)

    async def _handle_item_action_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
    ) -> None:
        state = context.user_data.pop("item_action", None)
        if not state:
            return
        chat_id = update.effective_chat.id
        item_id = int(state["item_id"])
        item = self.db.get_item(chat_id, item_id)
        if not item:
            await update.message.reply_text("Запись не найдена.", reply_markup=MENU_KEYBOARD)
            return

        if state["action"] == "edit":
            title = text.strip()
            if not title:
                await update.message.reply_text("Название не может быть пустым.", reply_markup=MENU_KEYBOARD)
                return
            self.db.update_item_title(chat_id, item_id, title)
            await update.message.reply_text(f"Изменено: #{item_id}", reply_markup=MENU_KEYBOARD)
            await self._notify_family(update, self._build_notification(update, "Запись изменена", item_id, title, item.person))
            return

        if state["action"] == "move":
            when = parse_datetime(text)
            if not when:
                await update.message.reply_text("Не понял дату. Пример: завтра 18:30", reply_markup=MENU_KEYBOARD)
                return
            self.db.reschedule_item(chat_id, item_id, when.isoformat(timespec="seconds"))
            await update.message.reply_text(f"Перенесено: #{item_id} на {when.strftime('%d.%m %H:%M')}", reply_markup=MENU_KEYBOARD)
            await self._notify_family(update, self._build_notification(update, "Запись перенесена", item_id, item.title, item.person, when))
            return

        await update.message.reply_text("Не понял действие с записью.", reply_markup=MENU_KEYBOARD)

    async def _handle_flow_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, flow: str, text: str
    ) -> None:
        if flow == "member":
            parts = text.split()
            if not parts:
                await update.message.reply_text(FLOW_PROMPTS["member"], reply_markup=MENU_KEYBOARD)
                return
            await self._save_member(
                update,
                update.effective_chat.id,
                parts[0],
                parts[1] if len(parts) > 1 else None,
                parts[2] if len(parts) > 2 else None,
            )
            return
        if flow == "event":
            await self._add_dated_item(update, text, "event", "событие")
            return
        if flow == "booking":
            await self._add_dated_item(update, text, "booking", "бронь")
            return
        if flow == "task":
            await self._add_dated_item(update, text, "task", "задача")
            return
        if flow == "reminder":
            await self._save_reminder_from_text(update, text)
            return
        if flow == "shopping":
            await self._save_simple_item(
                update,
                "shopping",
                text,
                "Добавлено в покупки",
                notify_title="Новая покупка",
            )
            return
        if flow == "marketplace":
            await self._save_simple_item(
                update,
                "marketplace",
                text,
                "Добавлено в покупки на маркетплейсах",
                notify_title="Новая покупка на маркетплейсе",
            )
            return
        if flow == "wishlist":
            await self._save_simple_item(
                update,
                "wishlist",
                text,
                "Добавлено в вишлист",
                notify_title="Новое желание в семейном вишлисте",
            )
            return
        if flow == "expense":
            await self._save_expense_from_text(update, text)
            return
        if flow == "menu":
            await self._save_menu_from_text(update, text)
            return
        if flow == "note":
            await self._save_simple_item(
                update,
                "note",
                text,
                "Заметка сохранена",
                notify_title="Новая семейная заметка",
            )
            return
        if flow == "ai":
            await self._answer_ai(update, text)
            return
        if flow == "done":
            if not text.isdigit():
                await update.message.reply_text("Нужен числовой ID записи.", reply_markup=MENU_KEYBOARD)
                return
            result = self.db.complete_item(update.effective_chat.id, int(text))
            await update.message.reply_text(self._completion_text(result), reply_markup=MENU_KEYBOARD)
            return

        await update.message.reply_text("Не понял действие. Выберите пункт меню заново.", reply_markup=MENU_KEYBOARD)

    async def _start_constructor(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, flow: str
    ) -> None:
        context.user_data.pop("flow", None)
        context.user_data["constructor"] = {"flow": flow, "step": 0, "data": {}}
        await self._send_constructor_step(update, context, flow, 0)

    async def _send_constructor_step(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, flow: str, step: int
    ) -> None:
        field, _prompt = CONSTRUCTORS[flow][step]
        if field == "person":
            members = self.db.list_members(update.effective_chat.id)
            if members:
                keyboard = [
                    [InlineKeyboardButton(self._member_button_label(member), callback_data=f"member:{member['id']}")]
                    for member in members
                ]
                keyboard.append([InlineKeyboardButton("Ввести вручную", callback_data="member:manual")])
                await update.message.reply_text(
                    self._constructor_prompt(flow, step),
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
                return
        if field == "reminder":
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("За 10 минут", callback_data="reminder_offset:10m"),
                        InlineKeyboardButton("За 1 час", callback_data="reminder_offset:1h"),
                    ],
                    [
                        InlineKeyboardButton("За 1 день", callback_data="reminder_offset:1d"),
                        InlineKeyboardButton("Не напоминать", callback_data="reminder_offset:none"),
                    ],
                ]
            )
            await update.message.reply_text(
                self._constructor_prompt(flow, step),
                reply_markup=keyboard,
            )
            return
        await update.message.reply_text(
            self._constructor_prompt(flow, step),
            reply_markup=MENU_KEYBOARD,
        )

    async def _handle_constructor_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
    ) -> None:
        state = context.user_data.get("constructor")
        if not state:
            return

        flow = state["flow"]
        step = int(state["step"])
        field, _prompt = CONSTRUCTORS[flow][step]
        normalized = None if text.strip() == "-" else text.strip()

        error = self._validate_constructor_field(flow, field, normalized)
        if error:
            await update.message.reply_text(error, reply_markup=MENU_KEYBOARD)
            return

        state["data"][field] = normalized
        step += 1
        if step < len(CONSTRUCTORS[flow]):
            state["step"] = step
            context.user_data["constructor"] = state
            await self._send_constructor_step(update, context, flow, step)
            return

        context.user_data.pop("constructor", None)
        await self._show_constructor_confirmation(update, context, flow, state["data"])

    async def choose_reminder_offset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        if not self._is_access_allowed(update):
            await query.answer("Нет доступа", show_alert=True)
            return

        state = context.user_data.get("constructor")
        if not state:
            await query.answer("Конструктор уже закрыт", show_alert=True)
            return

        flow = state["flow"]
        step = int(state["step"])
        field, _prompt = CONSTRUCTORS[flow][step]
        if field != "reminder":
            await query.answer("Сейчас выбирается другое поле", show_alert=True)
            return

        value = (query.data or "").removeprefix("reminder_offset:")
        state["data"][field] = None if value == "none" else value
        step += 1
        await query.answer()
        await query.edit_message_text(f"Напоминание: {self._reminder_label(state['data'][field])}")

        if step < len(CONSTRUCTORS[flow]):
            state["step"] = step
            context.user_data["constructor"] = state
            await query.message.reply_text(
                self._constructor_prompt(flow, step),
                reply_markup=MENU_KEYBOARD,
            )
            return

        context.user_data.pop("constructor", None)
        await self._show_constructor_confirmation(update, context, flow, state["data"])

    async def choose_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        if not self._is_access_allowed(update):
            await query.answer("Нет доступа", show_alert=True)
            return

        state = context.user_data.get("constructor")
        if not state:
            await query.answer("Конструктор уже закрыт", show_alert=True)
            return

        flow = state["flow"]
        step = int(state["step"])
        field, _prompt = CONSTRUCTORS[flow][step]
        if field != "person":
            await query.answer("Сейчас выбирается другое поле", show_alert=True)
            return

        value = (query.data or "").removeprefix("member:")
        if value == "manual":
            await query.answer()
            await query.edit_message_text("Введите имя вручную.")
            return

        member = self.db.get_member(query.message.chat_id, int(value))
        if not member:
            await query.answer("Участник не найден", show_alert=True)
            return

        member_text = self._member_display(member)
        state["data"][field] = member_text
        step += 1
        await query.answer()
        await query.edit_message_text(f"Выбрано: {member_text}", parse_mode="HTML")

        if step < len(CONSTRUCTORS[flow]):
            state["step"] = step
            context.user_data["constructor"] = state
            await query.message.reply_text(
                self._constructor_prompt(flow, step),
                reply_markup=MENU_KEYBOARD,
            )
            return

        context.user_data.pop("constructor", None)
        await self._show_constructor_confirmation(update, context, flow, state["data"])

    def _constructor_prompt(self, flow: str, step: int) -> str:
        total = len(CONSTRUCTORS[flow])
        _field, prompt = CONSTRUCTORS[flow][step]
        return f"Шаг {step + 1}/{total}\n{prompt}"

    def _member_button_label(self, member) -> str:
        if member["username"]:
            return f"{member['name']} (@{member['username']})"
        return str(member["name"])

    def _member_display(self, member) -> str:
        return member["mention"] or str(member["name"])

    def _member_person_values(self, member) -> list[str]:
        values = [
            member["mention"],
            member["name"],
            f"@{member['username']}" if member["username"] else None,
            member["username"],
        ]
        return [str(value) for value in dict.fromkeys(values) if value]

    def _normalize_ai_constructor_data(
        self,
        flow: str,
        parsed: dict,
        members: list[dict[str, str | None]],
    ) -> dict[str, str | None]:
        data: dict[str, str | None] = {}
        for field, _prompt in CONSTRUCTORS[flow]:
            value = parsed.get(field)
            if value is None:
                data[field] = None
                continue
            value_s = str(value).strip()
            if not value_s or value_s.lower() in {"null", "none", "-"}:
                data[field] = None
                continue
            data[field] = value_s

        if "person" in data and data["person"]:
            data["person"] = self._resolve_ai_person(data["person"], members)
        if "recurrence" in data and data["recurrence"]:
            data["recurrence"] = data["recurrence"].lower()
        if "reminder" in data and data["reminder"]:
            data["reminder"] = data["reminder"].lower()
        return data

    def _resolve_ai_person(
        self, value: str, members: list[dict[str, str | None]]
    ) -> str:
        normalized = value.strip().lstrip("@").casefold()
        for member in members:
            candidates = [
                member.get("name"),
                member.get("username"),
                f"@{member['username']}" if member.get("username") else None,
                member.get("role"),
            ]
            if normalized in {str(candidate).strip().lstrip("@").casefold() for candidate in candidates if candidate}:
                return self._member_display(member)
        return value

    async def _show_constructor_confirmation(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        flow: str,
        data: dict[str, str | None],
    ) -> None:
        context.user_data["pending_confirmation"] = {"flow": flow, "data": data}
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Сохранить", callback_data="confirm:save"),
                    InlineKeyboardButton("Изменить", callback_data="confirm:edit"),
                ],
                [InlineKeyboardButton("Отмена", callback_data="confirm:cancel")],
            ]
        )
        await update.effective_message.reply_html(
            self._constructor_summary(flow, data),
            reply_markup=keyboard,
        )

    async def confirm_constructor(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        if not self._is_access_allowed(update):
            await query.answer("Нет доступа", show_alert=True)
            return

        action = (query.data or "").removeprefix("confirm:")
        pending = context.user_data.get("pending_confirmation")
        if not pending:
            await query.answer("Нет записи для подтверждения", show_alert=True)
            return

        flow = pending["flow"]
        data = pending["data"]
        if action == "save":
            context.user_data.pop("pending_confirmation", None)
            await query.answer("Сохраняю")
            await query.edit_message_text("Сохраняю запись...")
            await self._save_constructor_result(update, flow, data)
            return

        if action == "edit":
            context.user_data.pop("pending_confirmation", None)
            context.user_data["constructor"] = {"flow": flow, "step": 0, "data": {}}
            await query.answer()
            await query.edit_message_text("Ок, заполним заново.")
            await query.message.reply_text(
                self._constructor_prompt(flow, 0),
                reply_markup=MENU_KEYBOARD,
            )
            return

        if action == "cancel":
            context.user_data.pop("pending_confirmation", None)
            await query.answer()
            await query.edit_message_text("Запись отменена.")
            return

        await query.answer("Неизвестное действие", show_alert=True)

    def _constructor_summary(self, flow: str, data: dict[str, str | None]) -> str:
        labels = {
            "event": "Проверьте событие",
            "task": "Проверьте задачу",
            "shopping": "Проверьте покупку",
            "marketplace": "Проверьте покупку на маркетплейсе",
            "wishlist": "Проверьте вишлист",
            "reminder": "Проверьте напоминание",
        }
        field_labels = {
            "date": "Дата",
            "person": "Кому",
            "title": "Что",
            "category": "Категория",
            "recurrence": "Повтор",
            "reminder": "Напомнить",
            "notes": "Комментарий",
        }
        lines = [f"<b>{escape(labels.get(flow, 'Проверьте запись'))}</b>"]
        for field, _prompt in CONSTRUCTORS[flow]:
            value = data.get(field)
            if not value:
                continue
            if field == "date":
                parsed = parse_datetime(value)
                value = parsed.strftime("%d.%m %H:%M") if parsed else value
            label = escape(field_labels.get(field, field))
            if field == "person":
                formatted = self._format_person(value)
            elif field == "reminder":
                formatted = escape(self._reminder_label(value))
            else:
                formatted = escape(value)
            lines.append(f"{label}: {formatted}")
        lines.append("")
        lines.append("Сохранить запись?")
        return "\n".join(lines)

    def _validate_constructor_field(
        self, flow: str, field: str, value: str | None
    ) -> str | None:
        if field in {"date", "person", "title"} and not value:
            return "Это поле нужно заполнить. Для отмены отправьте Отмена."
        if field == "date" and value and not parse_datetime(value):
            return "Не понял дату. Пример: завтра 18:30"
        if field == "recurrence" and value and value.lower() not in {"daily", "weekly", "monthly"}:
            return "Повтор должен быть daily, weekly, monthly или '-'."
        if field == "reminder" and value and value.lower() not in {"10m", "1h", "1d"}:
            return "Напоминание должно быть 10m, 1h, 1d или '-'."
        return None

    def _reminder_label(self, value: str | None) -> str:
        labels = {
            "10m": "за 10 минут",
            "1h": "за 1 час",
            "1d": "за 1 день",
            None: "не напоминать",
        }
        return labels.get(value, value or "не напоминать")

    def _reminder_delta(self, value: str | None) -> timedelta | None:
        if value == "10m":
            return timedelta(minutes=10)
        if value == "1h":
            return timedelta(hours=1)
        if value == "1d":
            return timedelta(days=1)
        return None

    def _create_relative_reminder(
        self,
        chat_id: int,
        target_time: datetime,
        title: str,
        person: str | None,
        offset: str | None,
        flow: str,
    ) -> int | None:
        delta = self._reminder_delta(offset)
        if not delta:
            return None
        remind_at = target_time - delta
        if remind_at <= datetime.now():
            return None
        target_label = "задачи" if flow == "task" else "события"
        text = f"{title} ({self._reminder_label(offset)} до {target_label})"
        return self.db.add_reminder(
            chat_id,
            remind_at.isoformat(timespec="seconds"),
            text,
            person,
        )

    async def _save_constructor_result(
        self, update: Update, flow: str, data: dict[str, str | None]
    ) -> None:
        message = update.effective_message
        if flow in {"event", "task"}:
            when = parse_datetime(data["date"] or "")
            if not when:
                await message.reply_text("Не понял дату. Запись не сохранена.", reply_markup=MENU_KEYBOARD)
                return
            kind = flow
            label = "событие" if flow == "event" else "задача"
            title = data["title"] or ""
            person = data["person"]
            category = data.get("category")
            recurrence = data.get("recurrence")
            reminder_offset = data.get("reminder")
            item_id = self.db.add_item(
                update.effective_chat.id,
                kind,
                title,
                person=person,
                starts_at=when.isoformat(timespec="seconds") if flow == "event" else None,
                due_at=when.isoformat(timespec="seconds") if flow == "task" else None,
                category=category,
                recurrence=recurrence.lower() if recurrence else None,
            )
            reminder_id = self._create_relative_reminder(
                update.effective_chat.id,
                when,
                title,
                person,
                reminder_offset,
                flow,
            )
            await self._notify_family(
                update,
                self._build_notification(
                    update,
                    NOTIFICATION_TITLES[kind],
                    item_id,
                    title,
                    person,
                    when,
                    category,
                ),
            )
            reminder_text = f"\nНапоминание: #{reminder_id}" if reminder_id else ""
            await message.reply_html(f"Добавлено: {label} #{item_id}{reminder_text}", reply_markup=MENU_KEYBOARD)
            return

        if flow == "shopping":
            text = data["title"] or ""
            if data.get("notes"):
                text = f"{text} - {data['notes']}"
            await self._save_simple_item(
                update,
                "shopping",
                text,
                "Добавлено в покупки",
                notify_title="Новая покупка",
            )
            return

        if flow == "marketplace":
            parts = [data.get("title"), data.get("category"), data.get("notes")]
            text = " | ".join(part for part in parts if part)
            item_id = self.db.add_item(
                update.effective_chat.id,
                "marketplace",
                data["title"] or "",
                category=data.get("category"),
                notes=data.get("notes"),
            )
            await self._notify_family(
                update,
                self._build_notification(
                    update,
                    "Новая покупка на маркетплейсе",
                    item_id,
                    text,
                    None,
                    None,
                    data.get("category"),
                ),
            )
            await message.reply_html(f"Добавлено в покупки на маркетплейсах: #{item_id}", reply_markup=MENU_KEYBOARD)
            return

        if flow == "wishlist":
            parts = [data.get("title"), data.get("category"), data.get("notes")]
            text = " | ".join(part for part in parts if part)
            item_id = self.db.add_item(
                update.effective_chat.id,
                "wishlist",
                data["title"] or "",
                person=data.get("person"),
                category=data.get("category"),
                notes=data.get("notes"),
            )
            await self._notify_family(
                update,
                self._build_notification(
                    update,
                    "Новое желание в семейном вишлисте",
                    item_id,
                    text,
                    data.get("person"),
                    None,
                    data.get("category"),
                ),
            )
            await message.reply_html(f"Добавлено в вишлист: #{item_id}", reply_markup=MENU_KEYBOARD)
            return

        if flow == "reminder":
            when = parse_datetime(data["date"] or "")
            if not when:
                await message.reply_text("Не понял дату. Напоминание не сохранено.", reply_markup=MENU_KEYBOARD)
                return
            reminder_id = self.db.add_reminder(
                update.effective_chat.id,
                when.isoformat(timespec="seconds"),
                data["title"] or "",
                data.get("person"),
            )
            await self._notify_family(
                update,
                self._build_notification(
                    update,
                    "Новое напоминание",
                    reminder_id,
                    data["title"] or "",
                    data.get("person"),
                    when,
                ),
            )
            await message.reply_html(f"Напоминание создано: #{reminder_id}", reply_markup=MENU_KEYBOARD)
            return

        await message.reply_text("Не понял конструктор. Запись не сохранена.", reply_markup=MENU_KEYBOARD)

    async def _save_member(
        self,
        update: Update,
        chat_id: int,
        name: str,
        role: str | None,
        color: str | None,
    ) -> None:
        self.db.add_member(chat_id, name, role, color)
        await update.message.reply_text(f"Добавлено: {name}", reply_markup=MENU_KEYBOARD)

    async def _save_reminder_from_text(self, update: Update, text: str) -> None:
        try:
            parts = split_parts(text, 3)
            when = parse_datetime(parts[0])
        except ValueError:
            await update.message.reply_text(FLOW_PROMPTS["reminder"], reply_markup=MENU_KEYBOARD)
            return
        if not when:
            await update.message.reply_text("Не понял дату напоминания.", reply_markup=MENU_KEYBOARD)
            return
        reminder_id = self.db.add_reminder(
            update.effective_chat.id,
            when.isoformat(timespec="seconds"),
            parts[2],
            parts[1],
        )
        await self._notify_family(update, self._build_notification(update, "Новое напоминание", reminder_id, parts[2], parts[1], when))
        await update.message.reply_text(f"Напоминание создано: #{reminder_id}", reply_markup=MENU_KEYBOARD)

    async def _notify_family(self, update: Update, text: str) -> None:
        await update.get_bot().send_message(chat_id=update.effective_chat.id, text=text, parse_mode="HTML")

    def _build_notification(
        self,
        update: Update,
        title: str,
        item_id: int,
        text: str,
        person: str | None = None,
        when: datetime | None = None,
        category: str | None = None,
    ) -> str:
        author = self._author_name(update)
        lines = [f"{escape(title)}: #{item_id}", escape(text)]
        details = []
        if person:
            details.append(f"кому: {self._format_person(person)}")
        if when:
            details.append(f"когда: {escape(when.strftime('%d.%m %H:%M'))}")
        if category:
            details.append(f"раздел: {escape(category)}")
        if author:
            details.append(f"добавил: {escape(author)}")
        if details:
            lines.append(", ".join(details))
        return "\n".join(lines)

    def _format_person(self, person: str) -> str:
        if person.startswith("<a href=\"tg://user?id="):
            return person
        return escape(person)

    def _author_name(self, update: Update) -> str | None:
        user = update.effective_user
        if not user:
            return None
        return user.full_name or user.username

    def _is_access_allowed(self, update: Update) -> bool:
        if not self.settings.allowed_chat_ids and not self.settings.allowed_user_ids:
            return True

        chat = update.effective_chat
        user = update.effective_user
        if chat and chat.id in self.settings.allowed_chat_ids:
            return True
        if user and user.id in self.settings.allowed_user_ids:
            return True
        return False

    def _is_admin_allowed(self, update: Update) -> bool:
        if not self.settings.admin_user_ids:
            return self._is_access_allowed(update)
        user = update.effective_user
        return bool(user and user.id in self.settings.admin_user_ids)

    async def _deny_access(self, update: Update) -> None:
        chat_id = update.effective_chat.id if update.effective_chat else "unknown"
        user_id = update.effective_user.id if update.effective_user else "unknown"
        LOGGER.warning("Denied access for chat_id=%s user_id=%s", chat_id, user_id)
        if update.message:
            await update.message.reply_text(
                "Доступ к семейному боту закрыт. Отправьте /id владельцу бота, чтобы вас добавили."
            )

    async def _deny_admin_access(self, update: Update) -> None:
        user_id = update.effective_user.id if update.effective_user else "unknown"
        LOGGER.warning("Denied admin action for user_id=%s", user_id)
        if update.message:
            await update.message.reply_text(
                "Это действие доступно только администратору семьи. Отправьте /id владельцу бота, чтобы вас добавили в ADMIN_USER_IDS."
            )

    def _restricted(
        self,
        callback: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]],
    ) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]:
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._is_access_allowed(update):
                await self._deny_access(update)
                return
            await callback(update, context)

        return wrapper

    def _admin_restricted(
        self,
        callback: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]],
    ) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]:
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not self._is_access_allowed(update):
                await self._deny_access(update)
                return
            if not self._is_admin_allowed(update):
                await self._deny_admin_access(update)
                return
            await callback(update, context)

        return wrapper

    async def send_due_reminders(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        now = datetime.now()
        reminders = self.db.due_reminders(now, self.settings.reminder_lookahead_minutes)
        for reminder in reminders:
            person = f" для {reminder['person']}" if reminder["person"] else ""
            try:
                await context.bot.send_message(
                    chat_id=reminder["chat_id"],
                    text=f"Напоминание{person}: {reminder['text']}",
                )
                self.db.mark_reminder_sent(int(reminder["id"]))
            except Exception:
                LOGGER.exception("Failed to send reminder %s", reminder["id"])

    async def send_daily_digest(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_ids = self.settings.daily_digest_chat_ids or frozenset(self.db.list_known_chat_ids())
        if not chat_ids:
            LOGGER.info("Daily digest skipped: no target chats")
            return

        now = datetime.now()
        start = datetime.combine(now.date(), time.min)
        end = start + timedelta(days=1)
        for chat_id in sorted(chat_ids):
            try:
                answer = await self._build_digest_answer(
                    chat_id,
                    "Семейный дайджест на сегодня",
                    start,
                    end,
                )
                await context.bot.send_message(chat_id=chat_id, text=answer[:3900])
            except Exception:
                LOGGER.exception("Failed to send daily digest to chat %s", chat_id)

    def build_application(self) -> Application:
        app = Application.builder().token(self.settings.telegram_bot_token).build()
        if not self.settings.allowed_chat_ids and not self.settings.allowed_user_ids:
            LOGGER.warning("Access allowlist is empty. Bot is available to anyone who can message it.")

        app.add_handler(CommandHandler("id", self.show_ids))
        app.add_handler(CommandHandler("start", self._restricted(self.start)))
        app.add_handler(CommandHandler("keyboard", self._restricted(self.menu)))
        app.add_handler(CommandHandler("help", self._restricted(self.help)))
        app.add_handler(CommandHandler("join", self._restricted(self.join_member)))
        app.add_handler(CommandHandler("member", self._restricted(self.add_member)))
        app.add_handler(CommandHandler("members", self._restricted(self.members)))
        app.add_handler(CommandHandler("event", self._restricted(self.add_event)))
        app.add_handler(CommandHandler("booking", self._restricted(self.add_booking)))
        app.add_handler(CommandHandler("task", self._restricted(self.add_task)))
        app.add_handler(CommandHandler("buy", self._restricted(self.add_buy)))
        app.add_handler(CommandHandler("market", self._restricted(self.add_marketplace)))
        app.add_handler(CommandHandler("wish", self._restricted(self.add_wishlist)))
        app.add_handler(CommandHandler("expense", self._restricted(self.add_expense)))
        app.add_handler(CommandHandler("menu", self._restricted(self.add_menu)))
        app.add_handler(CommandHandler("note", self._restricted(self.add_note)))
        app.add_handler(CommandHandler("remind", self._restricted(self.add_reminder)))
        app.add_handler(CommandHandler("done", self._restricted(self.done)))
        app.add_handler(CommandHandler("today", self._restricted(self.today)))
        app.add_handler(CommandHandler("week", self._restricted(self.week)))
        app.add_handler(CommandHandler("digest", self._restricted(self.digest)))
        app.add_handler(CommandHandler("export", self._restricted(self.export_calendar)))
        app.add_handler(CommandHandler("backup", self._admin_restricted(self.backup)))
        app.add_handler(CommandHandler("restore", self._admin_restricted(self.restore)))
        app.add_handler(CommandHandler("my", self._restricted(self.my_tasks)))
        app.add_handler(CommandHandler("tasks", self._restricted(self.all_tasks)))
        app.add_handler(CommandHandler("add", self._restricted(self.add_from_text)))
        app.add_handler(CommandHandler("ask", self._restricted(self.ask)))
        app.add_handler(CallbackQueryHandler(self.choose_member, pattern=r"^member:"))
        app.add_handler(CallbackQueryHandler(self.choose_reminder_offset, pattern=r"^reminder_offset:"))
        app.add_handler(CallbackQueryHandler(self.confirm_constructor, pattern=r"^confirm:"))
        app.add_handler(CallbackQueryHandler(self.handle_item_action, pattern=r"^item:"))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._restricted(self.handle_menu_text)))
        app.job_queue.run_repeating(self.send_due_reminders, interval=60, first=5)
        if self.settings.daily_digest_time:
            app.job_queue.run_daily(self.send_daily_digest, time=self.settings.daily_digest_time)
        return app


def main() -> None:
    settings = load_settings()
    bot = FamilyPlannerBot(settings)
    app = bot.build_application()
    LOGGER.info("Family planner bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
