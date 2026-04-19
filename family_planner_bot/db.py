from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class Item:
    id: int
    kind: str
    chat_id: int
    title: str
    person: str | None
    starts_at: str | None
    due_at: str | None
    amount: float | None
    category: str | None
    recurrence: str | None
    notes: str | None
    is_done: bool


@dataclass(frozen=True)
class CompletionResult:
    found: bool
    advanced: bool = False
    next_at: str | None = None


class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def backup_to(self, target_path: Path) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        source = sqlite3.connect(self.path)
        target = sqlite3.connect(target_path)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()

    def restore_from(self, source_path: Path, safety_backup_path: Path) -> None:
        self._validate_backup(source_path)
        self.backup_to(safety_backup_path)
        source = sqlite3.connect(source_path)
        target = sqlite3.connect(self.path)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        self.init_schema()

    @staticmethod
    def _validate_backup(source_path: Path) -> None:
        required_tables = {"members", "items", "reminders"}
        required_columns = {
            "members": {"chat_id", "name", "created_at"},
            "items": {"chat_id", "kind", "title", "created_at"},
            "reminders": {"chat_id", "text", "remind_at", "created_at"},
        }
        conn = sqlite3.connect(source_path)
        conn.row_factory = sqlite3.Row
        try:
            result = conn.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise ValueError("backup integrity check failed")

            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            tables = {str(row["name"]) for row in rows}
            missing_tables = required_tables - tables
            if missing_tables:
                raise ValueError(f"backup missing tables: {', '.join(sorted(missing_tables))}")

            for table, columns in required_columns.items():
                found = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                missing_columns = columns - found
                if missing_columns:
                    raise ValueError(
                        f"backup table {table} missing columns: {', '.join(sorted(missing_columns))}"
                    )
        finally:
            conn.close()

    @contextmanager
    def connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    telegram_user_id INTEGER,
                    name TEXT NOT NULL,
                    username TEXT,
                    mention TEXT,
                    role TEXT,
                    color TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(chat_id, name)
                );

                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    person TEXT,
                    starts_at TEXT,
                    due_at TEXT,
                    amount REAL,
                    category TEXT,
                    recurrence TEXT,
                    notes TEXT,
                    is_done INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_items_chat_kind ON items(chat_id, kind);
                CREATE INDEX IF NOT EXISTS idx_items_starts ON items(chat_id, starts_at);
                CREATE INDEX IF NOT EXISTS idx_items_due ON items(chat_id, due_at);

                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    person TEXT,
                    text TEXT NOT NULL,
                    remind_at TEXT NOT NULL,
                    sent_at TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_reminders_pending
                    ON reminders(chat_id, remind_at, sent_at);
                """
            )
            self._ensure_column(conn, "members", "telegram_user_id", "INTEGER")
            self._ensure_column(conn, "members", "username", "TEXT")
            self._ensure_column(conn, "members", "mention", "TEXT")

    def add_member(
        self,
        chat_id: int,
        name: str,
        role: str | None,
        color: str | None,
        telegram_user_id: int | None = None,
        username: str | None = None,
        mention: str | None = None,
    ) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO members(
                    chat_id, telegram_user_id, name, username, mention, role, color, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, name)
                DO UPDATE SET
                    telegram_user_id = COALESCE(excluded.telegram_user_id, members.telegram_user_id),
                    username = COALESCE(excluded.username, members.username),
                    mention = COALESCE(excluded.mention, members.mention),
                    role = excluded.role,
                    color = excluded.color
                """,
                (chat_id, telegram_user_id, name, username, mention, role, color, now),
            )

    def list_members(self, chat_id: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, telegram_user_id, name, username, mention, role, color
                FROM members
                WHERE chat_id = ?
                ORDER BY name
                """,
                (chat_id,),
            ).fetchall()
        return list(rows)

    def get_member(self, chat_id: int, member_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT id, telegram_user_id, name, username, mention, role, color
                FROM members
                WHERE chat_id = ? AND id = ?
                """,
                (chat_id, member_id),
            ).fetchone()

    def get_member_by_user_id(self, chat_id: int, telegram_user_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT id, telegram_user_id, name, username, mention, role, color
                FROM members
                WHERE chat_id = ? AND telegram_user_id = ?
                ORDER BY id
                LIMIT 1
                """,
                (chat_id, telegram_user_id),
            ).fetchone()

    def add_item(
        self,
        chat_id: int,
        kind: str,
        title: str,
        person: str | None = None,
        starts_at: str | None = None,
        due_at: str | None = None,
        amount: float | None = None,
        category: str | None = None,
        recurrence: str | None = None,
        notes: str | None = None,
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO items(
                    chat_id, kind, title, person, starts_at, due_at, amount,
                    category, recurrence, notes, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    kind,
                    title,
                    person,
                    starts_at,
                    due_at,
                    amount,
                    category,
                    recurrence,
                    notes,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def mark_done(self, chat_id: int, item_id: int) -> bool:
        return self.complete_item(chat_id, item_id).found

    def complete_item(self, chat_id: int, item_id: int) -> CompletionResult:
        item = self.get_item(chat_id, item_id)
        if not item:
            return CompletionResult(found=False)

        source_value = item.starts_at or item.due_at
        if item.recurrence and source_value:
            next_at = self._next_future_occurrence(source_value, item.recurrence)
            if next_at:
                field = "starts_at" if item.starts_at else "due_at"
                next_s = next_at.isoformat(timespec="seconds")
                with self.connect() as conn:
                    cur = conn.execute(
                        f"UPDATE items SET {field} = ?, is_done = 0 WHERE chat_id = ? AND id = ?",
                        (next_s, chat_id, item_id),
                    )
                    if cur.rowcount > 0:
                        return CompletionResult(found=True, advanced=True, next_at=next_s)

        with self.connect() as conn:
            cur = conn.execute(
                "UPDATE items SET is_done = 1 WHERE chat_id = ? AND id = ?",
                (chat_id, item_id),
            )
            return CompletionResult(found=cur.rowcount > 0)

    def get_item(self, chat_id: int, item_id: int) -> Item | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM items WHERE chat_id = ? AND id = ?",
                (chat_id, item_id),
            ).fetchone()
        return self._item_from_row(row) if row else None

    def update_item_title(self, chat_id: int, item_id: int, title: str) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                "UPDATE items SET title = ? WHERE chat_id = ? AND id = ?",
                (title, chat_id, item_id),
            )
            return cur.rowcount > 0

    def reschedule_item(self, chat_id: int, item_id: int, when: str) -> bool:
        item = self.get_item(chat_id, item_id)
        if not item:
            return False
        field = "starts_at" if item.starts_at else "due_at"
        with self.connect() as conn:
            cur = conn.execute(
                f"UPDATE items SET {field} = ? WHERE chat_id = ? AND id = ?",
                (when, chat_id, item_id),
            )
            return cur.rowcount > 0

    def delete_item(self, chat_id: int, item_id: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                "DELETE FROM items WHERE chat_id = ? AND id = ?",
                (chat_id, item_id),
            )
            return cur.rowcount > 0

    def list_window(self, chat_id: int, start: datetime, end: datetime) -> list[Item]:
        start_s = start.isoformat(timespec="seconds")
        end_s = end.isoformat(timespec="seconds")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM items
                WHERE chat_id = ?
                  AND is_done = 0
                  AND (
                    (starts_at IS NOT NULL AND starts_at >= ? AND starts_at < ?)
                    OR (due_at IS NOT NULL AND due_at >= ? AND due_at < ?)
                    OR kind IN ('shopping', 'marketplace', 'wishlist', 'note')
                  )
                ORDER BY COALESCE(starts_at, due_at, created_at), id
                """,
                (chat_id, start_s, end_s, start_s, end_s),
            ).fetchall()
            recurring_rows = conn.execute(
                """
                SELECT * FROM items
                WHERE chat_id = ?
                  AND is_done = 0
                  AND recurrence IS NOT NULL
                  AND (starts_at IS NOT NULL OR due_at IS NOT NULL)
                """,
                (chat_id,),
            ).fetchall()

        items = [self._item_from_row(row) for row in rows]
        items.extend(
            occurrence
            for row in recurring_rows
            if (occurrence := self._expand_recurring(row, start, end)) is not None
        )
        unique = {(item.id, item.starts_at, item.due_at): item for item in items}
        return sorted(
            unique.values(),
            key=lambda item: (item.starts_at or item.due_at or "", item.id),
        )

    def list_context(self, chat_id: int, days: int = 14) -> list[Item]:
        now = datetime.now()
        return self.list_window(chat_id, now - timedelta(days=1), now + timedelta(days=days))

    def list_tasks(self, chat_id: int, people: list[str] | None = None) -> list[Item]:
        params: list[object] = [chat_id]
        person_filter = ""
        if people:
            placeholders = ",".join("?" for _ in people)
            person_filter = f" AND person IN ({placeholders})"
            params.extend(people)

        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM items
                WHERE chat_id = ?
                  AND kind = 'task'
                  AND is_done = 0
                  {person_filter}
                ORDER BY COALESCE(due_at, created_at), id
                """,
                params,
            ).fetchall()
        return [self._item_from_row(row) for row in rows]

    def list_known_chat_ids(self) -> list[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT chat_id FROM items
                UNION
                SELECT chat_id FROM reminders
                UNION
                SELECT chat_id FROM members
                ORDER BY chat_id
                """
            ).fetchall()
        return [int(row["chat_id"]) for row in rows]

    def add_reminder(self, chat_id: int, remind_at: str, text: str, person: str | None) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO reminders(chat_id, person, text, remind_at, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (chat_id, person, text, remind_at, now),
            )
            return int(cur.lastrowid)

    def due_reminders(self, now: datetime, lookahead_minutes: int) -> list[sqlite3.Row]:
        until = now + timedelta(minutes=lookahead_minutes)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM reminders
                WHERE sent_at IS NULL AND remind_at <= ?
                ORDER BY remind_at, id
                """,
                (until.isoformat(timespec="seconds"),),
            ).fetchall()
        return list(rows)

    def list_reminders_window(self, chat_id: int, start: datetime, end: datetime) -> list[sqlite3.Row]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, chat_id, person, text, remind_at, sent_at
                FROM reminders
                WHERE chat_id = ?
                  AND sent_at IS NULL
                  AND remind_at >= ?
                  AND remind_at < ?
                ORDER BY remind_at, id
                """,
                (
                    chat_id,
                    start.isoformat(timespec="seconds"),
                    end.isoformat(timespec="seconds"),
                ),
            ).fetchall()
        return list(rows)

    def mark_reminder_sent(self, reminder_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                "UPDATE reminders SET sent_at = ? WHERE id = ?",
                (now, reminder_id),
            )

    @staticmethod
    def _item_from_row(row: sqlite3.Row) -> Item:
        return Item(
            id=int(row["id"]),
            kind=str(row["kind"]),
            chat_id=int(row["chat_id"]),
            title=str(row["title"]),
            person=row["person"],
            starts_at=row["starts_at"],
            due_at=row["due_at"],
            amount=row["amount"],
            category=row["category"],
            recurrence=row["recurrence"],
            notes=row["notes"],
            is_done=bool(row["is_done"]),
        )

    def _expand_recurring(self, row: sqlite3.Row, start: datetime, end: datetime) -> Item | None:
        item = self._item_from_row(row)
        source_value = item.starts_at or item.due_at
        if not source_value or not item.recurrence:
            return None

        try:
            source = datetime.fromisoformat(source_value)
        except ValueError:
            return None

        occurrence = source
        while occurrence < start:
            occurrence = self._next_occurrence(occurrence, item.recurrence)
            if occurrence == source:
                return None

        if occurrence >= end:
            return None

        occurrence_s = occurrence.isoformat(timespec="seconds")
        return Item(
            id=item.id,
            kind=item.kind,
            chat_id=item.chat_id,
            title=item.title,
            person=item.person,
            starts_at=occurrence_s if item.starts_at else None,
            due_at=occurrence_s if item.due_at else None,
            amount=item.amount,
            category=item.category,
            recurrence=item.recurrence,
            notes=item.notes,
            is_done=item.is_done,
        )

    @staticmethod
    def _next_occurrence(value: datetime, recurrence: str) -> datetime:
        if recurrence == "daily":
            return value + timedelta(days=1)
        if recurrence == "weekly":
            return value + timedelta(weeks=1)
        if recurrence == "monthly":
            month = value.month + 1
            year = value.year
            if month > 12:
                month = 1
                year += 1
            day = min(value.day, Database._days_in_month(year, month))
            return value.replace(year=year, month=month, day=day)
        return value

    @staticmethod
    def _next_future_occurrence(source_value: str, recurrence: str) -> datetime | None:
        try:
            occurrence = datetime.fromisoformat(source_value)
        except ValueError:
            return None

        next_occurrence = Database._next_occurrence(occurrence, recurrence)
        if next_occurrence == occurrence:
            return None

        now = datetime.now()
        guard = 0
        while next_occurrence <= now and guard < 500:
            previous = next_occurrence
            next_occurrence = Database._next_occurrence(next_occurrence, recurrence)
            if next_occurrence == previous:
                return None
            guard += 1
        return next_occurrence

    @staticmethod
    def _days_in_month(year: int, month: int) -> int:
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        return (next_month - timedelta(days=1)).day

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
