from __future__ import annotations

import os
import sys
import logging
import asyncio
import concurrent.futures
import hashlib
import fcntl
import atexit
from datetime import datetime, timedelta, date, time
from functools import wraps

import pytz
import aiosqlite
import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    BotCommandScopeChat,
    InlineQueryResultArticle,
    InputTextMessageContent,
)

from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters,
    ContextTypes,
    InlineQueryHandler,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)


def resource_path(relative_path: str) -> str:
    try:
        base_path = sys._MEIPASS  # type: ignore[attr-defined]
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)



TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","8047200746:AAHI8fMpGPG41CfXj3hk6g6lqMsmZs_bG4A")
if not TELEGRAM_TOKEN:
    raise RuntimeError("Не задана переменная окружения TELEGRAM_BOT_TOKEN")

SERVICE_ACCOUNT_FILE = resource_path("service_account.json")

SPREADSHEET_ID = "1Z39dIQrgdhSoWdD5AE9jIMtfn1ahTxl-femjqxyER0Q"
SPREADSHEET_GID = 765012037

MOSCOW_TZ = pytz.timezone("Europe/Moscow")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1002656708512"))

ADMIN_PASSWORD_HASH = os.getenv(
    "ADMIN_PASSWORD_HASH",
    hashlib.sha256("1235".encode("utf-8")).hexdigest(),
)

_CACHE_TIMEOUT_SECONDS = 600
DATA_START_ROW = 6  
PLACE_COL_START = 5
PLACE_COL_END = 11 
PRICE_COL_INDEX_1BASED = 14
PRICE_COL_INDEX_0BASED = 13



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot_actions.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

_cached_sheet_data: list[list[str]] = []
_cached_sheet_last_update: datetime | None = None
free_places_cache: dict[int, list[int]] = {}
cache_lock = asyncio.Lock()

admin_sessions: set[int] = set()
admin_password_pending: set[int] = set()
user_sessions: dict[int, dict] = {}
remind_settings: dict[int, bool] = {}
reminder_tasks: dict[int, asyncio.Task] = {}

scheduler: AsyncIOScheduler | None = None
_instance_lock_file = None


REGISTER_NAME, CHANGE_NAME = range(2)
CHOOSING_TIME, CONFIRMING = range(20, 22)

MORNING_MESSAGE = (
    "☀️ Всем доброго дня!) Записываемся на занятия:\n"
    "https://docs.google.com/spreadsheets/d/1Z39dIQrgdhSoWdD5AE9jIMtfn1ahTxl-femjqxyER0Q/edit?gid=765012037#gid=765012037"
)


def acquire_instance_lock() -> None:
    """Не даёт запустить одновременно несколько экземпляров бота."""
    global _instance_lock_file

    lock_path = os.getenv("BOT_LOCK_FILE", "/tmp/telegram-bot.lock")
    _instance_lock_file = open(lock_path, "w", encoding="utf-8")

    try:
        fcntl.flock(_instance_lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError(
            f"Другой экземпляр бота уже запущен. Файл блокировки: {lock_path}"
        ) from exc

    _instance_lock_file.seek(0)
    _instance_lock_file.truncate()
    _instance_lock_file.write(str(os.getpid()))
    _instance_lock_file.flush()


def release_instance_lock() -> None:
    global _instance_lock_file
    if _instance_lock_file is None:
        return
    try:
        fcntl.flock(_instance_lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        _instance_lock_file.close()
        _instance_lock_file = None


atexit.register(release_instance_lock)

def weekday_rus(value: date | datetime) -> str:
    dt = value if isinstance(value, datetime) else datetime.combine(value, time.min)
    weekdays = {
        "Monday": "Понедельник",
        "Tuesday": "Вторник",
        "Wednesday": "Среда",
        "Thursday": "Четверг",
        "Friday": "Пятница",
        "Saturday": "Суббота",
        "Sunday": "Воскресенье",
    }
    return weekdays.get(dt.strftime("%A"), dt.strftime("%A"))


def safe_parse_date(date_str: str | None) -> date | None:
    """Безопасный парсинг даты из колонки C."""
    if not date_str:
        return None

    s = str(date_str).strip().replace("\r", "").replace("\n", " ")
    if not s:
        return None
    formats = (
        "%d.%m.%Y",
        "%d.%m.%y",
        "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    logger.warning(f"Не удалось распарсить дату: {s!r}")
    return None


def parse_time_and_teacher(cell_value: str | None) -> tuple[time | None, str]:
    if not cell_value:
        return None, ""

    raw = str(cell_value).replace("\r", "\n").strip()
    if not raw:
        return None, ""
    parts = [p.strip() for p in raw.split("\n") if p.strip()]

    if not parts:
        return None, ""

    time_part = parts[0]
    teacher = " ".join(parts[1:]) if len(parts) > 1 else ""

    normalized_time = (
        time_part.replace("-", ":")
        .replace(".", ":")
        .replace("–", ":")
        .replace("—", ":")
        .strip()
    )
    if " " in normalized_time:
        normalized_time = normalized_time.split()[0]

    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            parsed_time = datetime.strptime(normalized_time, fmt).time()
            return parsed_time, teacher
        except ValueError:
            continue

    logger.warning(f"Не удалось распарсить время из ячейки D: raw={raw!r}")
    return None, teacher


def normalize_sheet_row(row: list[str]) -> list[str]:
    """Приводит строку таблицы к безопасному виду."""
    return [str(cell).strip() if cell is not None else "" for cell in row]


def is_effective_lesson_row(row: list[str]) -> bool:
    if not row:
        return False

    if len(row) < 4:
        return False

    row = normalize_sheet_row(row)

    lesson_date = safe_parse_date(row[2] if len(row) > 2 else "")
    if not lesson_date:
        return False

    if len(row) <= 3 or not row[3].strip():
        return False

    lesson_time, _ = parse_time_and_teacher(row[3])
    if not lesson_time:
        return False

    return True


def get_price_from_row(row: list[str]) -> str:
    """Берёт цену из колонки N."""
    if len(row) > PRICE_COL_INDEX_0BASED:
        return str(row[PRICE_COL_INDEX_0BASED]).strip()
    return ""


def combine_lesson_datetime(lesson_date: date, lesson_time: time) -> datetime:
    return MOSCOW_TZ.localize(datetime.combine(lesson_date, lesson_time))

async def async_update_cell_no_wait(sheet: gspread.Worksheet, row: int, col: int, val: str) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, sheet.update_cell, row, col, val)


@retry(
    wait=wait_exponential(min=2, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(Exception),
)
async def get_google_sheet() -> gspread.Worksheet:
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)

    loop = asyncio.get_running_loop()

    def _open_sheet():
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheets = spreadsheet.worksheets()
        for ws in worksheets:
            if ws.id == SPREADSHEET_GID:
                return ws
        raise ValueError(f"Лист с gid={SPREADSHEET_GID} не найден")

    sheet = await loop.run_in_executor(_executor, _open_sheet)
    return sheet


async def refresh_cache() -> None:
    """Обновляет кэш данных из Google Sheets."""
    global _cached_sheet_data, _cached_sheet_last_update, free_places_cache

    async with cache_lock:
        loop = asyncio.get_running_loop()
        try:
            sheet = await get_google_sheet()
            data = await loop.run_in_executor(_executor, sheet.get_all_values)
        except Exception as e:
            logger.error(f"Ошибка загрузки Google Sheets: {e}")
            return

        normalized_data = [normalize_sheet_row(row) for row in data]
        _cached_sheet_data = normalized_data
        _cached_sheet_last_update = datetime.now(MOSCOW_TZ)

        free_places_cache.clear()

        for row_idx, row in enumerate(normalized_data[DATA_START_ROW - 1:], start=DATA_START_ROW):
            if not is_effective_lesson_row(row):
                continue

            places = row[PLACE_COL_START - 1:PLACE_COL_END]
            free_cols = [col for col, val in enumerate(places, start=PLACE_COL_START) if not val.strip()]
            free_places_cache[row_idx] = free_cols

        logger.info("Кэш Google Sheets обновлён")


async def get_cached_data() -> list[list[str]]:
    global _cached_sheet_last_update
    now = datetime.now(MOSCOW_TZ)

    if (
        not _cached_sheet_last_update
        or (now - _cached_sheet_last_update).total_seconds() > _CACHE_TIMEOUT_SECONDS
    ):
        await refresh_cache()

    return _cached_sheet_data


async def ensure_user_in_cache(name: str) -> None:
    data = await get_cached_data()
    found = any(
        name in row[PLACE_COL_START - 1:PLACE_COL_END]
        for row in data[DATA_START_ROW - 1:]
        if len(row) >= PLACE_COL_END
    )
    if not found:
        await refresh_cache()


async def update_name_in_sheet(old_name: str, new_name: str) -> None:
    sheet = await get_google_sheet()
    loop = asyncio.get_running_loop()
    vals = await loop.run_in_executor(_executor, sheet.get_all_values)

    updates: list[tuple[int, int]] = []
    for r, raw_row in enumerate(vals, start=1):
        if r < DATA_START_ROW:
            continue
        row = normalize_sheet_row(raw_row)
        for c in range(PLACE_COL_START, PLACE_COL_END + 1):
            if len(row) >= c and row[c - 1] == old_name:
                updates.append((r, c))

    for row_idx, col_idx in updates:
        try:
            await loop.run_in_executor(_executor, sheet.update_cell, row_idx, col_idx, new_name)
        except Exception as e:
            logger.error(f"Ошибка обновления имени в ячейке {row_idx},{col_idx}: {e}")

async def init_db() -> None:
    """Инициализирует базу SQLite."""
    async with aiosqlite.connect("users.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                registered_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sent_notifications (
                notification_key TEXT PRIMARY KEY,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()


async def user_exists_by_param(field: str, value: str | int) -> bool:
    allowed_fields = {"telegram_id", "name"}
    if field not in allowed_fields:
        raise ValueError(f"Недопустимое поле: {field}")

    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(f"SELECT 1 FROM users WHERE {field}=?", (value,))
        res = await cursor.fetchone()
        await cursor.close()
        return res is not None


async def user_is_registered(tid: int) -> bool:
    return await user_exists_by_param("telegram_id", tid)


async def name_exists(name: str) -> bool:
    return await user_exists_by_param("name", name)


async def register_user(tid: int, name: str) -> None:
    async with aiosqlite.connect("users.db") as db:
        await db.execute("INSERT INTO users (telegram_id, name) VALUES (?, ?)", (tid, name))
        await db.commit()


async def get_user_name(tid: int) -> str | None:
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT name FROM users WHERE telegram_id=?", (tid,))
        res = await cursor.fetchone()
        await cursor.close()
        return res[0] if res else None


async def update_user_name(tid: int, new_name: str) -> None:
    async with aiosqlite.connect("users.db") as db:
        await db.execute("UPDATE users SET name=? WHERE telegram_id=?", (new_name, tid))
        await db.commit()


async def get_all_users() -> list[tuple[int, str, str]]:
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute(
            "SELECT telegram_id, name, registered_at FROM users ORDER BY registered_at DESC"
        )
        users = await cursor.fetchall()
        await cursor.close()
        return users


async def get_users_count() -> int:
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        count = await cursor.fetchone()
        await cursor.close()
        return count[0] if count else 0

async def claim_notification(notification_key: str) -> bool:
    """Атомарно резервирует уведомление и защищает от повторной отправки."""
    async with aiosqlite.connect("users.db") as db:
        try:
            await db.execute(
                "INSERT INTO sent_notifications (notification_key) VALUES (?)",
                (notification_key,),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def release_notification(notification_key: str) -> None:
    """Разрешает повторную попытку, если отправка завершилась ошибкой."""
    async with aiosqlite.connect("users.db") as db:
        await db.execute(
            "DELETE FROM sent_notifications WHERE notification_key=?",
            (notification_key,),
        )
        await db.commit()


def private_chat_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_chat and update.effective_chat.type != "private":
            try:
                if update.message:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=update.message.message_id,
                    )
            except Exception:
                pass

            if update.effective_user:
                await context.bot.send_message(
                    chat_id=update.effective_user.id,
                    text="Пожалуйста, используйте бота в личных сообщениях.",
                )
            return
        return await func(update, context, *args, **kwargs)

    return wrapper

def registration_required(func):
    @wraps(func)
    @private_chat_only
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        uid = update.effective_user.id
        if not await user_is_registered(uid):
            if update.message:
                await update.message.reply_text("Пожалуйста, сначала зарегистрируйтесь через /start")
            elif update.callback_query:
                await update.callback_query.message.reply_text(
                    "Пожалуйста, сначала зарегистрируйтесь через /start"
                )
            return
        return await func(update, context, *args, **kwargs)

    return wrapper


def admin_only(func):
    @wraps(func)
    @private_chat_only
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        uid = update.effective_user.id
        if uid not in admin_sessions:
            if update.message:
                await update.message.reply_text("Доступ запрещён. Войдите в админский режим через /login")
            elif update.callback_query:
                await update.callback_query.message.reply_text(
                    "Доступ запрещён. Войдите в админский режим через /login"
                )
            return
        return await func(update, context, *args, **kwargs)

    return wrapper


def error_handler(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Unhandled error in {func.__name__}: {e}")
            try:
                if update:
                    if update.message and update.effective_chat and update.effective_chat.type == "private":
                        await update.message.reply_text("Произошла ошибка, попробуйте позже.")
                    elif update.callback_query:
                        await update.callback_query.message.reply_text("Произошла ошибка, попробуйте позже.")
            except Exception:
                pass

    return wrapper


def is_command_message(update: Update) -> bool:
    if update.message and update.message.entities:
        for entity in update.message.entities:
            if entity.type == "bot_command":
                return True
    return False

@error_handler
async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаляет команды из группы и пишет пользователю в личку."""
    if is_command_message(update):
        try:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=update.message.message_id,
            )
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение с командой: {e}")

        user_id = update.effective_user.id
        command_text = update.message.text or "Команда получена"
        await context.bot.send_message(chat_id=user_id, text=f"Вы вызвали команду: {command_text}")

@error_handler
@registration_required
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    commands_info = (
        "📚 Доступные команды бота:\n"
        "/start - Запустить бота и зарегистрироваться\n"
        "/signup - Записаться на занятие\n"
        "/myrecord - Мои записи\n"
        "/cancelrecord - Отмена записи\n"
        "/change_name - Сменить имя\n"
        "/remindme - Включить/выключить напоминания\n"
        "/help - Показать это сообщение\n"
    )
    await update.message.reply_text(commands_info)


@error_handler
@private_chat_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id

    if await user_is_registered(uid):
        await update.message.reply_text("Вы уже зарегистрированы. Используйте команды бота.")
        await set_bot_commands_global(context.application, uid)
        return ConversationHandler.END

    await update.message.reply_text(
        "Добро пожаловать! Пожалуйста, введите имя для регистрации.\n"
        "Для отмены регистрации введите /cancel"
    )
    await set_bot_commands_global(context.application, uid)
    return REGISTER_NAME


@error_handler
@private_chat_only
async def register_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.text and update.message.text.strip() == "/cancel":
        await update.message.reply_text("Регистрация отменена.")
        return ConversationHandler.END

    uid = update.effective_user.id
    name = update.message.text.strip()

    if not name:
        await update.message.reply_text("Имя пустое, попробуйте ещё раз. Для отмены введите /cancel")
        return REGISTER_NAME

    if name.startswith("/"):
        await update.message.reply_text("Пожалуйста, введите имя, а не команду. Для отмены введите /cancel")
        return REGISTER_NAME

    if await name_exists(name):
        await update.message.reply_text("Это имя занято, введите другое. Для отмены введите /cancel")
        return REGISTER_NAME

    try:
        await register_user(uid, name)
        await update.message.reply_text(f"✅ Регистрация выполнена! Ваше имя: {name}")
        await set_bot_commands_global(context.application, uid)
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Ошибка при регистрации пользователя {uid}: {e}")
        await update.message.reply_text("❌ Ошибка при регистрации, попробуйте позже.")
        return ConversationHandler.END


@error_handler
async def cancel_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Операция отменена.")
    return ConversationHandler.END


@error_handler
@registration_required
async def change_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Введите новое имя:\nДля отмены введите /cancel")
    return CHANGE_NAME


@error_handler
@registration_required
async def process_new_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.text and update.message.text.strip() == "/cancel":
        await update.message.reply_text("Смена имени отменена.")
        return ConversationHandler.END

    uid = update.effective_user.id
    new_name = update.message.text.strip()

    if not new_name:
        await update.message.reply_text("Имя пустое. Попробуйте снова. Для отмены введите /cancel")
        return CHANGE_NAME

    if new_name.startswith("/"):
        await update.message.reply_text("Пожалуйста, введите имя, а не команду. Для отмены введите /cancel")
        return CHANGE_NAME

    if await name_exists(new_name):
        await update.message.reply_text("Имя уже занято, попробуйте другое. Для отмены введите /cancel")
        return CHANGE_NAME

    old_name = await get_user_name(uid)
    await update.message.reply_text("Обновляю имя в базе и таблице...")

    try:
        await update_user_name(uid, new_name)
        if old_name:
            await update_name_in_sheet(old_name, new_name)
        if uid in user_sessions:
            user_sessions[uid]["name"] = new_name
        await refresh_cache()
    except Exception as e:
        logger.error(f"Ошибка изменения имени: {e}")
        await update.message.reply_text("❌ Ошибка при изменении имени, попробуйте позже.")
        return ConversationHandler.END

    await update.message.reply_text(f"✅ Имя успешно изменено на: {new_name}")
    await set_bot_commands_global(context.application, uid)
    return ConversationHandler.END


@error_handler
@registration_required
async def signup_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запись на занятие: показывает доступные занятия."""
    await update.message.reply_text("Получаю список доступных занятий...")

    uid = update.effective_user.id
    name = await get_user_name(uid)
    if not name:
        await update.message.reply_text("Пользователь не найден. Пройдите /start заново.")
        return ConversationHandler.END

    await ensure_user_in_cache(name)
    rows = await get_cached_data()

    now = datetime.now(MOSCOW_TZ)
    today = now.date()

    available: dict[str, dict] = {}
    keyboard: list[list[InlineKeyboardButton]] = []

    for idx, row in enumerate(rows[DATA_START_ROW - 1:], start=DATA_START_ROW):
        if not is_effective_lesson_row(row):
            continue

        lesson_date = safe_parse_date(row[2])
        lesson_time, teacher = parse_time_and_teacher(row[3])

        if not lesson_date or not lesson_time:
            continue

        lesson_datetime = combine_lesson_datetime(lesson_date, lesson_time)

        if lesson_datetime <= now:
            continue

        # запрет записи менее чем за 2 часа до занятия
        if lesson_date == today and (lesson_datetime - now) < timedelta(hours=2):
            continue

        places = row[PLACE_COL_START - 1:PLACE_COL_END]
        if all(place.strip() for place in places):
            continue

        if name in places:
            continue

        price = get_price_from_row(row)
        weekday = weekday_rus(lesson_date)
        time_str = lesson_time.strftime("%H:%M")

        btn_text = f"{lesson_date.strftime('%d.%m.%Y')} | {time_str} | {weekday}"
        if teacher:
            btn_text += f" | {teacher}"
        if price:
            btn_text += f" | {price} р"

        available[str(idx)] = {
            "date": lesson_date,
            "time": lesson_time,
            "teacher": teacher,
            "price": price,
        }
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"signup_{idx}")])

        logger.info(
            f"[signup_command] row={idx}, date={lesson_date}, time={lesson_time}, "
            f"teacher={teacher!r}, price={price!r}"
        )

    if not keyboard:
        await update.message.reply_text("Свободных мест нет.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Выберите занятие:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    user_sessions[uid] = {"lessons": available, "name": name}
    return CHOOSING_TIME


@error_handler
async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id
    session = user_sessions.get(uid)
    if not session:
        await query.message.reply_text("Сессия устарела, начните заново с /signup")
        return ConversationHandler.END

    row = query.data.split("_")[1]
    if row not in session["lessons"]:
        await query.message.reply_text("Неверный выбор, начните заново с /signup")
        return ConversationHandler.END

    lesson = session["lessons"][row]
    session.update(
        {
            "selected_row": int(row),
            "selected_date": lesson["date"],
            "selected_time": lesson["time"],
            "selected_teacher": lesson["teacher"],
            "selected_price": lesson["price"],
        }
    )

    await query.edit_message_reply_markup(reply_markup=None)

    keyboard = [[
        InlineKeyboardButton("Да", callback_data="confirm_yes"),
        InlineKeyboardButton("Нет", callback_data="confirm_no"),
    ]]

    msg = (
        f"Вы выбрали {lesson['date'].strftime('%d.%m.%Y')} | "
        f"{lesson['time'].strftime('%H:%M')}"
    )
    if lesson["teacher"]:
        msg += f" | {lesson['teacher']}"
    if lesson["price"]:
        msg += f" | {lesson['price']} р"
    msg += "\nПодтверждаете запись?"

    await query.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    return CONFIRMING


@error_handler
async def confirm_signup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id
    session = user_sessions.get(uid)
    if not session:
        await query.message.reply_text("Сессия устарела, начните заново с /signup")
        return ConversationHandler.END

    if query.data != "confirm_yes":
        lessons = session["lessons"]
        keyboard = []

        for r, lesson in lessons.items():
            btn_text = (
                f"{lesson['date'].strftime('%d.%m.%Y')} | "
                f"{lesson['time'].strftime('%H:%M')}"
            )
            if lesson["teacher"]:
                btn_text += f" | {lesson['teacher']}"
            if lesson["price"]:
                btn_text += f" | {lesson['price']} р"

            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"signup_{r}")])

        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("Выберите занятие:", reply_markup=InlineKeyboardMarkup(keyboard))
        return CHOOSING_TIME

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text("Проверяем свободные места и записываем...")

    row = session["selected_row"]
    name = session["name"]

    sheet = await get_google_sheet()
    free_cols = free_places_cache.get(row, [])

    if not free_cols:
        await refresh_cache()
        free_cols = free_places_cache.get(row, [])

    if not free_cols:
        await query.message.reply_text("Свободных мест нет.")
        user_sessions.pop(uid, None)
        return ConversationHandler.END

    col_to_update = free_cols.pop(0)

    loop = asyncio.get_running_loop()
    current_value = await loop.run_in_executor(_executor, lambda: sheet.cell(row, col_to_update).value or "")

    if current_value.strip():
        await refresh_cache()
        await query.message.reply_text("Место уже занято, попробуйте запись снова.")
        user_sessions.pop(uid, None)
        return ConversationHandler.END

    await async_update_cell_no_wait(sheet, row, col_to_update, name)

    free_places_cache[row] = free_cols

    msg = (
        f"✅ Вы записаны на {session['selected_date'].strftime('%d.%m.%Y')} "
        f"в {session['selected_time'].strftime('%H:%M')}"
    )
    if session.get("selected_teacher"):
        msg += f" у {session['selected_teacher']}"
    if session.get("selected_price"):
        msg += f" ({session['selected_price']} р)"

    await query.message.reply_text(msg)

    user_sessions.pop(uid, None)
    await refresh_cache()
    return ConversationHandler.END


@error_handler
@registration_required
async def myrecord_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    name = await get_user_name(uid)
    if not name:
        await update.message.reply_text("Пользователь не найден.")
        return

    await update.message.reply_text("Ищу ваши записи...")
    await ensure_user_in_cache(name)
    rows = await get_cached_data()

    today = datetime.now(MOSCOW_TZ).date()
    records = []

    for row in rows[DATA_START_ROW - 1:]:
        if not is_effective_lesson_row(row):
            continue

        lesson_date = safe_parse_date(row[2])
        lesson_time, teacher = parse_time_and_teacher(row[3])

        if not lesson_date or not lesson_time:
            continue

        if lesson_date < today:
            continue

        places = row[PLACE_COL_START - 1:PLACE_COL_END]
        if name in places:
            price = get_price_from_row(row)
            msg = (
                f"{lesson_date.strftime('%d.%m.%Y')} | "
                f"{lesson_time.strftime('%H:%M')} | "
                f"{weekday_rus(lesson_date)}"
            )
            if teacher:
                msg += f" | {teacher}"
            if price:
                msg += f" | {price} р"
            records.append(msg)

    if records:
        await update.message.reply_text("📋 Ваши записи:\n" + "\n".join(records))
    else:
        await update.message.reply_text("❌ У вас нет записей.")


@error_handler
@registration_required
async def cancelrecord_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    name = await get_user_name(uid)
    if not name:
        await update.message.reply_text("Пользователь не найден.")
        return

    await ensure_user_in_cache(name)
    rows = await get_cached_data()
    now = datetime.now(MOSCOW_TZ)

    occupied = []

    for i, row in enumerate(rows[DATA_START_ROW - 1:], start=DATA_START_ROW):
        if not is_effective_lesson_row(row):
            continue

        lesson_date = safe_parse_date(row[2])
        lesson_time, teacher = parse_time_and_teacher(row[3])
        price = get_price_from_row(row)

        if not lesson_date or not lesson_time:
            continue

        lesson_dt = combine_lesson_datetime(lesson_date, lesson_time)

        if lesson_dt < now:
            continue

        places = row[PLACE_COL_START - 1:PLACE_COL_END]
        if name in places:
            occupied.append((i, lesson_dt, teacher, price))

    if not occupied:
        await update.message.reply_text("❌ У вас нет записей для отмены.")
        return

    keyboard = []
    to_cancel_data = {}

    for r, lesson_dt, teacher, price in occupied:
        btn_text = (
            f"{lesson_dt.strftime('%d.%m.%Y')} | "
            f"{lesson_dt.strftime('%H:%M')} | "
            f"{weekday_rus(lesson_dt.date())}"
        )
        if teacher:
            btn_text += f" | {teacher}"
        if price:
            btn_text += f" | {price} р"
        btn_text += " — Отменить"

        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"cancel_confirm_{r}")])
        to_cancel_data[r] = {
            "lesson_dt": lesson_dt,
            "teacher": teacher,
            "price": price,
        }

    await update.message.reply_text(
        "Выберите запись для отмены:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    user_sessions[uid] = {"to_cancel": to_cancel_data, "name": name}


@error_handler
async def cancel_confirm_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id
    session = user_sessions.get(uid)

    if not session or "to_cancel" not in session:
        await query.message.reply_text("Сессия истекла. Введите /cancelrecord заново.")
        return

    row = int(query.data.split("_")[2])
    lesson_info = session["to_cancel"].get(row)
    if not lesson_info:
        await query.message.reply_text("Сессия истекла. Введите /cancelrecord заново.")
        return

    lesson_dt = lesson_info["lesson_dt"]
    now = datetime.now(MOSCOW_TZ)

    if (lesson_dt - now).total_seconds() < 7200:
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("❌ Отмена невозможна менее чем за 2 часа до занятия.")
        user_sessions.pop(uid, None)
        return

    buttons = [[
        InlineKeyboardButton("Да", callback_data=f"cancel_yes_{row}"),
        InlineKeyboardButton("Нет", callback_data="cancel_no"),
    ]]

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(
        f"Вы уверены, что хотите отменить запись на занятие {lesson_dt.strftime('%d.%m.%Y %H:%M')}?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@error_handler
async def cancel_yes_no_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id
    session = user_sessions.get(uid)
    if not session:
        await query.message.reply_text("Сессия устарела. Введите /cancelrecord заново.")
        return

    if query.data == "cancel_no":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("Отмена записи отменена.")
        user_sessions.pop(uid, None)
        return

    row = int(query.data.split("_")[2])
    name = await get_user_name(uid)
    if not name:
        await query.message.reply_text("Пользователь не найден.")
        user_sessions.pop(uid, None)
        return

    sheet = await get_google_sheet()
    loop = asyncio.get_running_loop()

    row_values = await loop.run_in_executor(_executor, lambda: sheet.row_values(row))
    places = normalize_sheet_row(row_values)[PLACE_COL_START - 1:PLACE_COL_END]

    if name not in places:
        await query.message.reply_text("Запись не найдена.")
        user_sessions.pop(uid, None)
        return

    idx_to_free = None
    for idx, val in enumerate(places, start=PLACE_COL_START):
        if val == name:
            idx_to_free = idx
            break

    if idx_to_free is None:
        await query.message.reply_text("Ошибка отмены записи.")
        user_sessions.pop(uid, None)
        return

    current_value = await loop.run_in_executor(_executor, lambda: sheet.cell(row, idx_to_free).value or "")
    if current_value.strip() != name:
        await query.message.reply_text("Данные изменились. Отмена невозможна.")
        user_sessions.pop(uid, None)
        return

    await query.message.reply_text("Отменяю запись...")
    await async_update_cell_no_wait(sheet, row, idx_to_free, "")

    free_places_cache.setdefault(row, []).append(idx_to_free)
    free_places_cache[row] = sorted(set(free_places_cache[row]))

    row_values_after = await loop.run_in_executor(_executor, lambda: sheet.row_values(row))
    row_after = normalize_sheet_row(row_values_after)

    lesson_date = safe_parse_date(row_after[2] if len(row_after) > 2 else "")
    lesson_time, _teacher = parse_time_and_teacher(row_after[3] if len(row_after) > 3 else "")

    await query.edit_message_reply_markup(reply_markup=None)

    if lesson_date and lesson_time:
        msg = f"✅ Запись {lesson_date.strftime('%d.%m.%Y')} | {lesson_time.strftime('%H:%M')} отменена."
    else:
        msg = "✅ Запись отменена."

    await query.message.reply_text(msg)

    user_sessions.pop(uid, None)
    await refresh_cache()


@error_handler
@registration_required
async def remindme_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    enabled = not remind_settings.get(uid, False)
    remind_settings[uid] = enabled

    if enabled:
        existing = reminder_tasks.get(uid)
        if existing is None or existing.done():
            task = context.application.create_task(
                reminder_task(uid, context),
                name=f"reminder:{uid}",
            )
            reminder_tasks[uid] = task
        await update.message.reply_text("🔔 Персональные напоминания включены.")
    else:
        task = reminder_tasks.pop(uid, None)
        if task and not task.done():
            task.cancel()
        await update.message.reply_text("🔕 Персональные напоминания выключены.")


async def reminder_task(uid: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Одна фоновая задача персональных напоминаний на пользователя."""
    name = await get_user_name(uid)
    if not name:
        remind_settings[uid] = False
        reminder_tasks.pop(uid, None)
        return

    try:
        while remind_settings.get(uid, False):
            now = datetime.now(MOSCOW_TZ)
            data = await get_cached_data()

            for row_idx, row in enumerate(data[DATA_START_ROW - 1:], start=DATA_START_ROW):
                if not is_effective_lesson_row(row):
                    continue

                lesson_date = safe_parse_date(row[2])
                lesson_time, teacher = parse_time_and_teacher(row[3])
                if not lesson_date or not lesson_time:
                    continue

                places = row[PLACE_COL_START - 1:PLACE_COL_END]
                if name not in places:
                    continue

                lesson_datetime = combine_lesson_datetime(lesson_date, lesson_time)
                diff = (lesson_datetime - now).total_seconds()
                if not 14 * 60 <= diff <= 16 * 60:
                    continue

                notification_key = (
                    f"personal:{uid}:{row_idx}:{lesson_datetime.isoformat()}"
                )
                if not await claim_notification(notification_key):
                    continue

                text = (
                    f"⏰ Напоминание: занятие начнётся через 15 минут "
                    f"({lesson_date.strftime('%d.%m.%Y')} в {lesson_time.strftime('%H:%M')})"
                )
                if teacher:
                    text += f" у {teacher}"

                try:
                    await context.bot.send_message(chat_id=uid, text=text)
                except Exception:
                    await release_notification(notification_key)
                    raise

            await asyncio.sleep(30)

    except asyncio.CancelledError:
        logger.info("Задача напоминаний пользователя %s остановлена", uid)
        raise
    except Exception as e:
        logger.exception("Ошибка в reminder_task пользователя %s: %s", uid, e)
    finally:
        reminder_tasks.pop(uid, None)


@error_handler
async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query_text = update.inline_query.query
    if not query_text:
        return

    if update.inline_query.from_user.id not in admin_sessions:
        return

    try:
        users = await get_all_users()
        results = []

        for i, (user_id, name, registered_at) in enumerate(users):
            if query_text.lower() in name.lower() or query_text in str(user_id):
                reg_date = datetime.fromisoformat(registered_at) if registered_at else "неизвестно"
                reg_date_str = reg_date.strftime("%d.%m.%Y %H:%M") if isinstance(reg_date, datetime) else str(reg_date)

                results.append(
                    InlineQueryResultArticle(
                        id=str(i),
                        title=f"{name} (ID: {user_id})",
                        description=f"Зарегистрирован: {reg_date_str}",
                        input_message_content=InputTextMessageContent(
                            message_text=f"👤 Пользователь: {name}\n🆔 ID: {user_id}\n📅 Регистрация: {reg_date_str}"
                        ),
                    )
                )

        await update.inline_query.answer(results, cache_time=1)

    except Exception as e:
        logger.error(f"Ошибка в inline query: {e}")
@error_handler
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid in admin_sessions:
        await update.message.reply_text("Вы уже в админском режиме.")
        return

    admin_password_pending.add(uid)
    await update.message.reply_text("Введите админский пароль:")


@error_handler
@private_chat_only
async def password_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid not in admin_password_pending:
        return

    password = update.message.text.strip()
    if hashlib.sha256(password.encode("utf-8")).hexdigest() == ADMIN_PASSWORD_HASH:
        admin_sessions.add(uid)
        admin_password_pending.discard(uid)
        await update.message.reply_text("✅ Пароль верен. Админский режим активирован.")
        await set_bot_commands_global(context.application, uid)
    else:
        admin_password_pending.discard(uid)
        await update.message.reply_text("❌ Неверный пароль. Попробуйте /login снова.")


@error_handler
@admin_only
async def exit_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    if uid in admin_sessions:
        admin_sessions.remove(uid)
        await update.message.reply_text("✅ Вы вышли из админского режима.")
        await set_bot_commands_global(context.application, uid)
    else:
        await update.message.reply_text("❌ Вы не в админском режиме.")


@error_handler
@admin_only
async def info_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Получаю сегодняшнее расписание...")
    await refresh_cache()

    rows = await get_cached_data()
    today = datetime.now(MOSCOW_TZ).date()
    msgs = []

    for idx, row in enumerate(rows[DATA_START_ROW - 1:], start=DATA_START_ROW):
        if not is_effective_lesson_row(row):
            continue

        lesson_date = safe_parse_date(row[2])
        lesson_time, teacher = parse_time_and_teacher(row[3])
        price = get_price_from_row(row)

        logger.info(
            f"[info_today] row={idx}, raw_date={row[2]!r}, raw_d={row[3]!r}, "
            f"parsed_date={lesson_date}, parsed_time={lesson_time}, price={price!r}"
        )

        if lesson_date != today or not lesson_time:
            continue

        participants = [p for p in row[PLACE_COL_START - 1:PLACE_COL_END] if p.strip()]
        participants_str = ", ".join(participants) if participants else "(никого не записано)"

        msg = (
            f"{lesson_date.strftime('%d.%m.%Y')} | "
            f"{lesson_time.strftime('%H:%M')} | "
            f"{weekday_rus(lesson_date)}"
        )
        if teacher:
            msg += f" | {teacher}"
        msg += f" | {participants_str}"
        if price:
            msg += f" | {price} р"

        msgs.append(msg)

    if msgs:
        await update.message.reply_text("\n".join(msgs))
    else:
        await update.message.reply_text("❌ Занятий сегодня нет.")


@error_handler
@admin_only
async def send_morning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Отправляю утреннее сообщение в группу...")
    await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=MORNING_MESSAGE)
    await update.message.reply_text("✅ Утреннее сообщение отправлено.")


@error_handler
@admin_only
async def send_evening(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    today = datetime.now(MOSCOW_TZ).date()
    rows = await get_cached_data()

    row_data = None
    for row in rows[DATA_START_ROW - 1:]:
        if not is_effective_lesson_row(row):
            continue
        lesson_date = safe_parse_date(row[2])
        if lesson_date == today:
            row_data = row
            break

    if not row_data:
        await update.message.reply_text("❌ Занятий сегодня нет. Вечернее сообщение не отправлено.")
        return

    val_n = get_price_from_row(row_data) or "Н/Д"
    evening_msg = f"Подводим итоги — по {val_n} р. Приносите наличными до конца недели."
    await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=evening_msg)
    await update.message.reply_text("✅ Вечернее сообщение отправлено.")


@error_handler
@admin_only
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        users = await get_all_users()
        total_count = await get_users_count()

        if not users:
            await update.message.reply_text("📊 Зарегистрированных пользователей пока нет.")
            return

        users_list = []
        for i, (user_id, name, registered_at) in enumerate(users, 1):
            reg_date = datetime.fromisoformat(registered_at) if registered_at else "неизвестно"
            reg_date_str = reg_date.strftime("%d.%m.%Y") if isinstance(reg_date, datetime) else str(reg_date)
            users_list.append(f"{i}. {name} (ID: {user_id}) - зарегистрирован {reg_date_str}")

        message = f"📊 Всего зарегистрированных пользователей: {total_count}\n\n" + "\n".join(users_list)

        if len(message) > 4000:
            parts = [message[i:i + 4000] for i in range(0, len(message), 4000)]
            for part in parts:
                await update.message.reply_text(part)
        else:
            await update.message.reply_text(message)

    except Exception as e:
        logger.error(f"Ошибка при получении списка пользователей: {e}")
        await update.message.reply_text("❌ Ошибка при получении списка пользователей.")


@error_handler
@admin_only
async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Получаю список пользователей...")
    async with aiosqlite.connect("users.db") as db:
        cursor = await db.execute("SELECT telegram_id, name FROM users ORDER BY name")
        users = await cursor.fetchall()
        await cursor.close()

    if not users:
        await update.message.reply_text("❌ Пользователей нет.")
        return

    keyboard = [
        [InlineKeyboardButton(f"{name} (ID {tid})", callback_data=f"delete_user_{tid}")]
        for tid, name in users
    ]

    await update.message.reply_text(
        "👥 Пользователи:\n(нажмите кнопку для удаления)",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


@error_handler
@admin_only
async def admin_users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data.startswith("delete_user_"):
        del_uid = int(query.data.split("_")[-1])
        async with aiosqlite.connect("users.db") as db:
            await db.execute("DELETE FROM users WHERE telegram_id=?", (del_uid,))
            await db.commit()

        await query.edit_message_text(f"✅ Пользователь с ID {del_uid} удалён.")
        await refresh_cache()

async def set_bot_commands_global(application: Application, user_id: int | None = None) -> None:
    """Устанавливает команды для конкретного пользователя или глобально."""
    if user_id:
        if user_id in admin_sessions:
            cmds = [
                BotCommand("info_today", "Расписание на сегодня"),
                BotCommand("send_morning", "Утренняя рассылка"),
                BotCommand("send_evening", "Вечерняя рассылка"),
                BotCommand("admin_users", "Управление пользователями"),
                BotCommand("exit", "Выйти из админского режима"),
                BotCommand("users", "Список пользователей"),
                BotCommand("help", "Помощь"),
            ]
        elif await user_is_registered(user_id):
            cmds = [
                BotCommand("signup", "Записаться на занятие"),
                BotCommand("myrecord", "Мои записи"),
                BotCommand("cancelrecord", "Отмена записи"),
                BotCommand("change_name", "Сменить имя"),
                BotCommand("remindme", "Напоминания"),
                BotCommand("help", "Помощь"),
            ]
        else:
            cmds = [
                BotCommand("start", "Запустить бота"),
                BotCommand("help", "Помощь"),
            ]

        try:
            await application.bot.set_my_commands(cmds, BotCommandScopeChat(user_id))
        except Exception as e:
            logger.error(f"Failed to set commands for user {user_id}: {e}")
    else:
        try:
            await application.bot.set_my_commands([BotCommand("start", "Запустить бота")])
        except Exception as e:
            logger.error(f"Failed to set global commands: {e}")

async def background_cache_updater() -> None:
    while True:
        try:
            await refresh_cache()
            logger.info("Фоновое обновление кэша прошло успешно")
        except Exception as e:
            logger.error(f"Ошибка фонового обновления: {e}")
        await asyncio.sleep(300)


async def scheduled_send_morning(application: Application) -> None:
    today = datetime.now(MOSCOW_TZ).date()
    rows = await get_cached_data()

    has_lessons = any(
        is_effective_lesson_row(row) and safe_parse_date(row[2]) == today
        for row in rows[DATA_START_ROW - 1:]
    )
    if not has_lessons:
        logger.info("[scheduled_send_morning] Нет занятий, сообщение не отправлено.")
        return

    notification_key = f"group:morning:{today.isoformat()}"
    if not await claim_notification(notification_key):
        logger.warning("[scheduled_send_morning] Дубликат заблокирован: %s", notification_key)
        return

    try:
        await application.bot.send_message(chat_id=GROUP_CHAT_ID, text=MORNING_MESSAGE)
    except Exception:
        await release_notification(notification_key)
        raise

    logger.info("[scheduled_send_morning] Сообщение отправлено.")


async def scheduled_send_evening(application: Application) -> None:
    today = datetime.now(MOSCOW_TZ).date()
    rows = await get_cached_data()

    row_data = next(
        (
            row
            for row in rows[DATA_START_ROW - 1:]
            if is_effective_lesson_row(row) and safe_parse_date(row[2]) == today
        ),
        None,
    )
    if not row_data:
        logger.info("[scheduled_send_evening] Нет занятий, сообщение не отправлено.")
        return

    notification_key = f"group:evening:{today.isoformat()}"
    if not await claim_notification(notification_key):
        logger.warning("[scheduled_send_evening] Дубликат заблокирован: %s", notification_key)
        return


    val_n = get_price_from_row(row_data) or "Н/Д"
    evening_msg = f"Подводим итоги — по {val_n} р. Приносите наличными до конца недели."
    try:
        await application.bot.send_message(chat_id=GROUP_CHAT_ID, text=evening_msg)
    except Exception:
        await release_notification(notification_key)
        raise

    logger.info("[scheduled_send_evening] Сообщение отправлено.")


async def start_scheduler(application: Application) -> None:
    global scheduler

    if scheduler is not None and scheduler.running:
        logger.warning("APScheduler уже запущен — повторный запуск пропущен.")
        return

    scheduler = AsyncIOScheduler(
        timezone=MOSCOW_TZ,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 300,
        },
    )
    scheduler.add_job(
        scheduled_send_morning,
        "cron",
        hour=11,
        minute=0,
        args=[application],
        id="morning_message",
        replace_existing=True,
    )
    scheduler.add_job(
        scheduled_send_evening,
        "cron",
        hour=18,
        minute=0,
        args=[application],
        id="evening_message",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("APScheduler запущен. Jobs: %s", [job.id for job in scheduler.get_jobs()])


async def on_startup(application: Application) -> None:
    await init_db()
    await refresh_cache()

    if application.bot_data.get("cache_updater_started"):
        logger.warning("Фоновое обновление кэша уже запущено.")
    else:
        application.bot_data["cache_updater_started"] = True
        application.create_task(background_cache_updater(), name="cache-updater")

    await start_scheduler(application)


async def on_shutdown(application: Application) -> None:
    global scheduler

    for task in list(reminder_tasks.values()):
        if not task.done():
            task.cancel()
    reminder_tasks.clear()

    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=False)
        scheduler = None
        logger.info("APScheduler остановлен.")

    _executor.shutdown(wait=False, cancel_futures=True)
    release_instance_lock()


def main() -> None:
    acquire_instance_lock()

    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .connect_timeout(60)
        .read_timeout(60)
        .write_timeout(60)
        .pool_timeout(60)
        .build()
    )

    registration_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REGISTER_NAME: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), register_name_handler),
                CommandHandler("cancel", cancel_registration),
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_registration)],
    )


    change_name_handler = ConversationHandler(
        entry_points=[CommandHandler("change_name", change_name)],
        states={
            CHANGE_NAME: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), process_new_name),
                CommandHandler("cancel", cancel_registration),
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_registration)],
    )

    application.add_handler(registration_handler)
    application.add_handler(change_name_handler)

    application.add_handler(
        MessageHandler(
            filters.ALL & (filters.ChatType.GROUP | filters.ChatType.SUPERGROUP),
            group_message_handler,
        )
    )

    application.add_handler(InlineQueryHandler(inline_query))

    application.add_handler(CommandHandler("remindme", remindme_command))
    application.add_handler(CommandHandler("signup", signup_command))
    application.add_handler(CallbackQueryHandler(choose_time, pattern=r"^signup_\d+$"))
    application.add_handler(CallbackQueryHandler(confirm_signup, pattern=r"^confirm_(yes|no)$"))
    application.add_handler(CommandHandler("myrecord", myrecord_command))
    application.add_handler(CommandHandler("cancelrecord", cancelrecord_command))
    application.add_handler(CallbackQueryHandler(cancel_confirm_selected, pattern=r"^cancel_confirm_\d+$"))
    application.add_handler(CallbackQueryHandler(cancel_yes_no_handler, pattern=r"^cancel_yes_\d+$|^cancel_no$"))
    application.add_handler(CommandHandler("users", users_command))

    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), password_received))
    application.add_handler(CommandHandler("exit", exit_admin))
    application.add_handler(CommandHandler("info_today", info_today))
    application.add_handler(CommandHandler("send_morning", send_morning))
    application.add_handler(CommandHandler("send_evening", send_evening))
    application.add_handler(CommandHandler("admin_users", admin_users))
    application.add_handler(CallbackQueryHandler(admin_users_callback, pattern=r"^delete_user_\d+$"))

    application.add_handler(CommandHandler("help", help_command))

    application.run_polling(
        drop_pending_updates=True,
        timeout=60,
        read_timeout=60,
        write_timeout=60,
        connect_timeout=60,
        pool_timeout=60,
    )


if __name__ == "__main__":
    main()
