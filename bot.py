import os
import logging
from datetime import datetime
import pytz
import csv
import json
from pathlib import Path
import random

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
# 🔒 Не логируем HTTP-запросы с токеном
logging.getLogger("httpx").setLevel(logging.WARNING)

# --- Timezone ---
TIMEZONE = pytz.timezone("Asia/Tbilisi")

# --- Files ---
TRACKS_FILE = "tracks.csv"
HISTORY_FILE = "user_history.json"
VOTES_FILE = "votes.json"
SUBSCRIBERS_FILE = "subscribers.json"

TRACKS_CACHE = None

# Admin user id (set as Railway variable ADMIN_USER_ID)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))


# ---------- Работа с треками ----------

def load_tracks():
    """
    Загружаем треки из tracks.csv (кэшируем в памяти).
    Ожидаются поля:
      id,date,slot,title&artist,video_link,audio,message
    """
    global TRACKS_CACHE
    if TRACKS_CACHE is not None:
        return TRACKS_CACHE

    path = Path(TRACKS_FILE)
    if not path.exists():
        logger.warning("Tracks file %s not found", TRACKS_FILE)
        TRACKS_CACHE = []
        return TRACKS_CACHE

    tracks = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("id"):
                continue

            tracks.append({
                "id": str(row.get("id", "")).strip(),
                "date": (row.get("date") or "").strip(),
                "slot": (row.get("slot") or "").strip(),
                # В CSV колонка называется "title&artist", в коде используем удобное имя
                "title_artist": (row.get("title&artist") or "").strip(),
                "video_link": (row.get("video_link") or "").strip(),
                "audio": (row.get("audio") or "").strip(),
                "message": (row.get("message") or "").strip(),
            })

    TRACKS_CACHE = tracks
    logger.info("Loaded %d tracks from %s", len(tracks), TRACKS_FILE)
    return TRACKS_CACHE


# ---------- История треков по пользователям (/today) ----------

def load_history():
    """
    История: {chat_id: {last_date, track_id, used_track_ids: []}}
    """
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load history: %s", e)
        return {}


def save_history(history: dict):
    path = Path(HISTORY_FILE)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save history: %s", e)


def get_local_now():
    return datetime.now(TIMEZONE)


def is_window_open(now: datetime) -> bool:
    """
    Открыто ли «окошко» 08:00–10:00 (по TIMEZONE).
    08:00 включительно, 10:00 не включительно.
    """
    return 8 <= now.hour < 10


def choose_track_for_user(chat_id: int, today_date: str):
    """
    Выбор трека для конкретного чата/пользователя на сегодня.

    Логика:
    - если уже выдавали трек сегодня -> вернуть тот же;
    - иначе выбрать случайный из тех, что ещё НЕ были у пользователя;
    - если все уже были, начать новый круг со всех треков.

    Важно: используется только поле id и остальные поля трека для отображения.
    """
    tracks = load_tracks()
    if not tracks:
        return None

    history = load_history()
    key = str(chat_id)
    user_entry = history.get(key)

    # Уже был трек сегодня -> возвращаем его
    if user_entry and user_entry.get("last_date") == today_date:
        track_id = user_entry.get("track_id")
        for t in tracks:
            if t["id"] == track_id:
                return t

    # Иначе выбираем новый
    used_ids = set(user_entry.get("used_track_ids", [])) if user_entry else set()
    available = [t for t in tracks if t["id"] not in used_ids]

    if not available:
        used_ids = set()
        available = tracks[:]

    chosen = random.choice(available)
    used_ids.add(chosen["id"])

    history[key] = {
        "last_date": today_date,
        "track_id": chosen["id"],
        "used_track_ids": list(used_ids),
    }
    save_history(history)
    return chosen


def build_main_keyboard():
    keyboard = [
        [KeyboardButton("🎵 Open today’s track")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def build_vote_inline_keyboard(track_id: str):
    """
    Инлайн-кнопка для голосования за трек.
    """
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❤️ I like this track", callback_data=f"VOTE:{track_id}")]]
    )


# ---------- Голосование ----------

def load_votes():
    """
    Структура: {track_id: {"likes": int, "voters": [user_id, ...]}}
    """
    path = Path(VOTES_FILE)
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load votes: %s", e)
        return {}


def save_votes(votes: dict):
    path = Path(VOTES_FILE)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(votes, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save votes: %s", e)


async def vote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка нажатия на кнопку "❤️ I like this track".
    Один пользователь может поставить лайк одному треку только один раз.
    """
    query = update.callback_query
    data = query.data or ""
    await query.answer()

    if not data.startswith("VOTE:"):
        return

    track_id = data.split(":", 1)[1]
    user_id = query.from_user.id

    votes = load_votes()
    entry = votes.get(track_id, {"likes": 0, "voters": []})
    voters = set(entry.get("voters", []))

    if user_id in voters:
        await query.answer("You already voted for this track 💿", show_alert=False)
        return

    voters.add(user_id)
    entry["likes"] = int(entry.get("likes", 0)) + 1
    entry["voters"] = list(voters)
    votes[track_id] = entry
    save_votes(votes)

    logger.info("User %s liked track %s", user_id, track_id)
    await query.answer("Thank you for your vote! ❤️", show_alert=False)


# ---------- Subscribers (optional, for future broadcast) ----------

def load_subscribers():
    path = Path(SUBSCRIBERS_FILE)
    if not path.exists():
        return set()
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
            return set(map(int, data.get("chat_ids", [])))
    except Exception as e:
        logger.error("Failed to load subscribers: %s", e)
        return set()


def save_subscribers(chat_ids: set[int]):
    path = Path(SUBSCRIBERS_FILE)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump({"chat_ids": sorted(list(chat_ids))}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save subscribers: %s", e)


# ---------- Admin: /setaudio ----------

async def setaudio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Админская команда: включает режим ожидания аудиофайла.
    После этого админ отправляет аудио, бот возвращает file_id.
    """
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        await update.message.reply_text("You are not allowed to use /setaudio.")
        return

    context.user_data["awaiting_audio"] = True
    await update.message.reply_text(
        "🎧 Send me the audio file now (as an Audio). "
        "I’ll reply with its file_id for tracks.csv.\n\n"
        "Tip: you can also send an audio with caption /setaudio."
    )


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Принимает аудио и возвращает file_id.
    Работает в двух режимах:
    1) после команды /setaudio (awaiting_audio=True)
    2) если в подписи к аудио есть /setaudio
    """
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        return

    msg = update.message
    if msg is None or msg.audio is None:
        return

    caption = (msg.caption or "").strip()
    awaiting = context.user_data.get("awaiting_audio", False)
    caption_mode = caption.startswith("/setaudio")

    if not (awaiting or caption_mode):
        await msg.reply_text(
            "If you want to save this audio’s file_id, send /setaudio first "
            "or add caption /setaudio to the audio message."
        )
        return

    audio = msg.audio
    file_id = audio.file_id
    unique_id = audio.file_unique_id

    context.user_data["awaiting_audio"] = False

    logger.info("Admin uploaded audio. file_id=%s unique_id=%s", file_id, unique_id)

    await msg.reply_text(
        "✅ Audio saved.\n\n"
        f"file_id:\n{file_id}\n\n"
        f"(debug) file_unique_id: {unique_id}\n\n"
        "👉 Put this file_id into tracks.csv column `audio`."
    )


# ---------- Handlers ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎄 *Advent Music Calendar*\n\n"
        "Этот бот будет присылать вам *2–3 музыкальных трека каждый день* "
        "с **16 по 26 декабря**.\n\n"
        "В каждом сообщении вы получите:\n"
        "• название трека\n"
        "• ссылку на клип или аудио\n"
        "Чтобы получать ежедневные треки, нажмите кнопку ниже 👇"
    )

    await update.message.reply_markdown(
        text,
        reply_markup=build_start_keyboard(),
    )

async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = get_local_now()
    local_time_str = now.strftime("%H:%M")
    today_date = now.date().isoformat()

    if not is_window_open(now):
        await update.message.reply_text(
            f"The Advent window is closed now. ⏰\n\n"
            f"You can open today’s track between 08:00 and 10:00.\n"
            f"Current time: {local_time_str}."
        )
        return

    chat_id = update.effective_chat.id
    track = choose_track_for_user(chat_id, today_date)

    if track is None:
        await update.message.reply_text(
            "There are no tracks in the calendar yet. "
            "Please ask the organizer to add some to tracks.csv. 🎧"
        )
        return

    title_artist = track.get("title_artist", "").strip() or "(no title)"
    video_link = track.get("video_link", "").strip()
    message = track.get("message", "").strip()
    track_id = track.get("id", "")

    logger.info("Chat %s opened track %s for %s", chat_id, track_id, today_date)

    # message может быть пустым — тогда не вставляем лишнюю пустую строку
    msg_block = (message + "\n\n") if message else ""

    link_block = f"🔗 [Watch / Listen here]({video_link})" if video_link else "🔗 (no link)"

    text = (
        f"✨ Advent Music Calendar\n\n"
        f"🎵 *Track of the day:*\n"
        f"_{title_artist}_\n\n"
        f"{msg_block}"
        f"{link_block}\n\n"
        f"If you liked this track, tap ❤️ below!"
    )

    await update.message.reply_markdown(
        text,
        reply_markup=build_vote_inline_keyboard(track_id),
        disable_web_page_preview=False,
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "🔔 Подписаться":
        return await subscribe(update, context)

    if text == "🎵 Open today’s track":
        return await today(update, context)

    await update.message.reply_text(
        "Используйте кнопки ниже, чтобы работать с Advent Music Calendar 🎄"
    )

async def top5(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает топ-5 треков по количеству лайков.
    """
    tracks = load_tracks()
    track_by_id = {t["id"]: t for t in tracks}
    votes = load_votes()

    if not votes:
        await update.message.reply_text("No votes yet. Nobody tapped ❤️ so far. 😊")
        return

    scored = []
    for track_id, info in votes.items():
        likes = int(info.get("likes", 0))
        if likes <= 0:
            continue
        track = track_by_id.get(track_id)
        if not track:
            continue
        scored.append((likes, track))

    if not scored:
        await update.message.reply_text("No tracks with likes yet.")
        return

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:5]

    lines = ["🏆 Top 5 Advent Tracks (by likes):", ""]
    for i, (likes, t) in enumerate(top, start=1):
        lines.append(f"{i}. {t.get('title_artist','(no title)')}  ({likes} ❤️)")
        if t.get("video_link"):
            lines.append(f"   {t['video_link']}")

    await update.message.reply_text("\n".join(lines))


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Админская команда: показывает список ВСЕХ треков и количество лайков.
    Доступна только админу (ADMIN_USER_ID).
    """
    user = update.effective_user
    logger.info("User id: %s", user.id if user else None)
    logger.info("ADMIN_USER_ID: %s", ADMIN_USER_ID)

    if user is None or user.id != ADMIN_USER_ID:
        await update.message.reply_text("You are not allowed to view stats.")
        return

    tracks = load_tracks()
    votes = load_votes()

    if not tracks:
        await update.message.reply_text("No tracks found in tracks.csv.")
        return

    likes_by_id = {t["id"]: int(votes.get(t["id"], {}).get("likes", 0)) for t in tracks}
    tracks_sorted = sorted(tracks, key=lambda t: likes_by_id.get(t["id"], 0), reverse=True)

    lines = ["📊 Advent Music – full stats:", ""]
    for t in tracks_sorted:
        tid = t.get("id", "")
        ta = t.get("title_artist", "(no title)")
        likes = likes_by_id.get(tid, 0)
        lines.append(f"{tid}. {ta}  ({likes} ❤️)")

    await update.message.reply_text("\n".join(lines))


# ---------- Main ----------

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Please set it as an environment variable.")

    application = ApplicationBuilder().token(token).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("top5", top5))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("setaudio", setaudio))

    # Important: audio handler before text handler
    application.add_handler(MessageHandler(filters.AUDIO, handle_audio))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(CallbackQueryHandler(vote_callback, pattern=r"^VOTE:"))

    application.run_polling()


if __name__ == "__main__":
    main()
