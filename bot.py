import os
import logging
import csv
import json
import random
from pathlib import Path
from datetime import datetime, time, date
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.error import Forbidden, BadRequest

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# =========================
# Timezone & schedule
# =========================
TIMEZONE = ZoneInfo("Asia/Tbilisi")

BROADCAST_START = date(2025, 12, 16)
BROADCAST_END = date(2025, 12, 26)

# slot times (Tbilisi)
SLOT_SEND_TIMES = {
    "1": time(11, 15, tzinfo=TIMEZONE),
    "2": time(12, 15, tzinfo=TIMEZONE),
    "3": time(13, 15, tzinfo=TIMEZONE),
}

# TOP5 send time (Dec 26)
TOP5_SEND_TIME = time(20, 0, tzinfo=TIMEZONE)  # 20:00

# =========================
# Files
# =========================
TRACKS_FILE = "tracks.csv"
HISTORY_FILE = "user_history.json"
VOTES_FILE = "votes.json"
SUBSCRIBERS_FILE = "subscribers.json"
BROADCAST_LOG_FILE = "broadcast_log.json"

TRACKS_CACHE = None

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))


# =========================
# Helpers
# =========================
def get_local_now() -> datetime:
    return datetime.now(TIMEZONE)


def is_window_open(now: datetime) -> bool:
    # 08:00 <= time < 10:00
    return True


# =========================
# Tracks
# =========================
def load_tracks():
    """
    tracks.csv columns:
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

            tracks.append(
                {
                    "id": str(row.get("id", "")).strip(),
                    "date": (row.get("date") or "").strip(),
                    "slot": (row.get("slot") or "").strip(),
                    "title_artist": (row.get("title&artist") or "").strip(),
                    "video_link": (row.get("video_link") or "").strip(),
                    "audio": (row.get("audio") or "").strip(),
                    "message": (row.get("message") or "").strip(),
                }
            )

    TRACKS_CACHE = tracks
    logger.info("Loaded %d tracks from %s", len(tracks), TRACKS_FILE)
    return TRACKS_CACHE


def get_tracks_for_date_slot(day_iso: str, slot: str) -> list[dict]:
    tracks = load_tracks()
    out = []
    for t in tracks:
        if (t.get("date") == day_iso) and (str(t.get("slot")) == str(slot)):
            if (
                (t.get("title_artist") or "").strip()
                or (t.get("video_link") or "").strip()
                or (t.get("audio") or "").strip()
            ):
                out.append(t)

    out.sort(key=lambda x: int(x.get("id") or 0))
    return out


def format_track_text(track: dict) -> str:
    title_artist = (track.get("title_artist") or "").strip() or "(no title)"
    video_link = (track.get("video_link") or "").strip()
    message = (track.get("message") or "").strip()

    msg_block = f"{message}\n\n" if message else ""
    link_block = f"🔗 [Watch / Listen here]({video_link})\n\n" if video_link else ""

    return (
        "🎄 *Advent Music Calendar*\n\n"
        f"🎵 *Track:*\n_{title_artist}_\n\n"
        f"{msg_block}"
        f"{link_block}"
        "Если вам понравилось — нажмите ❤️"
    )


# =========================
# Broadcast log
# =========================
def load_broadcast_log():
    """
    {chat_id: {"last_date": "YYYY-MM-DD", "sent_slots": ["1","2","TOP5"]}}
    """
    path = Path(BROADCAST_LOG_FILE)
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load broadcast log: %s", e)
        return {}


def save_broadcast_log(data: dict):
    path = Path(BROADCAST_LOG_FILE)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save broadcast log: %s", e)


# =========================
# History (/today random)
# =========================
def load_history():
    """
    {chat_id: {last_date, track_id, used_track_ids: []}}
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


def choose_track_for_user(chat_id: int, today_date: str):
    tracks = load_tracks()
    if not tracks:
        return None

    history = load_history()
    key = str(chat_id)
    user_entry = history.get(key)

    if user_entry and user_entry.get("last_date") == today_date:
        track_id = user_entry.get("track_id")
        for t in tracks:
            if t["id"] == track_id:
                return t

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


# =========================
# Votes
# =========================
def load_votes():
    """
    {track_id: {"likes": int, "voters": [user_id,...]}}
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


# =========================
# Subscribers
# =========================
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


# =========================
# Keyboards
# =========================
def build_start_keyboard(subscribed: bool):
    if subscribed:
        keyboard = [[KeyboardButton("🎵 Open today’s track")]]
    else:
        keyboard = [[KeyboardButton("Подписаться")]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def build_vote_inline_keyboard(track_id: str):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❤️ I like this track", callback_data=f"VOTE:{track_id}")]]
    )


# =========================
# Send track
# =========================
async def send_track_to_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, track: dict):
    track_id = track.get("id", "")
    text = format_track_text(track)
    audio_file_id = (track.get("audio") or "").strip()

    try:
        if audio_file_id:
            await context.bot.send_audio(
                chat_id=chat_id,
                audio=audio_file_id,
                caption=text[:900],
                parse_mode="Markdown",
                reply_markup=build_vote_inline_keyboard(track_id),
                disable_notification=True,  # тихо
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=build_vote_inline_keyboard(track_id),
                disable_web_page_preview=False,
                disable_notification=True,  # тихо
            )
    except Forbidden:
        subs = load_subscribers()
        if chat_id in subs:
            subs.discard(chat_id)
            save_subscribers(subs)
        logger.warning("Forbidden: removed chat %s from subscribers", chat_id)
    except BadRequest as e:
        logger.error("BadRequest when sending to %s: %s", chat_id, e)
    except Exception as e:
        logger.error("Unexpected send error to %s: %s", chat_id, e)


# =========================
# Broadcast jobs
# =========================
async def broadcast_slot_job(context: ContextTypes.DEFAULT_TYPE):
    slot = str(context.job.data.get("slot"))
    now = get_local_now()
    today = now.date()

    logger.info("[BROADCAST] Job started | slot=%s | now=%s", slot, now.strftime("%Y-%m-%d %H:%M:%S"))

    if today < BROADCAST_START or today > BROADCAST_END:
        logger.info("[BROADCAST] Outside date range | today=%s", today.isoformat())
        return

    today_iso = today.isoformat()
    tracks = get_tracks_for_date_slot(today_iso, slot)

    if not tracks:
        logger.info("[BROADCAST] No tracks found | date=%s | slot=%s", today_iso, slot)
        return

    subs = load_subscribers()
    if not subs:
        logger.info("[BROADCAST] No subscribers | date=%s | slot=%s", today_iso, slot)
        return

    logger.info(
        "[BROADCAST] Preparing send | date=%s | slot=%s | tracks=%d | subscribers=%d",
        today_iso, slot, len(tracks), len(subs)
    )

    log = load_broadcast_log()
    sent_chats = 0
    skipped_chats = 0

    for chat_id in list(subs):
        key = str(chat_id)
        entry = log.get(key, {"last_date": "", "sent_slots": []})

        if entry.get("last_date") != today_iso:
            entry = {"last_date": today_iso, "sent_slots": []}

        if slot in entry.get("sent_slots", []):
            skipped_chats += 1
            continue

        for t in tracks:
            await send_track_to_chat(context, chat_id, t)

        entry["sent_slots"].append(slot)
        log[key] = entry
        sent_chats += 1

    save_broadcast_log(log)

    logger.info(
        "[BROADCAST] Done | date=%s | slot=%s | sent_to=%d | skipped=%d",
        today_iso, slot, sent_chats, skipped_chats
    )


def build_top5_text() -> str:
    tracks = load_tracks()
    track_by_id = {t["id"]: t for t in tracks}
    votes = load_votes()

    scored = []
    for track_id, info in votes.items():
        likes = int(info.get("likes", 0))
        if likes <= 0:
            continue
        track = track_by_id.get(str(track_id))
        if track:
            scored.append((likes, track))

    if not scored:
        return "🏆 *Top 5 Advent Tracks*\n\nПока нет голосов ❤️"

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:5]

    lines = ["🏆 *Top 5 Advent Tracks (by likes)*", ""]
    for i, (likes, t) in enumerate(top, start=1):
        title = t.get("title_artist", "(no title)")
        link = (t.get("video_link") or "").strip()
        lines.append(f"{i}. *{title}* — {likes} ❤️")
        if link:
            lines.append(f"   🔗 {link}")

    lines.append("\nСпасибо, что голосовали! 🎄")
    return "\n".join(lines)


async def broadcast_top5_to_all(context: ContextTypes.DEFAULT_TYPE, force: bool = False) -> tuple[int, int]:
    """
    Возвращает (sent, skipped).
    Если force=True — игнорируем защиту TOP5 в broadcast_log и шлём всем.
    """
    subs = load_subscribers()
    if not subs:
        logger.info("[TOP5] No subscribers")
        return (0, 0)

    text = build_top5_text()
    log = load_broadcast_log()

    today_iso = get_local_now().date().isoformat()

    sent = 0
    skipped = 0

    for chat_id in list(subs):
        key = str(chat_id)
        entry = log.get(key, {"last_date": "", "sent_slots": []})

        if entry.get("last_date") != today_iso:
            entry = {"last_date": today_iso, "sent_slots": []}

        if (not force) and ("TOP5" in entry.get("sent_slots", [])):
            skipped += 1
            continue

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
                disable_notification=True,
                disable_web_page_preview=True,
            )
            if not force:
                entry["sent_slots"].append("TOP5")
                log[key] = entry
            sent += 1
        except Forbidden:
            subs.discard(chat_id)
            save_subscribers(subs)
            logger.warning("[TOP5] Forbidden: removed chat %s", chat_id)
        except Exception as e:
            logger.error("[TOP5] Send error to %s: %s", chat_id, e)

    if not force:
        save_broadcast_log(log)

    return (sent, skipped)


async def broadcast_top5_daily_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Запускается ежедневно в TOP5_SEND_TIME, но реально отправляет только 26 декабря.
    Защита от повторов — broadcast_log.json, слот "TOP5".
    """
    now = get_local_now()
    today = now.date()

    if today != BROADCAST_END:
        logger.info("[TOP5] Not the day | today=%s", today.isoformat())
        return

    logger.info("[TOP5] Daily job triggered | now=%s", now.strftime("%Y-%m-%d %H:%M:%S"))
    sent, skipped = await broadcast_top5_to_all(context, force=False)
    logger.info("[TOP5] Done | sent=%d | skipped=%d", sent, skipped)


# =========================
# Commands / handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subs = load_subscribers()
    subscribed = chat_id in subs

    if subscribed:
        text = (
            "✅ Вы уже подписаны!\n\n"
            "Бот будет присылать вам треки по расписанию 🎶\n"
            "А ещё вы можете открыть трек вручную утром через кнопку ниже."
        )
    else:
        text = (
            "🎄 *Advent Music Calendar*\n\n"
            "Этот бот будет присылать вам *2–3 музыкальных трека каждый день* "
            "с **16 по 26 декабря**.\n\n"
            "В каждом сообщении вы получите:\n"
            "• название трека\n"
            "• ссылку на клип или аудио\n"
            "• короткое сообщение\n\n"
            "Чтобы получать ежедневные треки, нажмите кнопку ниже 👇"
        )

    await update.message.reply_markdown(
        text,
        reply_markup=build_start_keyboard(subscribed=subscribed),
    )


async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subs = load_subscribers()

    if chat_id in subs:
        await update.message.reply_text("✅ Вы уже подписаны.", reply_markup=build_start_keyboard(True))
        return

    subs.add(chat_id)
    save_subscribers(subs)
    logger.info("Subscribed chat_id=%s | total_subscribers=%d", chat_id, len(subs))

    await update.message.reply_text(
        "🎶 Подписка активирована!\n\n"
        "С 16 по 26 декабря вы будете получать 2–3 трека в день. ✨",
        reply_markup=build_start_keyboard(True),
        disable_notification=True,
    )


async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subs = load_subscribers()

    if chat_id not in subs:
        await update.message.reply_text("ℹ️ Вы не были подписаны.", reply_markup=build_start_keyboard(False))
        return

    subs.discard(chat_id)
    save_subscribers(subs)
    await update.message.reply_text(
        "🧹 Подписка отключена.",
        reply_markup=build_start_keyboard(False),
        disable_notification=True,
    )


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = get_local_now()
    local_time_str = now.strftime("%H:%M")
    today_date = now.date().isoformat()

    if not is_window_open(now):
        await update.message.reply_text(
            f"The Advent window is closed now. ⏰\n\n"
            f"You can open today’s track between 08:00 and 10:00.\n"
            f"Current time: {local_time_str}.",
            disable_notification=True,
        )
        return

    chat_id = update.effective_chat.id
    track = choose_track_for_user(chat_id, today_date)

    if track is None:
        await update.message.reply_text(
            "There are no tracks in the calendar yet. "
            "Please ask the organizer to add some to tracks.csv. 🎧",
            disable_notification=True,
        )
        return

    track_id = track.get("id", "")
    text = format_track_text(track)

    await update.message.reply_markdown(
        text,
        reply_markup=build_vote_inline_keyboard(track_id),
        disable_web_page_preview=False,
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "Подписаться":
        return await subscribe(update, context)

    if text == "🎵 Open today’s track":
        return await today(update, context)

    await update.message.reply_text(
        "Используйте кнопки ниже, чтобы работать с Advent Music Calendar 🎄",
        disable_notification=True,
    )


async def vote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""

    if not data.startswith("VOTE:"):
        await query.answer()
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


async def top5(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = build_top5_text()
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)


async def top5_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /top5_test — админ-команда, чтобы сейчас разослать TOP5 всем подписчикам.
    (без записи в broadcast_log, чтобы не "сжечь" реальную рассылку 26-го)
    """
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        await update.message.reply_text("You are not allowed to use /top5_test.")
        return

    await update.message.reply_text("🚀 Sending TOP5 to all subscribers (test)...", disable_notification=True)
    sent, skipped = await broadcast_top5_to_all(context, force=True)
    await update.message.reply_text(
        f"✅ TOP5 test finished.\nSent: {sent}\nSkipped (not used in force mode): {skipped}",
        disable_notification=True,
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
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

    lines = ["Advent Music – full stats:", ""]
    for t in tracks_sorted:
        tid = t.get("id", "")
        ta = t.get("title_artist", "(no title)")
        likes = likes_by_id.get(tid, 0)
        lines.append(f"{tid}. {ta}  ({likes} ❤️)")

    await update.message.reply_text("\n".join(lines))


async def subscribers_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        await update.message.reply_text("You are not allowed to use this command.")
        return

    subs = load_subscribers()
    await update.message.reply_text(f"👥 Подписчиков: {len(subs)}")


async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        await update.message.reply_text("You are not allowed to use this command.")
        return

    files = {
        "subscribers.json": SUBSCRIBERS_FILE,
        "votes.json": VOTES_FILE,
        "broadcast_log.json": BROADCAST_LOG_FILE,
    }

    sent_any = False

    for name, path in files.items():
        p = Path(path)
        if not p.exists():
            await update.message.reply_text(f"⚠️ {name} not found.")
            continue
        try:
            await update.message.reply_document(
                document=p,
                filename=name,
                caption=f"📦 Backup: {name}",
                disable_notification=True,
            )
            sent_any = True
        except Exception as e:
            logger.error("Backup failed for %s: %s", name, e)
            await update.message.reply_text(f"❌ Failed to send {name}")

    if sent_any:
        await update.message.reply_text("✅ Backup completed.", disable_notification=True)


async def restore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        await update.message.reply_text("You are not allowed to use /restore.")
        return

    context.user_data["awaiting_restore"] = True
    await update.message.reply_text(
        "♻️ Restore mode enabled.\n\n"
        "Please send ONE of the following JSON files:\n"
        "• subscribers.json\n"
        "• votes.json\n"
        "• broadcast_log.json\n\n"
        "I will restore it immediately.",
        disable_notification=True,
    )


async def handle_restore_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id != ADMIN_USER_ID:
        return

    if not context.user_data.get("awaiting_restore"):
        return

    doc = update.message.document
    if doc is None:
        return

    filename = doc.file_name
    allowed = {
        "subscribers.json": SUBSCRIBERS_FILE,
        "votes.json": VOTES_FILE,
        "broadcast_log.json": BROADCAST_LOG_FILE,
    }

    if filename not in allowed:
        await update.message.reply_text(
            "❌ Unsupported file.\n"
            "Allowed files:\n"
            "• subscribers.json\n"
            "• votes.json\n"
            "• broadcast_log.json",
            disable_notification=True,
        )
        return

    file = await doc.get_file()
    content = await file.download_as_bytearray()

    try:
        data = json.loads(content.decode("utf-8"))
    except Exception:
        await update.message.reply_text("❌ Invalid JSON file.", disable_notification=True)
        return

    path = Path(allowed[filename])
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Restore failed for %s: %s", filename, e)
        await update.message.reply_text("❌ Failed to write file.", disable_notification=True)
        return

    context.user_data["awaiting_restore"] = False
    await update.message.reply_text(
        f"✅ Restored `{filename}` successfully.",
        parse_mode="Markdown",
        disable_notification=True,
    )


# =========================
# Main
# =========================
def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Please set it as an environment variable.")

    application = ApplicationBuilder().token(token).build()

    # --- Scheduled broadcasts (16–26 Dec, slots 1–3) ---
    for slot, t in SLOT_SEND_TIMES.items():
        application.job_queue.run_daily(
            broadcast_slot_job,
            time=t,
            days=(0, 1, 2, 3, 4, 5, 6),
            data={"slot": slot},
            name=f"broadcast_slot_{slot}",
        )

    # --- TOP5 daily job at 20:00 with date check (robust to redeploys) ---
    application.job_queue.run_daily(
        broadcast_top5_daily_job,
        time=TOP5_SEND_TIME,
        days=(0, 1, 2, 3, 4, 5, 6),
        name="broadcast_top5_daily",
    )

    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))

    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("top5", top5))
    application.add_handler(CommandHandler("top5_test", top5_test))

    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(CommandHandler("subscribers", subscribers_count))
    application.add_handler(CommandHandler("backup", backup))
    application.add_handler(CommandHandler("restore", restore))

    # Restore file handler (admin uploads json)
    application.add_handler(MessageHandler(filters.Document.ALL, handle_restore_file))

    # Text buttons
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Votes
    application.add_handler(CallbackQueryHandler(vote_callback, pattern=r"^VOTE:"))

    application.run_polling()


if __name__ == "__main__":
    main()
