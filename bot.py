import asyncio
import os
import re
import tempfile
import time
from pathlib import Path
from shutil import which
from subprocess import PIPE, Popen, run as run_proc

from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import Application, MessageHandler, CommandHandler, ContextTypes, filters

# ================== НАСТРОЙКИ ==================
SEGMENT_SECONDS = 20              # длина кусочков
SEND_DELAY_SEC = 0.6              # пауза между отправками
SAFE_MAX_MB = 48                  # безопасный размер для Bot API ~50MB
TARGET_VIDEO_BITRATE = "3M"       # если кусок вышел > SAFE_MAX_MB — пережмём до этого битрейта
TARGET_AUDIO_BITRATE = "128k"
YTDLP_CONCURRENT = 10             # параллельные сегменты при скачивании
YTDLP_FORMAT = "bestvideo[height<=720]+bestaudio/best[height<=720]/best"  # быстрее/легче
# =================================================

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
FFMPEG = os.getenv("FFMPEG_PATH") or "ffmpeg"
FFPROBE = os.path.join(os.path.dirname(FFMPEG), "ffprobe.exe") if os.path.isabs(FFMPEG) else "ffprobe"
COOKIES_FILE = os.getenv("COOKIES_FILE")
YTDLP_BIN = os.getenv("YTDLP_PATH") or "yt-dlp"
COOKIES_FROM_BROWSER = os.getenv("COOKIES_FROM_BROWSER")
BROWSER_PROFILE = os.getenv("BROWSER_PROFILE")
YTDLP_PROXY = os.getenv("YTDLP_PROXY")

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

HELP_TEXT = (
    "Пришлите видео ИЛИ ссылку на видео — я порежу на куски по 20 секунд и пришлю по порядку.\\n"
    "• Ссылки качаются через yt-dlp (включены параллельные сегменты)\\n"
    "• Режу без перекодирования (максимально быстро). Если кусок > ~50MB — пережимаю именно этот кусок.\\n"
    "• Прогресс показываю в одном сообщении, обновляя проценты.\\n"
)

# --------------- вспомогательные штуки ----------------
def bin_available(path_or_name: str) -> bool:
    if path_or_name and os.path.isabs(path_or_name):
        return os.path.exists(path_or_name)
    return which(path_or_name) is not None

def ff_cmd(*args: str) -> list[str]:
    return [FFMPEG, *args]

def progress_should_update(last_time: float, last_pct: int, pct: float) -> bool:
    return (time.time() - last_time) >= 0.5 and int(pct) != last_pct

async def edit_progress(msg, phase: str, pct: float):
    pct = max(0.0, min(100.0, pct))
    try:
        await msg.edit_text(f"{phase}: {pct:.0f}%")
    except Exception:
        pass

def parse_ffprobe_duration_seconds(path: Path) -> float | None:
    try:
        p = run_proc(
            [FFPROBE, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", str(path)],
            stdout=PIPE, stderr=PIPE, text=True
        )
        if p.returncode == 0 and p.stdout.strip():
            return float(p.stdout.strip())
    except Exception:
        pass
    return None

async def run_ffmpeg_segment_copy_with_progress(src: Path, out_pattern: Path, total_seconds: float, progress_msg):
    """
    Режем без перекодирования, отслеживаем прогресс через -progress pipe:1 (out_time_ms).
    """
    cmd = ff_cmd(
        "-hide_banner", "-v", "error",
        "-i", str(src),
        "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", str(SEGMENT_SECONDS),
        "-reset_timestamps", "1",
        "-progress", "pipe:1",
        str(out_pattern)
    )
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, text=True, bufsize=1)

    last_pct = -1
    last_edit = 0.0
    try:
        if proc.stdout:
            for line in proc.stdout:
                line = line.strip()
                if line.startswith("out_time_ms="):
                    try:
                        ms = float(line.split("=", 1)[1])
                        t = ms / 1_000_000.0  # микросекунды -> секунды
                        pct = (t / max(total_seconds, 0.001)) * 100.0
                        if progress_should_update(last_edit, last_pct, pct):
                            await edit_progress(progress_msg, "✂️ Нарезка", pct)
                            last_edit = time.time()
                            last_pct = int(pct)
                    except Exception:
                        pass
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
    return proc.returncode == 0

async def transcode_part_if_too_big(part: Path) -> Path:
    size_mb = part.stat().st_size / (1024 * 1024)
    if size_mb <= SAFE_MAX_MB:
        return part
    target = part.with_name(part.stem + "_small.mp4")
    c = ff_cmd(
        "-y", "-hide_banner", "-v", "error", "-stats",
        "-i", str(part),
        "-c:v", "libx264", "-preset", "veryfast",
        "-b:v", TARGET_VIDEO_BITRATE,
        "-maxrate", TARGET_VIDEO_BITRATE, "-bufsize", "2M",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", TARGET_AUDIO_BITRATE,
        str(target)
    )
    p = await asyncio.to_thread(run_proc, c, stdout=PIPE, stderr=PIPE, text=True)
    return target if p.returncode == 0 and target.exists() else part

# --------------- yt-dlp (быстрее + прогресс) ---------------
async def ytdlp_download(url: str, out_dir: Path, progress_msg) -> Path | None:
    """
    Скачивание через yt-dlp (Python API) с прогрессом и параллельными сегментами.
    """
    try:
        import yt_dlp
    except Exception:
        await progress_msg.edit_text("yt-dlp не установлен. Выполни: pip install yt-dlp")
        return None

    downloaded_file: Path | None = None
    last_pct = -1
    last_edit = 0.0
    loop = asyncio.get_running_loop()

    def hook(d):
        nonlocal downloaded_file, last_pct, last_edit
        status = d.get("status")

        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            done = d.get("downloaded_bytes")
            if total and done:
                pct = done / max(total, 1) * 100.0
                if (time.time() - last_edit) >= 0.5 and int(pct) != last_pct:
                    coro = edit_progress(progress_msg, "⬇️ Скачивание", pct)
                    try:
                        asyncio.run_coroutine_threadsafe(coro, loop)
                    except Exception:
                        pass
                    last_pct = int(pct)
                    last_edit = time.time()
        elif status == "finished":
            try:
                asyncio.run_coroutine_threadsafe(
                    edit_progress(progress_msg, "⬇️ Скачивание", 100.0), loop
                )
            except Exception:
                pass

    ydl_opts = {
        "outtmpl": str(out_dir / "%(title).200s.%(ext)s"),
        "format": YTDLP_FORMAT,
        "merge_output_format": "mp4",
        "noplaylist": True,
        "concurrent_fragment_downloads": YTDLP_CONCURRENT,
        "progress_hooks": [hook],
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
        "rm_cachedir": True,
    }
    # cookies: приоритет файл > браузер
    if COOKIES_FILE:
        ydl_opts["cookiefile"] = COOKIES_FILE
    elif COOKIES_FROM_BROWSER:
        ydl_opts["cookiesfrombrowser"] = (COOKIES_FROM_BROWSER,) if not BROWSER_PROFILE else (COOKIES_FROM_BROWSER, None, BROWSER_PROFILE)
    # proxy
    if YTDLP_PROXY:
        ydl_opts["proxy"] = YTDLP_PROXY

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = await asyncio.to_thread(ydl.extract_info, url, download=True)
            if "requested_downloads" in info and info["requested_downloads"]:
                downloaded_file = Path(info["requested_downloads"][0]["_filename"])
            else:
                downloaded_file = Path(ydl.prepare_filename(info))
    except Exception as e:
        await progress_msg.edit_text(
            "❌ Не удалось скачать видео.\nПроверь вход/18+, cookies или прокси.\n\n"
            f"Ошибка: {str(e)[:1500]}"
        )
        return None

    return downloaded_file if downloaded_file and downloaded_file.exists() else None

# ------------------- Telegram команды ---------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)

async def handle_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    if not bin_available(FFMPEG):
        await message.reply_text("ffmpeg не найден. Проверь `.env` → FFMPEG_PATH.")
        return
    if not bin_available(FFPROBE):
        await message.reply_text("ffprobe не найден. Обычно ставится вместе с ffmpeg.")
        return

    text = (message.text or "").strip()
    url_match = URL_RE.search(text) if text else None

    progress_msg = await message.reply_text("⏳ Подготовка… 0%")

    # ======== ВАРИАНТ 1: ссылка ========
    if url_match:
        url = url_match.group(0)
        if not bin_available(YTDLP_BIN):
            await progress_msg.edit_text("yt-dlp не найден. Установите в .venv:  `pip install yt-dlp`.")
            return

        await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.UPLOAD_VIDEO)
        tempdir = Path(tempfile.mkdtemp(prefix="dl_"))
        src_path: Path | None = None
        success = False
        try:
            src_path = await ytdlp_download(url, tempdir, progress_msg)
            if not src_path:
                return

            total = parse_ffprobe_duration_seconds(src_path) or 0.0
            out_pattern = tempdir / "part_%03d.mp4"
            ok = await run_ffmpeg_segment_copy_with_progress(src_path, out_pattern, total, progress_msg)
            if not ok:
                await progress_msg.edit_text("Ошибка нарезки (ffmpeg). Попробуй другое видео либо пришли лог.")
                return

            parts = sorted(tempdir.glob("part_*.mp4"))
            if not parts:
                await progress_msg.edit_text("Сегменты не получились (возможно, повреждённый файл).")
                return

            await progress_msg.edit_text(f"✅ Нарезка готова. Отправляю {len(parts)} фрагмент(ов)…")

            for i, part in enumerate(parts, 1):
                safe_part = await transcode_part_if_too_big(part)
                await asyncio.sleep(SEND_DELAY_SEC)
                caption = f"Фрагмент {i}/{len(parts)}"
                try:
                    await message.chat.send_video(video=safe_part.open("rb"), caption=caption)
                except Exception:
                    await asyncio.sleep(SEND_DELAY_SEC)
                    await message.chat.send_document(document=safe_part.open("rb"), caption=caption)

            await progress_msg.edit_text("✅ Готово: все фрагменты отправлены.")
            success = True
        finally:
            if success:
                try:
                    for p in tempdir.glob("*"):
                        try: p.unlink()
                        except Exception: pass
                    tempdir.rmdir()
                except Exception:
                    pass
        return

    # ======== ВАРИАНТ 2: прислали файл ========
    tg_file = None
    file_name = None
    if message.video:
        tg_file = await message.video.get_file()
        file_name = message.video.file_name or "video.mp4"
    elif message.document and (message.document.mime_type or "").startswith("video/"):
        tg_file = await message.document.get_file()
        file_name = message.document.file_name or "video.mp4"
    else:
        await progress_msg.edit_text("Пришлите видео или ссылку на видео.")
        return

    await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.UPLOAD_VIDEO)

    tempdir = Path(tempfile.mkdtemp(prefix="vid_"))
    src_path = tempdir / file_name
    success = False
    try:
        await tg_file.download_to_drive(custom_path=str(src_path))

        total = parse_ffprobe_duration_seconds(src_path) or 0.0
        out_pattern = tempdir / "part_%03d.mp4"
        ok = await run_ffmpeg_segment_copy_with_progress(src_path, out_pattern, total, progress_msg)
        if not ok:
            await progress_msg.edit_text("Ошибка нарезки (ffmpeg).")
            return

        parts = sorted(tempdir.glob("part_*.mp4"))
        if not parts:
            await progress_msg.edit_text("Сегменты не получились.")
            return

        await progress_msg.edit_text(f"✅ Нарезка готова. Отправляю {len(parts)} фрагмент(ов)…")

        for i, part in enumerate(parts, 1):
            safe_part = await transcode_part_if_too_big(part)
            await asyncio.sleep(SEND_DELAY_SEC)
            caption = f"Фрагмент {i}/{len(parts)}"
            try:
                await message.chat.send_video(video=safe_part.open("rb"), caption=caption)
            except Exception:
                await asyncio.sleep(SEND_DELAY_SEC)
                await message.chat.send_document(document=safe_part.open("rb"), caption=caption)

        await progress_msg.edit_text("✅ Готово: все фрагменты отправлены.")
        success = True
    finally:
        if success:
            try:
                for p in tempdir.glob("*"):
                    try: p.unlink()
                    except Exception: pass
                tempdir.rmdir()
            except Exception:
                pass

def build_app() -> Application:
    if not TOKEN:
        raise RuntimeError("Запишите токен в переменную окружения TELEGRAM_BOT_TOKEN (см. .env).")
    app = Application.builder().token(TOKEN).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.TEXT | filters.VIDEO | filters.Document.VIDEO, handle_any))
    return app

if __name__ == "__main__":
    app = build_app()
    print("Bot is running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
