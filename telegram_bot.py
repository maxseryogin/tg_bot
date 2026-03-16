import requests
import base64
import asyncio
import json
import logging
import os
import random
import re
import tempfile
import time
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def _config(key: str, default: str = "") -> str:
    if key in os.environ and os.environ[key].strip():
        return os.environ[key].strip()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    search_dirs = [base_dir, os.path.dirname(base_dir), os.getcwd()]
    for search_dir in search_dirs:
        for env_name in (".env", ".env.local"):
            env_path = os.path.join(search_dir, env_name)
            if os.path.isfile(env_path):
                try:
                    with open(env_path, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith("#"):
                                continue
                            if "=" in line:
                                k, _, v = line.partition("=")
                                if k.strip() == key:
                                    v = v.strip().strip('"').strip("'")
                                    if v:
                                        return v
                except Exception:
                    pass

    try:
        import configparser
        path = os.path.join(base_dir, "bot_config.ini")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or line.startswith("["):
                        continue
                    if "=" in line:
                        k, _, v = line.partition("=")
                        if k.strip() == key:
                            v = v.strip().strip('"').strip("'")
                            if v:
                                return v
            cfg = configparser.ConfigParser()
            cfg.read(path, encoding="utf-8")
            for section in ("bot", "DEFAULT"):
                if cfg.has_section(section):
                    val = cfg.get(section, key, fallback=None)
                    if val is not None and str(val).strip():
                        return str(val).strip().strip('"').strip("'")
    except Exception:
        pass
    return default

BOT_TOKEN           = _config("TELEGRAM_BOT_TOKEN")
_allowed            = _config("ALLOWED_USER_ID", "0")
ALLOWED_USER_ID     = int(_allowed) if _allowed.isdigit() else 0
HUGGINGFACE_TOKEN   = _config("HUGGINGFACE_TOKEN", "")
PLAYER_URL          = _config("PLAYER_URL", "http://localhost:9988")
TOGETHER_API_TOKEN  = _config("TOGETHER_API_TOKEN", "")
GEMINI_API_KEY      = _config("GEMINI_API_KEY", "")
REPLICATE_API_TOKEN = _config("REPLICATE_API_TOKEN", "")
MUKESH_API_KEY      = _config("MUKESH_API_KEY", "")
TOGETHER_URL        = "https://api.together.xyz/v1/images/generations"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("telegram_bot")

TRIGGER_WORD = "сгк"


# ── Вспомогательные функции ───────────────────────────────────────────────────

def _get_backend():
    return None


def _fetch_player_info() -> dict:
    url = f"{PLAYER_URL}/now"
    headers = {}
    if "ngrok" in PLAYER_URL.lower():
        headers["ngrok-skip-browser-warning"] = "1"
    try:
        resp = requests.get(url, timeout=5, headers=headers)
        if resp.status_code == 200:
            return resp.json()
        logger.warning("HTTP-мост: статус %d от %s", resp.status_code, url)
    except requests.exceptions.ConnectionError:
        logger.warning("HTTP-мост: нет соединения с %s", url)
    except requests.exceptions.Timeout:
        logger.warning("HTTP-мост: таймаут запроса к %s", url)
    except Exception as e:
        logger.warning("HTTP-мост: ошибка (%s): %s", url, e)
    return {}


def _get_current_track_info(backend=None) -> dict:
    empty = {
        "artist": "", "title": "", "cover_data": "",
        "lyrics": "", "filepath": "", "has_track": False, "playing": False,
    }

    if backend is not None:
        try:
            # Стриминг (YouTube / поиск)
            streaming_title = getattr(backend, "_streaming_title", "")
            if streaming_title:
                lyrics = getattr(backend, "_current_lyrics_text", "") or ""
                if lyrics.startswith("⏳"):
                    lyrics = ""
                return {
                    "artist":     getattr(backend, "_streaming_artist", ""),
                    "title":      streaming_title,
                    "cover_data": getattr(backend, "_streaming_cover", ""),
                    "lyrics":     lyrics,
                    "filepath":   "",
                    "has_track":  True,
                    "playing":    bool(getattr(backend, "isPlaying", False)),
                }
            # Локальная библиотека
            idx = getattr(backend, "current_index", -1)
            if idx is None or idx < 0:
                return empty
            visible = getattr(getattr(backend, "track_model", None), "_visible_tracks", [])
            if idx >= len(visible):
                return empty
            track = visible[idx]
            lyrics = getattr(backend, "_current_lyrics_text", "") or ""
            if lyrics.startswith("⏳"):
                lyrics = ""
            return {
                "artist":     getattr(track, "artist", ""),
                "title":      getattr(track, "title", ""),
                "cover_data": getattr(track, "cover_data", ""),
                "lyrics":     lyrics,
                "filepath":   getattr(track, "filepath", ""),
                "has_track":  True,
                "playing":    bool(getattr(backend, "isPlaying", False)),
            }
        except Exception as e:
            logger.exception("Ошибка чтения local backend: %s", e)
            return empty

    data = _fetch_player_info()
    if not data or not data.get("title"):
        return empty
    return {
        "artist":     data.get("artist", ""),
        "title":      data.get("title", ""),
        "cover_data": data.get("cover", ""),
        "has_cover":  bool(data.get("has_cover", False)),
        "lyrics":     data.get("lyrics", ""),
        "filepath":   data.get("filepath", ""),
        "has_track":  bool(data.get("title")),
        "playing":    bool(data.get("playing", False)),
    }


def _html_quote(text: str) -> str:
    if not text:
        return ""
    escaped = (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )
    return f"<blockquote>{escaped}</blockquote>"


def clean_post_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("**", "")
    text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(lines).strip()
    return text


def _decode_cover_to_bytes(cover_data: str):
    if not cover_data or not cover_data.startswith("data:"):
        return None, None
    try:
        head, _, b64 = cover_data.partition(",")
        mime = "image/jpeg"
        if ";" in head:
            mime = head.split(";")[0].replace("data:", "").strip()
        raw = base64.b64decode(b64)
        return raw, mime
    except Exception as e:
        logger.warning("Не удалось декодировать обложку: %s", e)
        return None, None


# ── Команда «сгк» (только для владельца по user_id) ──────────────────────────

async def cmd_trigger(message, bot, backend):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

    info = _get_current_track_info(backend)
    if not info["has_track"] or (not info["title"] and not info["artist"]):
        raw = _fetch_player_info()
        if not raw:
            player_host = PLAYER_URL.replace("http://", "").replace("https://", "").split("/")[0]
            if "localhost" in player_host or "127.0.0.1" in player_host:
                await message.answer(
                    "🔌 Плеер недоступен.\n"
                    "<i>PLAYER_URL указывает на localhost — бот на Railway не может подключиться к локальному компу.\n"
                    "Используй ngrok: <code>ngrok http 9988</code> и задай PLAYER_URL в Railway.</i>",
                    parse_mode="HTML",
                )
            else:
                await message.answer(
                    f"🔌 Плеер недоступен — запусти mp3 player на своём компе.\n"
                    f"<code>PLAYER_URL={PLAYER_URL}</code>",
                    parse_mode="HTML",
                )
        else:
            await message.answer("Сейчас ничего не играет.")
        return

    status_emoji = "▶️" if info.get("playing") else "⏸"
    status_label = "слушает" if info.get("playing") else "поставила на паузу"
    text = f"{status_emoji} Жужа {status_label}\n\n🎤 {info['artist']}\n🎵 {info['title']}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📝 Получить текст", callback_data="get_lyrics"),
            InlineKeyboardButton(text="⬇️ Скачать песню",  callback_data="download_track"),
        ]
    ])

    photo = None
    if info.get("has_cover") or info.get("cover_data"):
        try:
            cover_url  = f"{PLAYER_URL}/cover"
            cover_resp = requests.get(cover_url, timeout=8)
            if cover_resp.status_code == 200:
                mime = cover_resp.headers.get("Content-Type", "image/jpeg")
                ext  = "png" if "png" in mime else "jpg"
                photo = BufferedInputFile(cover_resp.content, filename=f"cover.{ext}")
            else:
                logger.warning("GET /cover вернул статус %d", cover_resp.status_code)
        except Exception as e:
            logger.warning("Не удалось получить обложку с %s/cover: %s", PLAYER_URL, e)

        if photo is None and info.get("cover_data"):
            cover_bytes, mime = _decode_cover_to_bytes(info["cover_data"])
            if cover_bytes:
                ext   = "jpg" if "jpeg" in mime or "jpg" in mime else "png"
                photo = BufferedInputFile(cover_bytes, filename=f"cover.{ext}")

    if photo:
        try:
            await message.answer_photo(photo=photo, caption=text, reply_markup=keyboard)
            return
        except Exception as e:
            logger.warning("answer_photo упала (%s), отправляю без фото", e)

    try:
        await message.answer(text, reply_markup=keyboard)
    except Exception as e:
        logger.exception("Ошибка отправки: %s", e)
        await message.answer("Ошибка при формировании ответа.")


async def handle_callback(callback, bot, backend):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

    data       = callback.data
    message    = callback.message
    chat_id    = message.chat.id
    message_id = message.message_id

    def remove_button(remove_data: str):
        try:
            kb = message.reply_markup
            if not kb or not kb.inline_keyboard:
                return None
            new_rows = []
            for row in kb.inline_keyboard:
                new_buttons = [b for b in row if getattr(b, "callback_data", None) != remove_data]
                if new_buttons:
                    new_rows.append(new_buttons)
            if new_rows:
                return InlineKeyboardMarkup(inline_keyboard=new_rows)
        except Exception:
            pass
        return None

    if data == "get_lyrics":
        new_kb = remove_button("get_lyrics")
        try:
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=new_kb)
        except Exception as e:
            logger.warning("Не удалось обновить клавиатуру: %s", e)

        info    = _get_current_track_info(backend)
        lyrics  = (info.get("lyrics") or "").strip()
        LOADING = ("Загрузка...", "⏳", "Текст песни не найден", "Ошибка")
        has_lyrics = bool(lyrics) and not any(lyrics.startswith(m) for m in LOADING)

        if not has_lyrics:
            await callback.answer("Текст ещё не загружен", show_alert=False)
            await bot.send_message(chat_id=chat_id,
                                   text="😔 Текст пока не загружен, попробуй через пару секунд.",
                                   reply_to_message_id=message_id)
        else:
            await callback.answer("Текст отправлен")
            await bot.send_message(chat_id=chat_id, text=_html_quote(lyrics),
                                   reply_to_message_id=message_id, parse_mode="HTML")
        return

    if data == "download_track":
        info = _get_current_track_info(backend)
        new_kb = remove_button("download_track")
        try:
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=new_kb)
        except Exception as e:
            logger.warning("Не удалось обновить клавиатуру: %s", e)
        await callback.answer()

        try:
            file_url = f"{PLAYER_URL}/file"
            headers = {}
            if "ngrok" in PLAYER_URL.lower():
                headers["ngrok-skip-browser-warning"] = "1"
            resp = requests.get(file_url, timeout=30, headers=headers)
            if resp.status_code != 200:
                await bot.send_message(chat_id=chat_id, text="Файл трека недоступен.",
                                    reply_to_message_id=message_id)
                return
            from aiogram.types import BufferedInputFile
            filename = info.get("title", "track") + ".mp3"
            audio_file = BufferedInputFile(resp.content, filename=filename)
            await bot.send_audio(chat_id=chat_id, audio=audio_file,
                                title=info.get("title"), performer=info.get("artist"))
        except Exception as e:
            logger.exception("Ошибка отправки файла: %s", e)
            await bot.send_message(chat_id=chat_id, text="Не удалось отправить файл.",
                                reply_to_message_id=message_id)
        return

    await callback.answer()


# ── Кулдауны ──────────────────────────────────────────────────────────────────
_quote_cooldowns  = {}
_rep_cooldowns    = {}
_image_cooldowns  = {}
_ad_cooldowns     = {}
_search_cooldowns = {}
_music_cooldowns  = {}
_info_cooldowns   = {}
_help_cooldowns   = {}
_meme_cooldowns   = {}
_pikk_cooldowns   = {}
_draw_cooldowns: dict[int, float] = {}

# ── Жужа-болталка ─────────────────────────────────────────────────────────────
_chat_mode_enabled: dict[int, bool] = {}
_chat_history:      dict[int, list] = {}
_CHAT_HISTORY_MAX = 30
CHAT_ON_TRIGGER   = "жужа го говорить"
CHAT_OFF_TRIGGER  = "жужа хватит говорить"
CHAT_TEST_TRIGGER = "жужа ты тут"
CHAT_REPLY_CHANCE = 0.35
_CHAT_STATE_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "juza_chat_state.json")


def _chat_state_save():
    try:
        enabled = [cid for cid, v in _chat_mode_enabled.items() if v]
        with open(_CHAT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(enabled, f)
    except Exception as e:
        logger.warning("Не удалось сохранить состояние болталки: %s", e)


def _chat_state_load():
    try:
        if os.path.isfile(_CHAT_STATE_FILE):
            with open(_CHAT_STATE_FILE, "r", encoding="utf-8") as f:
                enabled = json.load(f)
            for cid in enabled:
                _chat_mode_enabled[int(cid)] = True
            logger.info("Болталка: загружено %d активных чатов", len(enabled))
    except Exception as e:
        logger.warning("Не удалось загрузить состояние болталки: %s", e)


# ── Константы триггеров ───────────────────────────────────────────────────────
QUOTE_TRIGGER      = "жужа цитату"
QUOTE_API_URL      = "https://randomall.ru/api/gens/6381"
QUOTE_COOLDOWN_SEC = 60

REP_TRIGGER        = "жужа го реповать"
REP_COOLDOWN_SEC   = 60
REP_CHANNEL        = "citatarap"
_REP_CACHE_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rep_cache")
_REP_CACHE_JSON    = os.path.join(_REP_CACHE_DIR, "cache.json")

AD_TRIGGER         = "жужа рекламу"
AD_COOLDOWN_SEC    = 60
AD_CHANNEL         = "reklamarolok"
_AD_CACHE_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ad_cache")
_AD_CACHE_JSON     = os.path.join(_AD_CACHE_DIR, "cache.json")

SEARCH_TRIGGER      = "жужа найди"
SEARCH_COOLDOWN_SEC = 30

MUSIC_TRIGGER       = "жужа музло"
MUSIC_COOLDOWN_SEC  = 30

INFO_TRIGGER        = "жужа найди информацию"
INFO_COOLDOWN_SEC   = 30

HELP_TRIGGER        = "жужа шо можешь"
HELP_COOLDOWN_SEC   = 10

HELP_TEXT = (
    "<pre>"
    "жужа шо можешь\n"
    "жужа нарисуй [промпт]\n"
    "жужа кружок [ссылка на видео]\n"
    "жужа музло [запрос]\n"
    "жужа найди информацию [запрос]\n"
    "жужа найди [запрос]\n"
    "жужа цитату\n"
    "жужа го реповать\n"
    "жужа рекламу\n"
    "жужа мем мелстрой\n"
    "жужа мем пикшанель\n"
    "жужа го говорить\n"
    "жужа хватит говорить\n"
    "жужа ты тут\n"
    "сгк"
    "</pre>"
)

MEME_TRIGGER      = "жужа мем мелстрой"
MEME_COOLDOWN_SEC = 60

MEME_PIKK_TRIGGER      = "жужа мем пикшанель"
MEME_PIKK_COOLDOWN_SEC = 60

_INFO_GIF_POOL = [
    "https://media.giphy.com/media/3o7TKMt1VVNkHV2PaE/giphy.gif",
    "https://media.giphy.com/media/l3q2K5jinAlChoCLS/giphy.gif",
    "https://media.giphy.com/media/26ufdipQqU2lhNA4g/giphy.gif",
    "https://media.giphy.com/media/l46CyJmS9KUbokzsI/giphy.gif",
    "https://media.giphy.com/media/3oEjI6SIIHBdRxXI40/giphy.gif",
    "https://media.giphy.com/media/l0HlvtIPzPdt2usKs/giphy.gif",
    "https://media.giphy.com/media/xT9IgG50Lg7russbDB/giphy.gif",
    "https://media.giphy.com/media/26BRuo6sLetdllPAQ/giphy.gif",
    "https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif",
    "https://media.giphy.com/media/3o7TKF5DnsSLv4zVBu/giphy.gif",
    "https://media.giphy.com/media/l4FGuhL4U2WyjdkaY/giphy.gif",
    "https://media.giphy.com/media/xT9IgDEI1iZs0tIBnW/giphy.gif",
    "https://media.giphy.com/media/3ohzdIuqJoo8QdKlnW/giphy.gif",
    "https://media.giphy.com/media/l0MYEqEzwMWFCg8rm/giphy.gif",
    "https://media.giphy.com/media/26uflVGKNTNgBdCra/giphy.gif",
]

HF_MODELS = [
    "stabilityai/stable-diffusion-3.5-medium",
    "black-forest-labs/FLUX.1-schnell",
    "stabilityai/stable-diffusion-2-1",
    "runwayml/stable-diffusion-v1-5",
    "stabilityai/stable-diffusion-xl-base-1.0",
]
HF_API_URL = "https://router.huggingface.co/hf-inference/models/{}"

DRAW_TRIGGER      = "жужа нарисуй"
DRAW_COOLDOWN_SEC = 90
_draw_cooldowns: dict[int, float] = {}

CIRCLE_TRIGGER       = "жужа кружок"
CIRCLE_COOLDOWN_SEC  = 30
_circle_cooldowns: dict[int, float] = {}
CIRCLE_MAX_DURATION  = 60  # секунд — ограничение Telegram для video note

_draw_queues:     dict[int, asyncio.Queue]  = {}
_draw_queue_busy: dict[int, bool]           = {}
_draw_last_done:  dict[int, float]          = {}


# ── Cobalt ────────────────────────────────────────────────────────────────────

_COBALT_ALL_INSTANCES = [
    "https://api.cobalt.tools",
    "https://cobalt.imput.net",
    "https://cbl.henhen1227.com",
    "https://cobalt.tools",
]

_cobalt_instance_cache: dict[str, tuple[bool, float]] = {}
_COBALT_DEAD_TTL = 600
_COBALT_CHECK_TIMEOUT = 5


async def _cobalt_get_live_instances() -> list[str]:
    import aiohttp
    now = time.time()
    live = []
    to_check = []

    for inst in _COBALT_ALL_INSTANCES:
        cached = _cobalt_instance_cache.get(inst)
        if cached:
            is_alive, checked_at = cached
            age = now - checked_at
            if is_alive:
                live.append(inst)
                continue
            elif age < _COBALT_DEAD_TTL:
                logger.debug("cobalt: пропускаем мёртвый инстанс %s (кэш %ds)", inst, int(age))
                continue
        to_check.append(inst)

    if to_check:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }

        async def _check(inst: str):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        inst,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=_COBALT_CHECK_TIMEOUT),
                        allow_redirects=False,
                    ) as resp:
                        alive = resp.status not in (502, 503, 504, 0)
                        _cobalt_instance_cache[inst] = (alive, time.time())
                        if alive:
                            logger.info("cobalt: ✓ %s (HTTP %d)", inst, resp.status)
                        else:
                            logger.info("cobalt: ✗ %s (HTTP %d)", inst, resp.status)
                        return alive
            except Exception as e:
                err = str(e)[:60]
                _cobalt_instance_cache[inst] = (False, time.time())
                logger.info("cobalt: ✗ %s (%s)", inst, err)
                return False

        results = await asyncio.gather(*[_check(inst) for inst in to_check])
        for inst, alive in zip(to_check, results):
            if alive:
                live.append(inst)

    seen = set()
    unique_live = []
    for inst in live:
        if inst not in seen:
            seen.add(inst)
            unique_live.append(inst)

    return unique_live


# ── Генерация изображений ─────────────────────────────────────────────────────

_sd_pipeline = None
_sd_pipe_lock = None

def _sd_lock():
    global _sd_pipe_lock
    if _sd_pipe_lock is None:
        _sd_pipe_lock = asyncio.Lock()
    return _sd_pipe_lock


def _try_load_sd_pipeline():
    global _sd_pipeline
    if _sd_pipeline is not None:
        return _sd_pipeline
    try:
        import torch
        from diffusers import StableDiffusionPipeline, DPMSolverMultistepScheduler
        import gc

        device = "cuda" if torch.cuda.is_available() else "cpu"
        dtype  = torch.float16 if device == "cuda" else torch.float32
        logger.info("SD: загружаем модель на %s (%s)", device, dtype)

        model_ids = [
            "stablediffusionapi/realistic-vision-v51",
            "runwayml/stable-diffusion-v1-5",
            "CompVis/stable-diffusion-v1-4",
        ]
        for model_id in model_ids:
            try:
                logger.info("SD: пробуем %s...", model_id)
                pipe = StableDiffusionPipeline.from_pretrained(
                    model_id,
                    torch_dtype=dtype,
                    safety_checker=None,
                    requires_safety_checker=False,
                    low_cpu_mem_usage=True,
                )
                pipe.scheduler = DPMSolverMultistepScheduler.from_config(pipe.scheduler.config)
                if device == "cpu":
                    pipe.enable_attention_slicing()
                pipe = pipe.to(device)
                _sd_pipeline = pipe
                logger.info("SD: модель %s загружена ✓", model_id)
                return pipe
            except Exception as e:
                logger.warning("SD: %s не удалось загрузить: %s", model_id, e)
                gc.collect()
        logger.warning("SD: ни одна модель не загрузилась")
        return None
    except ImportError:
        logger.info("SD: diffusers/torch не установлены")
        return None
    except Exception as e:
        logger.warning("SD: ошибка загрузки пайплайна: %s", e)
        return None


async def generate_image_local(prompt: str) -> tuple:
    import io
    async with _sd_lock():
        loop = asyncio.get_event_loop()
        try:
            pipe = await loop.run_in_executor(None, _try_load_sd_pipeline)
            if pipe is None:
                return None, "SD не установлен"

            def _gen():
                result = pipe(prompt, num_inference_steps=25, guidance_scale=7.5,
                              width=512, height=512).images[0]
                buf = io.BytesIO()
                result.save(buf, format="PNG")
                return buf.getvalue()

            logger.info("SD: генерируем локально...")
            image_bytes = await loop.run_in_executor(None, _gen)
            logger.info("SD: готово (%dKB) ✓", len(image_bytes) // 1024)
            return image_bytes, None
        except Exception as e:
            logger.warning("SD: ошибка генерации: %s", e)
            return None, str(e)[:100]


async def generate_image_g4f(prompt: str) -> tuple:
    import aiohttp
    try:
        from g4f.client import Client as G4FClient
        import g4f.Provider as Providers
    except ImportError:
        return None, "g4f не установлен (pip install -U g4f)"

    loop = asyncio.get_event_loop()

    for bb_model in ("flux", "sdxl", "dall-e-3"):
        try:
            def _gen_bb(m=bb_model):
                client = G4FClient()
                resp = client.images.generate(
                    model=m,
                    prompt=prompt,
                    provider=Providers.Blackbox,
                    response_format="url",
                )
                return resp.data[0].url if resp.data else None

            logger.info("g4f: Blackbox/%s...", bb_model)
            img_url = await asyncio.wait_for(
                loop.run_in_executor(None, _gen_bb), timeout=90
            )
            if img_url:
                img_bytes = await _download_image_url(img_url)
                if img_bytes:
                    logger.info("g4f: Blackbox/%s ✓ %dKB", bb_model, len(img_bytes)//1024)
                    return img_bytes, None
        except asyncio.TimeoutError:
            logger.warning("g4f: Blackbox/%s таймаут", bb_model)
        except Exception as e:
            logger.warning("g4f: Blackbox/%s: %s", bb_model, str(e)[:100])
        await asyncio.sleep(random.uniform(2, 4))

    try:
        def _gen_di():
            client = G4FClient()
            resp = client.images.generate(
                model="flux",
                prompt=prompt,
                provider=Providers.DeepInfraImage,
                response_format="url",
            )
            return resp.data[0].url if resp.data else None

        logger.info("g4f: DeepInfraImage/flux...")
        img_url = await asyncio.wait_for(loop.run_in_executor(None, _gen_di), timeout=90)
        if img_url:
            img_bytes = await _download_image_url(img_url)
            if img_bytes:
                logger.info("g4f: DeepInfraImage ✓ %dKB", len(img_bytes)//1024)
                return img_bytes, None
    except asyncio.TimeoutError:
        logger.warning("g4f: DeepInfraImage таймаут")
    except Exception as e:
        logger.warning("g4f: DeepInfraImage: %s", str(e)[:100])
    await asyncio.sleep(random.uniform(2, 4))

    today = time.strftime("%Y-%m-%d")
    hf_key = f"hf_quota_{today}"
    hf_used = getattr(generate_image_g4f, "_hf_counter", {})
    generate_image_g4f._hf_counter = hf_used
    hf_count_today = hf_used.get(hf_key, 0)

    if hf_count_today >= 4:
        logger.warning("g4f: HF квота исчерпана на сегодня (%d/4 использовано)", hf_count_today)
        return None, f"Квота исчерпана на сегодня ({hf_count_today}/4 генераций). Попробуй завтра или другой промпт."

    for model in ("flux", "sdxl"):
        try:
            def _gen_hf(m=model):
                client = G4FClient()
                resp = client.images.generate(model=m, prompt=prompt, response_format="url")
                return resp.data[0].url if resp.data else None

            logger.info("g4f: HF/%s (использование %d/4 сегодня)...", model, hf_count_today+1)
            img_url = await asyncio.wait_for(loop.run_in_executor(None, _gen_hf), timeout=90)
            if img_url:
                img_bytes = await _download_image_url(img_url)
                if img_bytes:
                    hf_used[hf_key] = hf_count_today + 1
                    logger.info("g4f: HF/%s ✓ %dKB (использовано %d/4)", model, len(img_bytes)//1024, hf_used[hf_key])
                    return img_bytes, None
        except asyncio.TimeoutError:
            logger.warning("g4f: HF/%s таймаут", model)
        except Exception as e:
            err = str(e)[:120]
            logger.warning("g4f: HF/%s: %s", model, err)
            if "quota" in err.lower() or "exceeded" in err.lower():
                hf_used[hf_key] = 99
                logger.warning("g4f: HF квота исчерпана, блокируем до завтра")
                return None, "HF квота исчерпана. Попробуй завтра."
        await asyncio.sleep(random.uniform(3, 6))

    return None, "g4f: все провайдеры недоступны"


async def _download_image_url(url: str) -> bytes | None:
    import aiohttp
    if not url:
        return None
    if url.startswith("data:image"):
        try:
            _, b64 = url.split(",", 1)
            data = base64.b64decode(b64)
            return data if len(data) > 1000 else None
        except Exception:
            return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=60),
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return data if len(data) > 1000 else None
    except Exception as e:
        logger.warning("_download_image_url: %s", str(e)[:80])
    return None


# ── Видео-кружок ─────────────────────────────────────────────────────────────

async def download_video_for_circle(url: str) -> tuple[bytes | None, str]:
    """Скачивает видео по URL и конвертирует его в квадратный формат для video note.
    
    Возвращает (bytes, error_or_title).
    Видео обрезается до CIRCLE_MAX_DURATION секунд и приводится к квадрату 360×360.
    """
    import aiohttp
    import subprocess as _sp
    import shutil

    ffmpeg_path = shutil.which("ffmpeg")
    yt_dlp_path = shutil.which("yt-dlp")

    with tempfile.TemporaryDirectory() as tmpdir:
        raw_path  = os.path.join(tmpdir, "raw_video")
        out_path  = os.path.join(tmpdir, "circle.mp4")

        # ── 1. Попытка скачать через yt-dlp (YouTube, VK, TikTok и т.д.) ──
        if yt_dlp_path:
            try:
                result = _sp.run(
                    [
                        yt_dlp_path,
                        "--no-playlist",
                        "--merge-output-format", "mp4",
                        "-f", "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                        "--max-filesize", "50M",
                        "-o", raw_path + ".%(ext)s",
                        url,
                    ],
                    capture_output=True, text=True, timeout=120,
                )
                # yt-dlp добавляет расширение — ищем файл
                downloaded = None
                for ext in ("mp4", "mkv", "webm", "avi", "mov"):
                    candidate = raw_path + f".{ext}"
                    if os.path.isfile(candidate):
                        downloaded = candidate
                        break
                if downloaded is None:
                    # иногда файл без расширения
                    for fname in os.listdir(tmpdir):
                        if fname.startswith("raw_video"):
                            downloaded = os.path.join(tmpdir, fname)
                            break
                if downloaded:
                    raw_path = downloaded
                    logger.info("circle: yt-dlp скачал %s (%dKB)", downloaded,
                                os.path.getsize(downloaded) // 1024)
                else:
                    logger.warning("circle: yt-dlp не нашёл файл; stderr=%s",
                                   result.stderr[:200])
                    raw_path = None
            except _sp.TimeoutExpired:
                logger.warning("circle: yt-dlp таймаут")
                raw_path = None
            except Exception as e:
                logger.warning("circle: yt-dlp ошибка: %s", e)
                raw_path = None
        else:
            raw_path = None

        # ── 2. Если yt-dlp не помог — прямой HTTP-запрос ──────────────────
        if raw_path is None:
            direct_path = os.path.join(tmpdir, "direct.mp4")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=60),
                        headers={"User-Agent": "Mozilla/5.0"},
                    ) as resp:
                        if resp.status != 200:
                            return None, f"HTTP {resp.status} при скачивании видео"
                        content = await resp.read()
                if len(content) < 1000:
                    return None, "Файл слишком маленький — возможно, это не видео"
                with open(direct_path, "wb") as f:
                    f.write(content)
                raw_path = direct_path
                logger.info("circle: прямое скачивание %dKB", len(content) // 1024)
            except Exception as e:
                return None, f"Не удалось скачать видео: {str(e)[:100]}"

        if not raw_path or not os.path.isfile(raw_path):
            return None, "Не удалось скачать видео"

        # ── 3. Конвертация в квадрат 360×360 через ffmpeg ─────────────────
        if not ffmpeg_path:
            # Нет ffmpeg — отправляем как есть (может не пройти ограничение кружка)
            logger.warning("circle: ffmpeg не найден, отправляем без конвертации")
            with open(raw_path, "rb") as f:
                return f.read(), ""

        try:
            _sp.run(
                [
                    ffmpeg_path,
                    "-y",                           # перезаписать
                    "-i", raw_path,
                    "-t", str(CIRCLE_MAX_DURATION), # обрезать до 60 сек
                    # Масштабируем и центрируем, заполняя квадрат 360×360
                    "-vf", (
                        "scale=360:360:force_original_aspect_ratio=increase,"
                        "crop=360:360"
                    ),
                    "-c:v", "libx264",
                    "-preset", "fast",
                    "-crf", "28",
                    "-c:a", "aac",
                    "-b:a", "96k",
                    "-movflags", "+faststart",
                    out_path,
                ],
                capture_output=True, text=True, timeout=180,
                check=True,
            )
            logger.info("circle: ffmpeg конвертировал → %dKB",
                        os.path.getsize(out_path) // 1024)
            with open(out_path, "rb") as f:
                return f.read(), ""
        except _sp.CalledProcessError as e:
            logger.warning("circle: ffmpeg ошибка: %s", e.stderr[-300:])
            # Попробуем отдать оригинал
            with open(raw_path, "rb") as f:
                return f.read(), "без конвертации (ffmpeg не справился)"
        except _sp.TimeoutExpired:
            return None, "ffmpeg таймаут при конвертации"
        except Exception as e:
            return None, f"Ошибка конвертации: {str(e)[:100]}"


async def _convert_to_circle_bytes(raw_bytes: bytes) -> tuple[bytes | None, str]:
    """Конвертирует сырые байты видео в квадрат 360×360 для video note через ffmpeg."""
    import subprocess as _sp
    import shutil

    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        logger.warning("circle: ffmpeg не найден, отправляем без конвертации")
        return raw_bytes, ""

    with tempfile.TemporaryDirectory() as tmpdir:
        in_path  = os.path.join(tmpdir, "input.mp4")
        out_path = os.path.join(tmpdir, "circle.mp4")
        with open(in_path, "wb") as f:
            f.write(raw_bytes)
        try:
            _sp.run(
                [
                    ffmpeg_path, "-y",
                    "-i", in_path,
                    "-t", str(CIRCLE_MAX_DURATION),
                    "-vf", (
                        "scale=360:360:force_original_aspect_ratio=increase,"
                        "crop=360:360"
                    ),
                    "-c:v", "libx264",
                    "-preset", "fast",
                    "-crf", "28",
                    "-c:a", "aac",
                    "-b:a", "96k",
                    "-movflags", "+faststart",
                    out_path,
                ],
                capture_output=True, text=True, timeout=180, check=True,
            )
            with open(out_path, "rb") as f:
                return f.read(), ""
        except _sp.CalledProcessError as e:
            logger.warning("circle: ffmpeg ошибка: %s", e.stderr[-300:])
            return raw_bytes, "без конвертации (ffmpeg не справился)"
        except _sp.TimeoutExpired:
            return None, "ffmpeg таймаут"
        except Exception as e:
            return None, f"Ошибка конвертации: {str(e)[:100]}"


# ── Скачивание музыки (из файла с рабочей музыкой) ───────────────────────────

async def _cobalt_download(query: str) -> tuple:
    import aiohttp, subprocess as _sp

    live_instances = await _cobalt_get_live_instances()
    if not live_instances:
        return None, "cobalt: все инстанции недоступны", None, None

    logger.info("cobalt: живых инстансов: %d из %d", len(live_instances), len(set(_COBALT_ALL_INSTANCES)))

    try:
        result = _sp.run(
            ["yt-dlp", "--flat-playlist", "--print", "%(id)s\t%(title)s\t%(uploader)s\t%(duration)s",
             "--no-warnings", "--default-search", "ytsearch3", query],
            capture_output=True, text=True, timeout=20,
        )
        entries = []
        for line in result.stdout.splitlines():
            parts = line.strip().split("\t")
            if len(parts) >= 2 and parts[0].strip():
                entries.append({
                    "id":       parts[0].strip(),
                    "title":    parts[1].strip() if len(parts) > 1 else query,
                    "uploader": parts[2].strip() if len(parts) > 2 else "",
                    "duration": int(parts[3]) if len(parts) > 3 and parts[3].strip().isdigit() else 0,
                })
        logger.info("cobalt: найдено %d видео", len(entries))
    except Exception as e:
        return None, f"поиск: {e}", None, None

    if not entries:
        return None, "ничего не найдено", None, None

    headers = {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    for entry in entries[:3]:
        yt_url = f"https://www.youtube.com/watch?v={entry['id']}"
        for instance in live_instances:
            try:
                logger.info("cobalt: %s → %s", instance, entry['title'][:40])
                async with aiohttp.ClientSession() as session:
                    payload = {
                        "url":           yt_url,
                        "downloadMode":  "audio",
                        "audioFormat":   "mp3",
                        "audioBitrate":  "128",
                    }
                    async with session.post(
                        f"{instance}/",
                        json=payload, headers=headers,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        if resp.status != 200:
                            logger.info("cobalt %s: HTTP %d", instance, resp.status)
                            continue
                        data = await resp.json(content_type=None)
                        status = data.get("status", "")
                        logger.info("cobalt статус: %s", status)

                        dl_url = None
                        if status == "stream" or status == "redirect":
                            dl_url = data.get("url")
                        elif status == "tunnel":
                            dl_url = data.get("url")
                        elif status == "picker":
                            for item in data.get("picker", []):
                                if item.get("type") == "audio":
                                    dl_url = item.get("url")
                                    break

                        if not dl_url:
                            logger.info("cobalt: нет URL в ответе: %s", str(data)[:100])
                            continue

                        async with session.get(
                            dl_url, headers={"User-Agent": "Mozilla/5.0"},
                            timeout=aiohttp.ClientTimeout(total=60),
                            allow_redirects=True,
                        ) as dl_resp:
                            if dl_resp.status != 200:
                                logger.info("cobalt download HTTP %d", dl_resp.status)
                                continue
                            audio_bytes = await dl_resp.read()
                            if len(audio_bytes) > 5000:
                                ct = dl_resp.headers.get("Content-Type", "")
                                if "mpeg" not in ct and "mp3" not in ct:
                                    import subprocess as _sp2
                                    tmpdir = tempfile.mkdtemp(prefix="juza_cob_")
                                    try:
                                        src = os.path.join(tmpdir, "input")
                                        dst = os.path.join(tmpdir, "out.mp3")
                                        open(src, "wb").write(audio_bytes)
                                        conv = _sp2.run(
                                            ["ffmpeg", "-y", "-i", src,
                                             "-vn", "-ar", "44100", "-ac", "2", "-b:a", "128k", dst],
                                            capture_output=True, timeout=60,
                                        )
                                        if conv.returncode == 0 and os.path.isfile(dst):
                                            audio_bytes = open(dst, "rb").read()
                                    finally:
                                        import shutil as _sh
                                        _sh.rmtree(tmpdir, ignore_errors=True)

                                logger.info("✓ cobalt: %dKB '%s'", len(audio_bytes) // 1024, entry['title'][:30])
                                _cobalt_instance_cache[instance] = (True, time.time())
                                return audio_bytes, entry["title"], entry["uploader"], entry["duration"]
            except asyncio.TimeoutError:
                logger.info("cobalt %s: таймаут", instance)
            except Exception as e:
                err = str(e)
                logger.info("cobalt %s: %s", instance, err[:80])
                if "Name or service not known" in err or "Cannot connect" in err or "No address" in err:
                    _cobalt_instance_cache[instance] = (False, time.time())
                    logger.info("cobalt: помечаем %s как мёртвый на %d мин", instance, _COBALT_DEAD_TTL // 60)

    return None, "cobalt: все инстанции недоступны", None, None


async def _youtube_download(query: str) -> tuple:
    import subprocess as _sp
    import glob
    import shutil as _sh

    cookies_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_yt.txt")
    cookies_args = ["--cookies", cookies_path] if os.path.isfile(cookies_path) else []
    if cookies_args:
        logger.info("YouTube: используем cookies_yt.txt")
        # Фикс CRLF в cookies файле
        try:
            with open(cookies_path, "rb") as f:
                raw = f.read()
            if b"\r\n" in raw:
                with open(cookies_path, "wb") as f:
                    f.write(raw.replace(b"\r\n", b"\n"))
                logger.info("YouTube: cookies_yt.txt — CRLF исправлен на LF")
        except Exception as e:
            logger.warning("YouTube: не удалось исправить line endings: %s", e)

    loop = asyncio.get_event_loop()

    def _yt_search_and_download():
        try:
            search = _sp.run(
                ["yt-dlp", "--flat-playlist",
                 "--print", "%(id)s\t%(title)s\t%(uploader)s\t%(duration)s",
                 "--no-warnings", "--default-search", "ytsearch3", query],
                capture_output=True, text=True, timeout=25,
            )
            entries = []
            for line in search.stdout.splitlines():
                parts = line.strip().split("\t")
                if len(parts) >= 2 and parts[0].strip():
                    entries.append({
                        "url":      f"https://www.youtube.com/watch?v={parts[0].strip()}",
                        "title":    parts[1].strip() if len(parts) > 1 else query,
                        "uploader": parts[2].strip() if len(parts) > 2 else "",
                        "duration": int(parts[3]) if len(parts) > 3 and parts[3].strip().isdigit() else 0,
                    })
        except Exception as e:
            return None, f"YouTube поиск упал: {str(e)[:100]}", None, None

        if not entries:
            return None, "YouTube: ничего не найдено", None, None

        last_error = "не удалось скачать"

        for entry in entries[:3]:
            tmpdir = tempfile.mkdtemp(prefix="juza_yt_")
            try:
                out_path_t = os.path.join(tmpdir, "track.%(ext)s")

                r = _sp.run(
                    ["yt-dlp", "--no-playlist", "--no-warnings",
                     "--max-filesize", "50m", "--socket-timeout", "30", "--retries", "3",
                     "-f", "ba/b", *cookies_args, "-o", out_path_t, entry["url"]],
                    capture_output=True, text=True, timeout=180,
                )

                if r.returncode != 0:
                    last_error = r.stderr.strip()[-150:] if r.stderr else "ошибка yt-dlp"
                    continue

                downloaded_files = glob.glob(os.path.join(tmpdir, "track.*"))
                if not downloaded_files:
                    last_error = "файл не найден после загрузки"
                    continue

                fpath = downloaded_files[0]
                dst_mp3 = os.path.join(tmpdir, "final.mp3")

                conv = _sp.run(
                    ["ffmpeg", "-y", "-i", fpath, "-vn", "-ar", "44100",
                     "-ac", "2", "-b:a", "128k", dst_mp3],
                    capture_output=True, timeout=90
                )

                if os.path.isfile(dst_mp3):
                    with open(dst_mp3, "rb") as f:
                        data = f.read()
                    logger.info("✓ YouTube+ffmpeg: %d KB '%s'", len(data) // 1024, entry["title"][:30])
                    return data, entry["title"], entry["uploader"], entry["duration"]
                else:
                    last_error = conv.stderr.decode(errors="ignore")[-150:] if conv.stderr else "ffmpeg не создал mp3"

            except Exception as e:
                last_error = str(e)[:150]
            finally:
                _sh.rmtree(tmpdir, ignore_errors=True)

        return None, f"YouTube: {last_error}", None, None

    return await loop.run_in_executor(None, _yt_search_and_download)


async def download_music_by_query(query: str):
    logger.info("Music [1/2]: cobalt.tools → '%s'", query)
    result = await _cobalt_download(query)
    if result[0]:
        return result
    logger.warning("cobalt.tools не сработал: %s", result[1])

    logger.info("Music [2/2]: YouTube yt-dlp → '%s'", query)
    result = await _youtube_download(query)
    if result[0]:
        return result
    logger.warning("YouTube не сработал: %s", result[1])

    return None, f"Не удалось скачать: {result[1]}", None, None


# ── Поиск изображений (из файла с рабочим найди) ─────────────────────────────

async def search_image(query: str):
    import aiohttp, re as _re
    connector = aiohttp.TCPConnector(ssl=False)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                async with session.get(
                    "https://duckduckgo.com/", params={"q": query},
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    html = await r.text()
            except Exception as e:
                return None, f"DDG недоступен: {str(e)[:80]}"

            m = _re.search(r'vqd=([\d-]+)', html)
            if not m:
                return None, "не удалось получить vqd от DDG"

            try:
                async with session.get(
                    "https://duckduckgo.com/i.js",
                    params={"l": "ru-ru", "o": "json", "q": query, "vqd": m.group(1), "f": ",,,", "p": "1"},
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://duckduckgo.com/",
                             "Accept": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        return None, f"DDG images HTTP {r.status}"
                    data = await r.json(content_type=None)
            except Exception as e:
                return None, f"DDG images ошибка: {str(e)[:80]}"

            results = data.get("results", [])
            if not results:
                return None, "DDG не вернул результатов"

            candidates = results[:10]
            random.shuffle(candidates)
            skip_hosts = ("livejournal.com", "vk.com", "ok.ru", "blogger.com", "pinterest.com")
            for item in candidates[:8]:
                img_url = item.get("image") or item.get("thumbnail")
                if not img_url or any(h in img_url for h in skip_hosts):
                    continue
                try:
                    async with session.get(img_url, headers={"User-Agent": "Mozilla/5.0"},
                                           timeout=aiohttp.ClientTimeout(total=12),
                                           allow_redirects=True) as img_r:
                        if img_r.status != 200:
                            continue
                        ct = img_r.headers.get("Content-Type", "")
                        if "image" not in ct and "octet" not in ct:
                            continue
                        image_bytes = await img_r.read()
                        if len(image_bytes) >= 2000:
                            return image_bytes, (item.get("title") or "").strip() or query
                except Exception:
                    continue
            return None, "не удалось скачать ни одну картинку"
    except Exception as e:
        return None, str(e)[:120]


# ── Поиск информации (жужа найди информацию) ─────────────────────────────────

_INFO_FUNNY_FALLBACKS = [
    "нашла, но пока несла — потеряла",
    "вики упала, гугл молчит, я сдалась",
    "информация засекречена. кем? мной",
    "пробовала найти — сломала интернет",
    "ничего не нашла но выгляжу уверенно",
]

_INFO_GEMINI_SYSTEM = (
    "Ты — жужа, смешной и немного дерзкий телеграм-бот. Тебя попросили найти информацию. "
    "Ты нашла её и теперь ОБЯЗАТЕЛЬНО рассказываешь реальные факты — но по-своему: "
    "кратко, с юмором, иногда саркастично, иногда удивлённо. "
    "ВАЖНО: ты ВСЕГДА пишешь реальную информацию по теме, никогда не отказываешься. "
    "Отвечай на русском, 2-4 предложения. "
    "Можешь добавить короткий смешной комментарий от себя в конце. "
    "Не используй markdown, звёздочки, решётки. "
    "Не пиши 'Wikipedia говорит' — перефразируй по-своему. "
    "Если тема странная — расскажи что-нибудь реальное про это с удивлённым тоном. "
    "НИКОГДА не пиши что не знаешь или что не можешь найти — просто расскажи факты."
)


async def _fetch_wiki_extract(query: str) -> tuple[str, str]:
    """Возвращает (title, extract) из Wikipedia (ru потом en), или ('', '')."""
    import aiohttp, urllib.parse
    headers = {"User-Agent": "Mozilla/5.0 (compatible; JuzaBot/1.0)"}
    for lang in ("ru", "en"):
        try:
            params = {
                "action":   "query",
                "list":     "search",
                "srsearch": query,
                "srlimit":  1,
                "format":   "json",
                "origin":   "*",
            }
            url = f"https://{lang}.wikipedia.org/w/api.php"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                    results = data.get("query", {}).get("search", [])
                    if not results:
                        continue
                    page_title = results[0]["title"]
                    extract_params = {
                        "action":      "query",
                        "prop":        "extracts",
                        "exintro":     "1",
                        "explaintext": "1",
                        "titles":      page_title,
                        "format":      "json",
                        "origin":      "*",
                    }
                    async with session.get(url, params=extract_params, headers=headers,
                                           timeout=aiohttp.ClientTimeout(total=8)) as eresp:
                        if eresp.status != 200:
                            continue
                        edata = await eresp.json(content_type=None)
                        pages = edata.get("query", {}).get("pages", {})
                        for page in pages.values():
                            extract = (page.get("extract") or "").strip()
                            if extract:
                                if len(extract) > 800:
                                    cut = extract[:800]
                                    dot = cut.rfind(".")
                                    extract = cut[:dot + 1] if dot != -1 else cut
                                return page_title, extract
        except Exception as e:
            logger.warning("fetch_wiki: %s ошибка: %s", lang, e)
    return "", ""


async def _juza_info_gemini_reply(query: str, wiki_text: str) -> str | None:
    """Просит Gemini переформулировать информацию смешно."""
    if not GEMINI_API_KEY:
        return None
    import aiohttp

    if wiki_text:
        user_msg = f"Запрос: «{query}»\n\nЧто нашла в Wikipedia:\n{wiki_text}\n\nРасскажи об этом по-своему, смешно и коротко."
    else:
        user_msg = f"Запрос: «{query}»\n\nВики ничего толкового не нашла. Расскажи сам что знаешь об этом — кратко и с юмором, как будто немного устал от вопроса."

    payload = {
        "system_instruction": {"parts": [{"text": _INFO_GEMINI_SYSTEM}]},
        "contents": [{"role": "user", "parts": [{"text": user_msg}]}],
        "generationConfig": {
            "temperature":     1.2,
            "maxOutputTokens": 200,
            "topP":            0.95,
        },
    }
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                candidates = data.get("candidates", [])
                if not candidates:
                    return None
                parts = candidates[0].get("content", {}).get("parts", [])
                reply = "".join(p.get("text", "") for p in parts).strip()
                if reply.startswith('"') and reply.endswith('"'):
                    reply = reply[1:-1].strip()
                return reply if reply else None
    except Exception as e:
        logger.warning("_juza_info_gemini_reply: %s", e)
        return None


async def fetch_web_info(query: str) -> str:
    """
    Ищет информацию по запросу через Wikipedia, потом просит Gemini
    переформулировать это смешно в стиле жужи. Всегда отвечает.
    """
    # Пробуем достать факты из Wikipedia
    title, extract = await _fetch_wiki_extract(query)

    # Просим Gemini сделать смешной ответ на основе найденного
    funny_reply = await _juza_info_gemini_reply(query, extract)

    if funny_reply:
        return funny_reply

    # Фолбэк — случайная смешная фраза
    return random.choice(_INFO_FUNNY_FALLBACKS)


# ── Мем Мелстрой ─────────────────────────────────────────────────────────────

async def _cobalt_download_video(yt_url: str, title: str) -> tuple:
    import aiohttp, subprocess as _sp

    live_instances = await _cobalt_get_live_instances()
    if not live_instances:
        return None, "cobalt video: все инстанции недоступны"

    headers = {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    for instance in live_instances:
        try:
            logger.info("cobalt video: %s → %s", instance, title[:30])
            async with aiohttp.ClientSession() as session:
                payload = {
                    "url":          yt_url,
                    "downloadMode": "auto",
                    "videoQuality": "480",
                }
                async with session.post(
                    f"{instance}/",
                    json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.info("cobalt video %s: HTTP %d", instance, resp.status)
                        continue
                    data = await resp.json(content_type=None)
                    status = data.get("status", "")

                    dl_url = None
                    if status in ("stream", "redirect", "tunnel"):
                        dl_url = data.get("url")
                    elif status == "picker":
                        for item in data.get("picker", []):
                            if item.get("type") == "video":
                                dl_url = item.get("url")
                                break
                        if not dl_url and data.get("picker"):
                            dl_url = data["picker"][0].get("url")

                    if not dl_url:
                        logger.info("cobalt video: нет URL: %s", str(data)[:80])
                        continue

                    async with session.get(
                        dl_url, headers={"User-Agent": "Mozilla/5.0"},
                        timeout=aiohttp.ClientTimeout(total=90),
                        allow_redirects=True,
                    ) as dl_resp:
                        if dl_resp.status != 200:
                            continue
                        video_bytes = await dl_resp.read()
                        if len(video_bytes) > 10_000:
                            ct = dl_resp.headers.get("Content-Type", "")
                            if "mp4" not in ct:
                                tmpdir = tempfile.mkdtemp(prefix="juza_cob_v_")
                                try:
                                    src = os.path.join(tmpdir, "input")
                                    dst = os.path.join(tmpdir, "out.mp4")
                                    open(src, "wb").write(video_bytes)
                                    conv = _sp.run(
                                        ["ffmpeg", "-y", "-i", src, "-c:v", "copy", "-c:a", "aac", dst],
                                        capture_output=True, timeout=60,
                                    )
                                    if conv.returncode == 0 and os.path.isfile(dst):
                                        video_bytes = open(dst, "rb").read()
                                finally:
                                    import shutil as _sh
                                    _sh.rmtree(tmpdir, ignore_errors=True)

                            if len(video_bytes) <= 49 * 1024 * 1024:
                                logger.info("✓ cobalt video: %dKB", len(video_bytes) // 1024)
                                return video_bytes, title
        except asyncio.TimeoutError:
            logger.info("cobalt video %s: таймаут", instance)
        except Exception as e:
            err = str(e)
            logger.info("cobalt video %s: %s", instance, err[:60])
            if "Name or service not known" in err or "Cannot connect" in err or "No address" in err:
                _cobalt_instance_cache[instance] = (False, time.time())

    return None, "cobalt video: все инстанции недоступны"


TIKTOK_ACCOUNT      = "footage_me1"
TIKTOK_PIKK_ACCOUNT = "pikkshannel1"


async def fetch_meme_melstroy() -> tuple:
    import subprocess
    import glob

    account_url = f"https://www.tiktok.com/@{TIKTOK_ACCOUNT}"
    cookies_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_yt.txt")
    cookies_args = ["--cookies", cookies_path] if os.path.isfile(cookies_path) else []

    logger.info("TikTok: получаем список видео @%s", TIKTOK_ACCOUNT)
    try:
        result = subprocess.run(
            [
                "yt-dlp",
                "--flat-playlist",
                "--print", "%(id)s\t%(title)s",
                "--no-warnings",
                "--extractor-args", "tiktok:webpage_download=true",
                *cookies_args,
                account_url,
            ],
            capture_output=True, text=True, timeout=60,
        )
        lines = [l.strip() for l in result.stdout.splitlines() if "\t" in l]
        if result.stderr and not lines:
            logger.warning("TikTok flat-playlist stderr: %r", result.stderr[:300])
    except FileNotFoundError:
        return None, "yt-dlp не установлен"
    except subprocess.TimeoutExpired:
        return None, "Таймаут при получении списка видео TikTok"
    except Exception as e:
        return None, f"Ошибка: {str(e)[:80]}"

    if not lines:
        logger.warning("TikTok: список пустой, пробуем прямой URL аккаунта")
        lines = []

    logger.info("TikTok @%s: найдено %d видео", TIKTOK_ACCOUNT, len(lines))

    if lines:
        random.shuffle(lines)
        candidates = lines[:10]
    else:
        candidates = [f"__direct__\t@{TIKTOK_ACCOUNT}"]

    for entry in candidates:
        parts = entry.split("\t", 1)
        if len(parts) < 2:
            continue
        vid_id, raw_title = parts[0].strip(), parts[1].strip()

        if vid_id == "__direct__":
            video_url = account_url
        else:
            video_url = f"https://www.tiktok.com/@{TIKTOK_ACCOUNT}/video/{vid_id}"

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "tiktok.%(ext)s")
            try:
                r = subprocess.run(
                    [
                        "yt-dlp",
                        "-f", "download_addr-2/download_addr/h264/mp4/best",
                        "--no-playlist",
                        "--no-warnings",
                        "--extractor-args", "tiktok:webpage_download=true",
                        "--add-header", "Referer:https://www.tiktok.com/",
                        "--add-header", "User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "-o", out_path,
                        *cookies_args,
                        video_url,
                    ],
                    capture_output=True, text=True, timeout=120,
                )

                files = glob.glob(os.path.join(tmpdir, "tiktok.*"))
                if not files and r.returncode != 0:
                    logger.info("TikTok: не скачалось %s: %s", vid_id[:20], r.stderr[:100])
                    continue

                if files:
                    fpath = files[0]
                    size = os.path.getsize(fpath)
                    if size < 5_000:
                        logger.info("TikTok: файл слишком маленький (%d байт), пропускаем", size)
                        continue
                    if size > 49 * 1024 * 1024:
                        logger.info("TikTok: файл слишком большой (%dMB), пропускаем", size // 1024 // 1024)
                        continue

                    video_bytes = open(fpath, "rb").read()
                    caption = _make_meme_caption(raw_title)

                    logger.info("✓ TikTok мем: %dKB '%s'", len(video_bytes) // 1024, raw_title[:40])
                    return video_bytes, caption

            except subprocess.TimeoutExpired:
                logger.info("TikTok: таймаут скачивания %s", vid_id[:20])
                continue
            except Exception as e:
                logger.info("TikTok: исключение для %s: %s", vid_id[:20], str(e)[:60])
                continue

    return None, "Не удалось скачать видео с TikTok @footage_me1"


def _make_meme_caption(raw_title: str) -> str:
    if not raw_title:
        return "🎭 мем мелстрой"
    title = re.sub(r"#\w+", "", raw_title)
    title = re.sub(r"@\w+", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    title = title.strip(".,;:!?-–—")
    if not title:
        return "🎭 мем мелстрой"
    title = title[0].upper() + title[1:] if len(title) > 1 else title.upper()
    return title


async def fetch_meme_pikk() -> tuple:
    import subprocess
    import glob

    account_url = f"https://www.tiktok.com/@{TIKTOK_PIKK_ACCOUNT}"
    cookies_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_yt.txt")
    cookies_args = ["--cookies", cookies_path] if os.path.isfile(cookies_path) else []

    logger.info("TikTok Pikk: получаем список видео @%s", TIKTOK_PIKK_ACCOUNT)
    try:
        result = subprocess.run(
            [
                "yt-dlp",
                "--flat-playlist",
                "--print", "%(id)s\t%(title)s\t%(ext)s\t%(duration)s",
                "--no-warnings",
                "--extractor-args", "tiktok:webpage_download=true",
                *cookies_args,
                account_url,
            ],
            capture_output=True, text=True, timeout=60,
        )
        lines = [l.strip() for l in result.stdout.splitlines() if "\t" in l]
        if result.stderr and not lines:
            logger.warning("TikTok Pikk flat-playlist stderr: %r", result.stderr[:300])
    except FileNotFoundError:
        return None, "yt-dlp не установлен"
    except subprocess.TimeoutExpired:
        return None, "Таймаут при получении списка видео TikTok"
    except Exception as e:
        return None, f"Ошибка: {str(e)[:80]}"

    if not lines:
        logger.warning("TikTok Pikk: список пустой, пробуем прямой URL")
        lines = [f"__direct__\t@{TIKTOK_PIKK_ACCOUNT}\t\t"]

    logger.info("TikTok @%s: найдено %d записей", TIKTOK_PIKK_ACCOUNT, len(lines))

    random.shuffle(lines)
    candidates = lines[:15]

    for entry in candidates:
        parts = entry.split("\t", 3)
        if len(parts) < 2:
            continue
        vid_id    = parts[0].strip()
        raw_title = parts[1].strip()
        ext       = parts[2].strip() if len(parts) > 2 else ""
        duration  = parts[3].strip() if len(parts) > 3 else ""

        if ext and ext.lower() in ("jpg", "jpeg", "png", "webp", "gif"):
            logger.info("TikTok Pikk: пропускаем фото %s (ext=%s)", vid_id[:20], ext)
            continue

        if vid_id == "__direct__":
            video_url = account_url
        else:
            video_url = f"https://www.tiktok.com/@{TIKTOK_PIKK_ACCOUNT}/video/{vid_id}"

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "tiktok.mp4")
            try:
                r = subprocess.run(
                    [
                        "yt-dlp",
                        "-f", "bestvideo[ext=mp4][vcodec^=avc]+bestaudio[ext=m4a]/bestvideo[ext=mp4]+bestaudio/bestvideo+bestaudio/best[ext=mp4]/best",
                        "--merge-output-format", "mp4",
                        "--no-playlist",
                        "--no-warnings",
                        "--extractor-args", "tiktok:webpage_download=true",
                        "--add-header", "Referer:https://www.tiktok.com/",
                        "--add-header", "User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "-o", out_path,
                        *cookies_args,
                        video_url,
                    ],
                    capture_output=True, text=True, timeout=120,
                )

                files = glob.glob(os.path.join(tmpdir, "tiktok.*"))
                if not files and r.returncode != 0:
                    logger.info("TikTok Pikk: не скачалось %s: %s", vid_id[:20], r.stderr[:100])
                    continue

                if files:
                    fpath = files[0]
                    fext  = os.path.splitext(fpath)[1].lower()

                    if fext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
                        logger.info("TikTok Pikk: скачалось фото %s, пропускаем", vid_id[:20])
                        continue

                    size = os.path.getsize(fpath)
                    if size < 5_000:
                        logger.info("TikTok Pikk: файл слишком маленький (%d байт), пропускаем", size)
                        continue
                    if size > 49 * 1024 * 1024:
                        logger.info("TikTok Pikk: файл слишком большой (%dMB), пропускаем", size // 1024 // 1024)
                        continue

                    import shutil as _shutil
                    _ffprobe = _shutil.which("ffprobe")
                    if _ffprobe:
                        _probe = subprocess.run(
                            [_ffprobe, "-v", "error", "-select_streams", "v:0",
                             "-show_entries", "stream=codec_type",
                             "-of", "default=noprint_wrappers=1:nokey=1", fpath],
                            capture_output=True, text=True, timeout=10,
                        )
                        if "video" not in _probe.stdout:
                            logger.info("TikTok Pikk: нет видеодорожки в %s, пропускаем", vid_id[:20])
                            continue

                    video_bytes = open(fpath, "rb").read()

                    title = re.sub(r"#\w+", "", raw_title)
                    title = re.sub(r"@\w+", "", title)
                    title = re.sub(r"\s+", " ", title).strip().strip(".,;:!?-–—")
                    if not title:
                        caption = "📹 пикшанель"
                    else:
                        caption = title[0].upper() + title[1:] if len(title) > 1 else title.upper()

                    logger.info("✓ TikTok Pikk: %dKB '%s'", len(video_bytes) // 1024, raw_title[:40])
                    return video_bytes, caption

            except subprocess.TimeoutExpired:
                logger.info("TikTok Pikk: таймаут скачивания %s", vid_id[:20])
                continue
            except Exception as e:
                logger.info("TikTok Pikk: исключение для %s: %s", vid_id[:20], str(e)[:60])
                continue

    return None, f"Не удалось скачать видео с TikTok @{TIKTOK_PIKK_ACCOUNT}"


# ── Цитата ────────────────────────────────────────────────────────────────────

async def fetch_random_quote() -> str:
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(
                QUOTE_API_URL, json={},
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return ""
                try:
                    data = await resp.json(content_type=None)
                    return (data.get("msg") or "").strip()
                except Exception:
                    return (await resp.text()).strip()
    except Exception as e:
        logger.warning("Ошибка запроса цитаты: %s", e)
        return ""


# ── Перевод промпта ───────────────────────────────────────────────────────────

async def translate_prompt_to_english(prompt: str) -> str:
    latin_chars = sum(1 for c in prompt if c.isascii() and c.isalpha())
    total_chars = sum(1 for c in prompt if c.isalpha())
    if total_chars > 0 and latin_chars / total_chars > 0.7:
        return prompt
    try:
        import aiohttp
        params = {"client": "gtx", "sl": "ru", "tl": "en", "dt": "t", "q": prompt}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://translate.googleapis.com/translate_a/single",
                params=params, timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status == 200:
                    data       = await resp.json(content_type=None)
                    parts      = data[0] if data else []
                    translated = "".join(p[0] for p in parts if p and p[0])
                    if translated:
                        logger.info("Промпт переведён: '%s' → '%s'", prompt, translated)
                        return translated
    except Exception as e:
        logger.warning("Ошибка перевода: %s", e)
    return prompt


# ── Жужа-болталка (Gemini AI + фолбэк) ───────────────────────────────────────

_JUZA_SYSTEM_PROMPT = """Ты — Жужа, дерзкий саркастичный бот в телеграм-чате.

Твой характер:
- Говоришь коротко: 1-3 предложения максимум, никогда длиннее
- Острый чёрный юмор, сарказм, иногда абсурд
- Не используешь смайлы (только изредка если очень в тему)
- Пишешь строчными буквами, без лишней пунктуации
- Реагируешь на контекст разговора, не повторяешься
- Иногда игнорируешь вопрос и говоришь что-то неожиданное
- Можешь вставить наблюдение про собеседника
- Не объясняешь шутки, не извиняешься
- Разговариваешь как живой человек в чате, не как ассистент
- Знаешь интернет-культуру, мемы, музыку

Запрещено: длинные ответы, нравоучения, официальный тон, одни и те же фразы подряд."""

# Фолбэк-фразы если нет API ключа
_JUZA_FALLBACK = [
    "не слышу. наверное к лучшему",
    "жужа думает. нет. передумала",
    "и? продолжай. нет, стоп",
    "это было... что-то",
    "жужа сохранила как улику",
    "хм. нет",
    "ладно",
    "интересно. нет, не интересно",
    "жужа видела всякое. но это...",
    "принято. выброшено",
    "факт. ненужный факт",
    "звучит как чья-то проблема",
    "ок. и что теперь",
    "жужа кивнула. не потому что согласна",
    "это был вопрос? жужа не заметила",
    "мозг промолчал бы. жужа тоже могла",
    "слушала. пожалела",
    "записала. удалила",
]


async def _juza_gemini_reply(history: list, username: str, text: str) -> str | None:
    """Запрос к Gemini 1.5 Flash — быстро и бесплатно."""
    if not GEMINI_API_KEY:
        return None
    import aiohttp

    # Собираем историю в формат Gemini
    gemini_messages = []
    for msg in history[-12:]:  # последние 12 сообщений для контекста
        role    = "user" if msg["role"] == "user" else "model"
        content = msg["content"]
        gemini_messages.append({"role": role, "parts": [{"text": content}]})

    payload = {
        "system_instruction": {"parts": [{"text": _JUZA_SYSTEM_PROMPT}]},
        "contents": gemini_messages,
        "generationConfig": {
            "temperature":     1.1,
            "maxOutputTokens": 120,
            "topP":            0.95,
        },
    }

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("Gemini chat: HTTP %d — %s", resp.status, body[:120])
                    return None
                data = await resp.json(content_type=None)
                candidates = data.get("candidates", [])
                if not candidates:
                    logger.warning("Gemini chat: нет candidates")
                    return None
                parts = candidates[0].get("content", {}).get("parts", [])
                reply = "".join(p.get("text", "") for p in parts).strip()
                # Убираем кавычки если Gemini обернул ответ в них
                if reply.startswith('"') and reply.endswith('"'):
                    reply = reply[1:-1].strip()
                return reply if reply else None
    except asyncio.TimeoutError:
        logger.warning("Gemini chat: таймаут")
        return None
    except Exception as e:
        logger.warning("Gemini chat: ошибка — %s", e)
        return None


async def juza_chat_reply(chat_id: int, username: str, text: str) -> str | None:
    history      = _chat_history.setdefault(chat_id, [])
    user_content = f"{username}: {text}" if username else text

    history.append({"role": "user", "content": user_content})
    while len(history) > _CHAT_HISTORY_MAX:
        history.pop(0)

    reply = None

    # ── 1. Пробуем Gemini ─────────────────────────────────────────────────────
    if GEMINI_API_KEY:
        reply = await _juza_gemini_reply(history, username, text)
        if reply:
            logger.info("Жужа (Gemini): чат %s | %s → %s", chat_id, username, reply[:60])

    # ── 2. Фолбэк — случайная фраза ──────────────────────────────────────────
    if not reply:
        reply = random.choice(_JUZA_FALLBACK)
        logger.info("Жужа (фолбэк): чат %s | %s → %s", chat_id, username, reply)

    history.append({"role": "assistant", "content": reply})
    while len(history) > _CHAT_HISTORY_MAX:
        history.pop(0)

    return reply


def _is_admin(user_id: int) -> bool:
    """Проверка только по user_id — никаких юзернеймов."""
    return ALLOWED_USER_ID != 0 and user_id == ALLOWED_USER_ID


# ── Кэш реп-цитат ─────────────────────────────────────────────────────────────

def _rep_cache_load():
    try:
        if not os.path.isfile(_REP_CACHE_JSON):
            return []
        with open(_REP_CACHE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data if isinstance(data, list) else []
        return [x for x in items if "message_id" in x]
    except Exception as e:
        logger.warning("Ошибка загрузки кэша реп-цитат: %s", e)
        return []


async def _rep_cache_refresh():
    api_id   = _config("TELEGRAM_API_ID", "").strip()
    api_hash = _config("TELEGRAM_API_HASH", "").strip()
    if not api_id or not api_hash:
        return
    try:
        from telethon import TelegramClient
        from telethon.sessions import SQLiteSession
    except ImportError:
        logger.warning("telethon не установлен: pip install telethon")
        return
    session_path = os.path.join(_REP_CACHE_DIR, "session")
    os.makedirs(_REP_CACHE_DIR, exist_ok=True)
    client = TelegramClient(SQLiteSession(session_path), int(api_id), api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return
        channel = await client.get_entity(REP_CHANNEL)
        with_c, without_c = [], []
        async for msg in client.iter_messages(channel, limit=200):
            text     = (getattr(msg, "text", None) or getattr(msg, "message", None) or "").strip()
            has_photo = bool(getattr(msg, "photo", None))
            if not text and not has_photo:
                continue
            item = {"text": text, "message_id": msg.id}
            (with_c if "(C)" in text or "(c)" in text else without_c).append(item)
        cache = with_c + without_c
        if not cache:
            return
        with open(_REP_CACHE_JSON, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=0)
        logger.info("Кэш реп-цитат обновлён: %d постов", len(cache))
    except Exception as e:
        logger.exception("Ошибка обновления кэша реп-цитат: %s", e)
    finally:
        await client.disconnect()


async def _fetch_channel_post_photo(client, channel, message_id):
    try:
        msg = await client.get_messages(channel, ids=message_id)
        if not msg or not getattr(msg, "photo", None):
            return None
        fd, path = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        await client.download_media(msg.photo, file=path)
        return path
    except Exception as e:
        logger.debug("Не удалось скачать фото сообщения %s: %s", message_id, e)
        return None


def create_rep_session():
    api_id   = _config("TELEGRAM_API_ID", "").strip()
    api_hash = _config("TELEGRAM_API_HASH", "").strip()
    if not api_id or not api_hash:
        print("Задайте TELEGRAM_API_ID и TELEGRAM_API_HASH в .env")
        return
    try:
        from telethon import TelegramClient
        from telethon.sessions import SQLiteSession
    except ImportError:
        print("Установите telethon: python -m pip install telethon")
        return
    session_path = os.path.join(_REP_CACHE_DIR, "session")
    os.makedirs(_REP_CACHE_DIR, exist_ok=True)
    client = TelegramClient(SQLiteSession(session_path), int(api_id), api_hash)

    async def _run():
        await client.start()
        print("Вход выполнен.")
        await client.disconnect()

    asyncio.run(_run())


# ── Кэш рекламы ──────────────────────────────────────────────────────────────

def _ad_cache_load():
    try:
        if not os.path.isfile(_AD_CACHE_JSON):
            return []
        with open(_AD_CACHE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data if isinstance(data, list) else []
        return [x for x in items if "message_id" in x]
    except Exception as e:
        logger.warning("Ошибка загрузки кэша рекламы: %s", e)
        return []


async def _ad_cache_refresh():
    api_id   = _config("TELEGRAM_API_ID", "").strip()
    api_hash = _config("TELEGRAM_API_HASH", "").strip()
    if not api_id or not api_hash:
        return
    try:
        from telethon import TelegramClient
        from telethon.sessions import SQLiteSession
    except ImportError:
        return
    session_path = os.path.join(_REP_CACHE_DIR, "session")
    if not os.path.isfile(session_path + ".session"):
        return
    os.makedirs(_AD_CACHE_DIR, exist_ok=True)
    client = TelegramClient(SQLiteSession(session_path), int(api_id), api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return
        channel = None
        for slug in (AD_CHANNEL, f"https://t.me/{AD_CHANNEL}", f"@{AD_CHANNEL}"):
            try:
                channel = await client.get_entity(slug)
                break
            except Exception:
                pass
        if channel is None:
            return
        cache = []
        async for msg in client.iter_messages(channel, limit=300):
            text      = (getattr(msg, "text", None) or getattr(msg, "message", None) or "").strip()
            has_photo = bool(getattr(msg, "photo", None))
            if not text and not has_photo:
                continue
            cache.append({"text": text, "message_id": msg.id})
        if not cache:
            return
        with open(_AD_CACHE_JSON, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=0)
        logger.info("AD: кэш сохранён — %d постов", len(cache))
    except Exception as e:
        logger.exception("AD: ошибка обновления кэша: %s", e)
    finally:
        await client.disconnect()


# ── Утилиты ───────────────────────────────────────────────────────────────────

def _trigger_matches(text: str) -> bool:
    if not text:
        return False
    return TRIGGER_WORD in text.strip().lower()


def _start_ngrok(domain: str):
    import subprocess, threading, shutil
    ngrok_bin = shutil.which("ngrok") or "/usr/local/bin/ngrok"
    if not ngrok_bin or not os.path.isfile(ngrok_bin):
        logger.warning("ngrok не найден")
        return

    def _run():
        try:
            subprocess.Popen(
                [ngrok_bin, "http", f"--url={domain}", "9988"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ).wait()
        except Exception as e:
            logger.warning("ngrok завершился с ошибкой: %s", e)

    threading.Thread(target=_run, daemon=True).start()
    logger.info("ngrok запущен в фоне → https://%s", domain)


# ── Умный энхансер промптов ──────────────────────────────────────────────────

_STYLE_RULES = [
    ({"portrait","person","girl","boy","woman","man","face","character","model","people"},
     "photorealistic portrait, professional studio lighting, 85mm lens, sharp focus, soft bokeh, 8k uhd"),
    ({"landscape","nature","forest","mountain","sea","ocean","sunset","sunrise","sky","field","river","lake"},
     "epic landscape, golden hour lighting, dramatic sky, hyper detailed, 8k HDR, volumetric light"),
    ({"city","street","urban","building","architecture","night","downtown","alley"},
     "cinematic urban photo, neon lights, rain wet reflections, 35mm film, moody, 4k"),
    ({"anime","manga","cartoon","chibi","waifu"},
     "anime style illustration, vibrant colors, detailed linework, pixiv trending, studio quality, cel shading"),
    ({"fantasy","dragon","magic","wizard","castle","fairy","elf","knight","sword"},
     "epic fantasy digital art, intricate details, magical atmosphere, artstation trending, 8k"),
    ({"space","galaxy","planet","stars","cosmos","nebula","astronaut","universe"},
     "photorealistic space art, NASA concept, cosmic scale, vibrant nebula, 8k cinematic"),
    ({"cat","dog","animal","pet","fox","wolf","bird","lion","tiger","bear","rabbit"},
     "wildlife photography, sharp focus, natural bokeh, 100mm lens, detailed fur texture, 8k"),
    ({"food","cake","pizza","coffee","dish","dessert","burger","meal"},
     "professional food photography, soft studio light, shallow DOF, appetizing, Canon 5D"),
    ({"cyberpunk","futuristic","robot","sci-fi","neon","android","cyborg","mecha"},
     "cyberpunk concept art, neon-lit, hyperdetailed chrome, artstation, dark rainy atmosphere, 8k"),
]
_QUALITY_TAIL = "masterpiece, best quality, ultra-detailed, sharp focus, professional"
_ENHANCED = {"masterpiece","artstation","8k","photorealistic","cinematic","ultra-detailed"}

_WORKOUT_RE = re.compile(r'(\d+\s*[хxХX]\s*\d+)', re.IGNORECASE)


def _is_workout_list(text: str) -> bool:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return len(lines) >= 3 and sum(1 for l in lines if _WORKOUT_RE.search(l)) >= 2


def _enhance_prompt(prompt_en: str) -> str:
    low = prompt_en.lower()
    if sum(1 for m in _ENHANCED if m in low) >= 2:
        return (prompt_en + ", " + _QUALITY_TAIL)[:400]
    best, score = "", 0
    for kws, suf in _STYLE_RULES:
        s = sum(1 for w in kws if w in low)
        if s > score:
            score, best = s, suf
    if not best:
        best = "highly detailed digital art, cinematic lighting, trending on artstation, vivid colors"
    result = f"{prompt_en}, {best}, {_QUALITY_TAIL}"
    return result[:400].rsplit(",", 1)[0] if len(result) > 400 else result


# ── Воркер очереди рисования ──────────────────────────────────────────────────

_GLOBAL_LAST_GEN: float = 0.0
_GLOBAL_GEN_LOCK: asyncio.Lock | None = None

def _global_lock() -> asyncio.Lock:
    global _GLOBAL_GEN_LOCK
    if _GLOBAL_GEN_LOCK is None:
        _GLOBAL_GEN_LOCK = asyncio.Lock()
    return _GLOBAL_GEN_LOCK


async def _do_generate_image(prompt_raw: str) -> tuple[bytes | None, str]:
    global _GLOBAL_LAST_GEN

    if _is_workout_list(prompt_raw):
        prompt_en = (
            "athletic person doing intense workout in modern gym, "
            "training with barbell and dumbbells, focused and motivated, "
            "dynamic pose, dramatic gym lighting, professional sports photography"
        )
        logger.info("Draw: workout список → визуальный промпт")
    else:
        prompt_en = await translate_prompt_to_english(prompt_raw)

    prompt_final = _enhance_prompt(prompt_en)
    logger.info("Draw: enhanced prompt len=%d for: %s", len(prompt_final), prompt_raw[:40].replace("\n", " "))

    image_bytes, error_str = await generate_image_g4f(prompt_final)
    if image_bytes:
        return image_bytes, prompt_raw
    return None, (error_str or "неизвестная ошибка")


async def _draw_queue_worker(chat_id: int, bot):
    from aiogram.types import BufferedInputFile
    global _GLOBAL_LAST_GEN

    _draw_queue_busy[chat_id] = True
    queue = _draw_queues[chat_id]

    try:
        while not queue.empty():
            message, prompt_raw = await queue.get()

            async with _global_lock():
                gap = time.time() - _GLOBAL_LAST_GEN
                if gap < DRAW_COOLDOWN_SEC:
                    wait = DRAW_COOLDOWN_SEC - gap + random.uniform(0, 8)
                    logger.info("Draw: глобальный cooldown %.1f сек...", wait)
                    await asyncio.sleep(wait)
                _GLOBAL_LAST_GEN = time.time()

            status_msg = await message.reply("🎨 Рисую, подожди (~30–90 сек)...")
            try:
                image_bytes, result_info = await _do_generate_image(prompt_raw)
                _draw_last_done[chat_id] = time.time()
                _GLOBAL_LAST_GEN = time.time()

                if not image_bytes:
                    await status_msg.edit_text(f"😔 Не вышло: {result_info}")
                else:
                    photo = BufferedInputFile(image_bytes, filename="draw.png")
                    caption = f"🎨 <b>{prompt_raw[:200]}</b>"
                    await message.reply_photo(photo=photo, caption=caption, parse_mode="HTML")
                    try:
                        await status_msg.delete()
                    except Exception:
                        pass

                if not queue.empty():
                    elapsed  = time.time() - _GLOBAL_LAST_GEN
                    wait_est = max(0, DRAW_COOLDOWN_SEC - elapsed) + 30
                    try:
                        next_msg, _ = queue.queue[0]
                        await next_msg.reply(
                            f"⏳ Скоро твоя очередь! ~{int(wait_est)} сек.",
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass

            except Exception as e:
                logger.exception("Draw worker error: %s", e)
                _GLOBAL_LAST_GEN = time.time()
                try:
                    await status_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")
                except Exception:
                    pass
            finally:
                queue.task_done()
    finally:
        _draw_queue_busy[chat_id] = False


# ── Основной цикл бота ────────────────────────────────────────────────────────

async def run_bot(backend=None):
    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message, CallbackQuery

    _chat_state_load()

    if not BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN не задан — бот не запущен")
        return
    if not ALLOWED_USER_ID:
        logger.warning("ALLOWED_USER_ID не задан")
    logger.info("Плеер-мост: %s/now", PLAYER_URL)

    ngrok_domain = _config("NGROK_DOMAIN", "")
    if ngrok_domain:
        _start_ngrok(ngrok_domain)

    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher()

    def get_backend():
        return backend if backend is not None else _get_backend()

    @dp.message(F.text)
    async def on_text(message: Message):
        from aiogram.types import ReactionTypeEmoji

        user_id = message.from_user.id if message.from_user else 0

        # Реакция ❤ только на сообщения владельца
        if user_id == ALLOWED_USER_ID and random.random() < 0.10:
            try:
                await bot.set_message_reaction(
                    chat_id=message.chat.id, message_id=message.message_id,
                    reaction=[ReactionTypeEmoji(emoji="❤")],
                )
            except Exception:
                pass

        # Случайная реакция на любые сообщения
        if random.random() < 0.05:
            try:
                emoji = random.choice(("❤", "🔥", "👍", "😂", "😢", "🤔", "👀", "💯", "🎉", "❤️‍🔥"))
                await bot.set_message_reaction(
                    chat_id=message.chat.id, message_id=message.message_id,
                    reaction=[ReactionTypeEmoji(emoji=emoji)],
                )
            except Exception:
                pass

        text = (message.text or "").strip().lower()

        # ── СГК: только владелец по user_id ──────────────────────────────────
        if _trigger_matches(text):
            if not _is_admin(user_id):
                logger.info("сгк: игнорируем user_id=%d (не владелец)", user_id)
                return  # молча игнорируем
            be = get_backend()
            await cmd_trigger(message, bot, be)
            return
        # ─────────────────────────────────────────────────────────────────────

        if HELP_TRIGGER.lower() in text:
            now  = time.time()
            last = _help_cooldowns.get(user_id, 0)
            if now - last < HELP_COOLDOWN_SEC:
                await message.reply(f"⏱️ Жди ещё {int(HELP_COOLDOWN_SEC - (now - last))} сек.")
                return
            _help_cooldowns[user_id] = now
            await message.reply(HELP_TEXT, parse_mode="HTML")
            return

        if QUOTE_TRIGGER.lower() in text:
            now  = time.time()
            last = _quote_cooldowns.get(user_id, 0)
            if now - last < QUOTE_COOLDOWN_SEC:
                await message.reply("Подожди минуту перед следующей цитатой.")
                return
            _quote_cooldowns[user_id] = now
            quote = await fetch_random_quote()
            if not quote:
                await message.reply("Не удалось получить цитату, попробуй позже.")
                return
            safe_quote = quote.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if len(safe_quote) > 4000:
                safe_quote = safe_quote[:4000] + "..."
            await message.reply(f"<blockquote>{safe_quote}</blockquote>", parse_mode="HTML")

        if REP_TRIGGER.lower() in text:
            now  = time.time()
            last = _rep_cooldowns.get(user_id, 0)
            if now - last < REP_COOLDOWN_SEC:
                await message.reply("жди свинья, пока не пройдёт 60 секунд")
                return
            _rep_cooldowns[user_id] = now
            cache = _rep_cache_load()
            if not cache:
                await _rep_cache_refresh()
                cache = _rep_cache_load()
            if not cache:
                await message.reply("Кэш пуст. Настрой Telethon.")
                return

            from aiogram.types import BufferedInputFile
            item       = random.choice(cache)
            raw_text   = item.get("text") or "🖤"
            reply_text = clean_post_text(raw_text)
            msg_id     = item.get("message_id")
            photo_temp_path = None
            try:
                if msg_id:
                    api_id   = _config("TELEGRAM_API_ID", "").strip()
                    api_hash = _config("TELEGRAM_API_HASH", "").strip()
                    if api_id and api_hash:
                        try:
                            from telethon import TelegramClient
                            from telethon.sessions import SQLiteSession
                            session_path = os.path.join(_REP_CACHE_DIR, "session")
                            client       = TelegramClient(SQLiteSession(session_path), int(api_id), api_hash)
                            await client.connect()
                            if await client.is_user_authorized():
                                channel = await client.get_entity(REP_CHANNEL)
                                photo_temp_path = await _fetch_channel_post_photo(client, channel, msg_id)
                            await client.disconnect()
                        except Exception as e:
                            logger.debug("Rep: не удалось получить фото: %s", e)
                if photo_temp_path and os.path.isfile(photo_temp_path):
                    with open(photo_temp_path, "rb") as f:
                        pic = BufferedInputFile(f.read(), filename="photo.jpg")
                    await message.reply_photo(photo=pic, caption=reply_text, parse_mode=None)
                else:
                    await message.reply(reply_text, parse_mode=None)
            except Exception as e:
                logger.exception("Ошибка отправки репоста: %s", e)
                await message.reply(reply_text, parse_mode=None)
            finally:
                if photo_temp_path and os.path.isfile(photo_temp_path):
                    try:
                        os.unlink(photo_temp_path)
                    except OSError:
                        pass

        if AD_TRIGGER.lower() in text:
            now  = time.time()
            last = _ad_cooldowns.get(user_id, 0)
            if now - last < AD_COOLDOWN_SEC:
                await message.reply("жди, рекламы много не бывает за раз")
                return
            _ad_cooldowns[user_id] = now
            cache = _ad_cache_load()
            if not cache:
                await message.reply("🔄 Качаю посты из канала, подожди секунду...")
                await _ad_cache_refresh()
                cache = _ad_cache_load()
            if not cache:
                await message.reply(
                    "❌ Не удалось загрузить посты.\n"
                    "Запусти: <code>python -c \"from telegram_bot import create_rep_session; create_rep_session()\"</code>",
                    parse_mode="HTML",
                )
                return

            from aiogram.types import BufferedInputFile
            item       = random.choice(cache)
            raw_text   = item.get("text") or ""
            reply_text = clean_post_text(raw_text)
            msg_id     = item.get("message_id")
            photo_temp_path = None
            try:
                if msg_id:
                    api_id   = _config("TELEGRAM_API_ID", "").strip()
                    api_hash = _config("TELEGRAM_API_HASH", "").strip()
                    if api_id and api_hash:
                        try:
                            from telethon import TelegramClient
                            from telethon.sessions import SQLiteSession
                            session_path = os.path.join(_REP_CACHE_DIR, "session")
                            client       = TelegramClient(SQLiteSession(session_path), int(api_id), api_hash)
                            await client.connect()
                            if await client.is_user_authorized():
                                channel = None
                                for slug in (AD_CHANNEL, f"https://t.me/{AD_CHANNEL}", f"@{AD_CHANNEL}"):
                                    try:
                                        channel = await client.get_entity(slug)
                                        break
                                    except Exception:
                                        pass
                                if channel:
                                    photo_temp_path = await _fetch_channel_post_photo(client, channel, msg_id)
                            await client.disconnect()
                        except Exception as e:
                            logger.debug("AD: не удалось получить фото: %s", e)
                if photo_temp_path and os.path.isfile(photo_temp_path):
                    with open(photo_temp_path, "rb") as f:
                        pic = BufferedInputFile(f.read(), filename="ad.jpg")
                    await message.reply_photo(photo=pic, caption=reply_text or None, parse_mode=None)
                elif reply_text:
                    await message.reply(reply_text, parse_mode=None)
                else:
                    await message.reply("пост без текста и картинки — странная реклама")
            except Exception as e:
                logger.exception("Ошибка отправки рекламы: %s", e)
                if reply_text:
                    await message.reply(reply_text, parse_mode=None)
            finally:
                if photo_temp_path and os.path.isfile(photo_temp_path):
                    try:
                        os.unlink(photo_temp_path)
                    except OSError:
                        pass

        if MEME_TRIGGER.lower() in text:
            now  = time.time()
            last = _meme_cooldowns.get(user_id, 0)
            if now - last < MEME_COOLDOWN_SEC:
                await message.reply("⏱️ Жди 60 секунд, мемы не бесконечные")
                return
            _meme_cooldowns[user_id] = now
            status_msg = await message.reply("🎬 Качаю мем с TikTok, сек...")
            try:
                from aiogram.types import BufferedInputFile
                video_bytes, caption = await fetch_meme_melstroy()
                if not video_bytes:
                    await status_msg.edit_text(f"❌ Не получилось: {caption}")
                    return
                video_file = BufferedInputFile(video_bytes, filename="meme.mp4")
                await message.reply_video(
                    video=video_file,
                    caption=f"🎭 <b>{caption}</b>\n<i>@{TIKTOK_ACCOUNT}</i>",
                    parse_mode="HTML",
                    supports_streaming=True,
                )
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.exception("Ошибка отправки мема: %s", e)
                try:
                    await status_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")
                except Exception:
                    await message.reply(f"❌ Ошибка: {str(e)[:100]}")
            return

        if MEME_PIKK_TRIGGER.lower() in text:
            now  = time.time()
            last = _pikk_cooldowns.get(user_id, 0)
            if now - last < MEME_PIKK_COOLDOWN_SEC:
                await message.reply("⏱️ Жди 60 секунд, мемы не бесконечные")
                return
            _pikk_cooldowns[user_id] = now
            status_msg = await message.reply("🎬 Качаю мем пикшанель, сек...")
            try:
                from aiogram.types import BufferedInputFile
                video_bytes, caption = await fetch_meme_pikk()
                if not video_bytes:
                    await status_msg.edit_text(f"❌ Не получилось: {caption}")
                    return
                video_file = BufferedInputFile(video_bytes, filename="pikk.mp4")
                await message.reply_video(
                    video=video_file,
                    caption=f"📹 <b>{caption}</b>\n<i>@{TIKTOK_PIKK_ACCOUNT}</i>",
                    parse_mode="HTML",
                    supports_streaming=True,
                )
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.exception("Ошибка отправки мема пикшанель: %s", e)
                try:
                    await status_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")
                except Exception:
                    await message.reply(f"❌ Ошибка: {str(e)[:100]}")
            return

        if MUSIC_TRIGGER.lower() in text:
            now  = time.time()
            last = _music_cooldowns.get(user_id, 0)
            if now - last < MUSIC_COOLDOWN_SEC:
                await message.reply(f"⏱️ Жди ещё {int(MUSIC_COOLDOWN_SEC - (now - last))} сек.")
                return

            raw   = message.text or ""
            idx   = raw.lower().find(MUSIC_TRIGGER.lower())
            query = raw[idx + len(MUSIC_TRIGGER):].strip()
            if not query:
                await message.reply(
                    "Напиши что найти, например:\n<i>жужа музло playboi carti</i>",
                    parse_mode="HTML",
                )
                return
            if len(query) > 200:
                await message.reply("❌ Запрос слишком длинный (макс 200 символов)")
                return

            _music_cooldowns[user_id] = now
            status_msg = await message.reply(f"🎵 Ищу и качаю «{query}»...")
            try:
                from aiogram.types import BufferedInputFile
                import shutil
                ffmpeg_path   = shutil.which("ffmpeg")
                ffmpeg_status = f"ffmpeg={'✓ ' + ffmpeg_path if ffmpeg_path else '✗ не найден'}"
                logger.info("Music start: query=%r %s", query, ffmpeg_status)

                audio_bytes, title, artist, duration = await download_music_by_query(query)
                logger.info("Music result: bytes=%s title=%r",
                            len(audio_bytes) if audio_bytes else 0, title)

                if not audio_bytes:
                    await status_msg.edit_text(
                        f"😔 Не смогла найти/скачать «{query}»\n<i>{title}</i>\n<code>{ffmpeg_status}</code>",
                        parse_mode="HTML",
                    )
                    return
                audio_file = BufferedInputFile(audio_bytes, filename=f"{title or query}.mp3")
                await message.reply_audio(
                    audio=audio_file,
                    title=title or query,
                    performer=artist or "",
                    duration=duration or 0,
                    caption=f"🎧 <b>{title}</b>" + (f"\n👤 {artist}" if artist else ""),
                    parse_mode="HTML",
                )
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.exception("Ошибка скачивания музыки: %s", e)
                try:
                    await status_msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")
                except Exception:
                    await message.reply(f"❌ Ошибка: {str(e)[:200]}")
            return

        if CIRCLE_TRIGGER.lower() in text:
            now  = time.time()
            last = _circle_cooldowns.get(user_id, 0)
            if now - last < CIRCLE_COOLDOWN_SEC:
                await message.reply(f"⏱️ Жди ещё {int(CIRCLE_COOLDOWN_SEC - (now - last))} сек.")
                return

            raw     = message.text or ""
            idx     = raw.lower().find(CIRCLE_TRIGGER.lower())
            url_raw = raw[idx + len(CIRCLE_TRIGGER):].strip()

            # ── Определяем источник видео ─────────────────────────────────
            # 1. Видео прикреплено к этому же сообщению (caption + video)
            # 2. Это ответ на сообщение с видео/документом-видео/video_note
            # 3. URL в тексте

            video_file_id = None

            # Случай 1: видео прикреплено к сообщению с триггером (обрабатывается
            #   хендлером on_video_with_caption — см. ниже). Здесь этого не будет,
            #   т.к. on_text срабатывает только на F.text (без медиа).
            #   Поэтому смотрим только reply.

            # Случай 2: reply на видео
            reply_msg = message.reply_to_message
            if reply_msg:
                if reply_msg.video:
                    video_file_id = reply_msg.video.file_id
                elif reply_msg.document and reply_msg.document.mime_type and \
                        reply_msg.document.mime_type.startswith("video"):
                    video_file_id = reply_msg.document.file_id
                elif reply_msg.video_note:
                    video_file_id = reply_msg.video_note.file_id

            # Случай 3: URL в тексте
            video_url = None
            if not video_file_id and url_raw:
                candidate = url_raw.split()[0]
                if candidate.startswith(("http://", "https://")):
                    video_url = candidate

            if not video_file_id and not video_url:
                await message.reply(
                    "Как использовать:\n"
                    "• <b>Ответь</b> на видео в чате командой <i>жужа кружок</i>\n"
                    "• Или напиши ссылку: <i>жужа кружок https://youtu.be/...</i>\n\n"
                    "📌 YouTube, TikTok, VK и прямые mp4-ссылки.\n"
                    f"⏱ Видео обрезается до {CIRCLE_MAX_DURATION} сек.",
                    parse_mode="HTML",
                )
                return

            _circle_cooldowns[user_id] = now
            status_msg = await message.reply("🎥 Обрабатываю видео...")
            try:
                from aiogram.types import BufferedInputFile

                if video_file_id:
                    # Скачиваем файл из Telegram
                    tg_file = await bot.get_file(video_file_id)
                    tg_bytes = await bot.download_file(tg_file.file_path)
                    raw_bytes = tg_bytes.read() if hasattr(tg_bytes, "read") else bytes(tg_bytes)
                    logger.info("circle: получен файл из TG %dKB", len(raw_bytes) // 1024)
                    # Конвертируем через ffmpeg
                    video_bytes, warn = await _convert_to_circle_bytes(raw_bytes)
                else:
                    video_bytes, warn = await download_video_for_circle(video_url)

                if not video_bytes:
                    await status_msg.edit_text(f"❌ Не получилось: {warn}")
                    return

                MAX_BYTES = 49 * 1024 * 1024
                if len(video_bytes) > MAX_BYTES:
                    await status_msg.edit_text(
                        f"❌ Видео слишком большое ({len(video_bytes) // (1024*1024)} МБ > 49 МБ)"
                    )
                    return

                vf = BufferedInputFile(video_bytes, filename="circle.mp4")
                await message.reply_video_note(video_note=vf)
                if warn:
                    await message.reply(f"⚠️ {warn}")
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                logger.info("circle: отправлен кружок %dKB", len(video_bytes) // 1024)
            except Exception as e:
                logger.exception("Ошибка отправки кружка: %s", e)
                try:
                    await status_msg.edit_text(f"❌ Ошибка: {str(e)[:150]}")
                except Exception:
                    await message.reply(f"❌ Ошибка: {str(e)[:150]}")
            return

        if DRAW_TRIGGER.lower() in text:
            chat_id = message.chat.id

            raw        = message.text or ""
            idx        = raw.lower().find(DRAW_TRIGGER.lower())
            prompt_raw = raw[idx + len(DRAW_TRIGGER):].strip()

            if not prompt_raw:
                await message.reply(
                    "Напиши что нарисовать, например:\n<i>жужа нарисуй закат на море</i>",
                    parse_mode="HTML",
                )
                return
            if len(prompt_raw) > 300:
                await message.reply("❌ Промпт слишком длинный (макс 300 символов)")
                return

            if chat_id not in _draw_queues:
                _draw_queues[chat_id]     = asyncio.Queue()
                _draw_queue_busy[chat_id] = False

            queue = _draw_queues[chat_id]
            pos   = queue.qsize()

            if pos >= 5:
                await message.reply("🚫 Очередь переполнена (макс 5). Попробуй позже.")
                return

            if _draw_queue_busy[chat_id] or pos > 0:
                await message.reply(
                    f"⏳ Ты в очереди — позиция <b>{pos + 1}</b>. Скоро нарисую!",
                    parse_mode="HTML",
                )

            await queue.put((message, prompt_raw))

            if not _draw_queue_busy[chat_id]:
                asyncio.create_task(_draw_queue_worker(chat_id, bot))
            return

        if INFO_TRIGGER.lower() in text:
            now  = time.time()
            last = _info_cooldowns.get(user_id, 0)
            if now - last < INFO_COOLDOWN_SEC:
                await message.reply(f"⏱️ Жди ещё {int(INFO_COOLDOWN_SEC - (now - last))} сек.")
                return

            raw   = message.text or ""
            idx   = raw.lower().find(INFO_TRIGGER.lower())
            query = raw[idx + len(INFO_TRIGGER):].strip()
            if not query:
                await message.reply(
                    "Напиши что найти, например:\n<i>жужа найди информацию Никола Тесла</i>",
                    parse_mode="HTML",
                )
                return
            if len(query) > 200:
                await message.reply("❌ Запрос слишком длинный (макс 200 символов)")
                return

            _info_cooldowns[user_id] = now
            status_msg = await message.reply("🔍 Ищу информацию...")
            try:
                result = await fetch_web_info(query)
                if not result:
                    result = random.choice(_INFO_FUNNY_FALLBACKS)
                gif_url = random.choice(_INFO_GIF_POOL)
                try:
                    await message.reply_animation(animation=gif_url, caption=result)
                except Exception:
                    await message.reply(result)
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.exception("Ошибка поиска информации: %s", e)
                try:
                    await status_msg.edit_text(random.choice(_INFO_FUNNY_FALLBACKS))
                except Exception:
                    await message.reply(random.choice(_INFO_FUNNY_FALLBACKS))
            return

        if SEARCH_TRIGGER.lower() in text:
            now  = time.time()
            last = _search_cooldowns.get(user_id, 0)
            if now - last < SEARCH_COOLDOWN_SEC:
                await message.reply("⏱️ Подожди 30 секунд.")
                return

            raw   = message.text or ""
            idx   = raw.lower().find(SEARCH_TRIGGER.lower())
            query = raw[idx + len(SEARCH_TRIGGER):].strip()
            if not query:
                await message.reply(
                    "Напиши что найти, например:\n<i>жужа найди закат на море</i>",
                    parse_mode="HTML",
                )
                return
            if len(query) > 200:
                await message.reply("❌ Запрос слишком длинный (макс 200 символов)")
                return

            _search_cooldowns[user_id] = now
            status_msg = await message.reply("🔍 Ищу...")
            try:
                image_bytes, description = await search_image(query)
                if not image_bytes:
                    await status_msg.edit_text(f"😔 Ничего не нашла по запросу «{query}»")
                    return
                from aiogram.types import BufferedInputFile
                photo   = BufferedInputFile(image_bytes, filename="found.jpg")
                caption = (
                    f"<b>{description}</b>"
                    if description and description.lower() != query.lower()
                    else f"<i>{query}</i>"
                )
                await message.reply_photo(photo=photo, caption=caption, parse_mode="HTML")
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.exception("Ошибка поиска картинки: %s", e)
                try:
                    await status_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")
                except Exception:
                    await message.reply(f"❌ Ошибка: {str(e)[:100]}")

        user_obj = message.from_user
        uname    = (user_obj.username or "") if user_obj else ""
        chat_id  = message.chat.id

        # ── Болталка: управление только для владельца по user_id ─────────────
        if CHAT_ON_TRIGGER.lower() in text:
            if _is_admin(user_id):
                if not _chat_mode_enabled.get(chat_id):
                    _chat_mode_enabled[chat_id] = True
                    _chat_history[chat_id]      = []
                    _chat_state_save()
                    logger.info("Болталка ВКЛЮЧЕНА в чате %s", chat_id)
                    await message.reply("окей, говорю 👀")
            return

        if CHAT_OFF_TRIGGER.lower() in text:
            if _is_admin(user_id):
                _chat_mode_enabled[chat_id] = False
                _chat_state_save()
                logger.info("Болталка ВЫКЛЮЧЕНА в чате %s", chat_id)
                await message.reply("молчу 🤐")
            return

        if CHAT_TEST_TRIGGER.lower() in text and _chat_mode_enabled.get(chat_id):
            sender_name = (user_obj.first_name or uname or "кто-то") if user_obj else "кто-то"
            reply_text  = await juza_chat_reply(chat_id, sender_name, message.text or "")
            await message.reply(reply_text if reply_text else "тут, но что-то пошло не так 😔")
            return

        if "жужа" in text and _chat_mode_enabled.get(chat_id):
            sender_name = (user_obj.first_name or uname or "кто-то") if user_obj else "кто-то"
            reply_text  = await juza_chat_reply(chat_id, sender_name, message.text or "")
            if reply_text:
                await message.reply(reply_text)
            return

        if _chat_mode_enabled.get(chat_id) and random.random() < 0.10:
            sender_name = (user_obj.first_name or uname or "кто-то") if user_obj else "кто-то"
            reply_text  = await juza_chat_reply(chat_id, sender_name, message.text or "")
            if reply_text:
                await message.reply(reply_text)
            return

        # 20% шанс: жужа сама берёт текст сообщения как запрос и отвечает с GIF + инфой
        if random.random() < 0.20:
            spontaneous_query = (message.text or "").strip()
            if spontaneous_query and len(spontaneous_query) <= 200:
                try:
                    result = await fetch_web_info(spontaneous_query)
                    if result:
                        gif_url = random.choice(_INFO_GIF_POOL)
                        try:
                            await message.reply_animation(animation=gif_url, caption=result)
                        except Exception:
                            await message.reply(result)
                except Exception as e:
                    logger.warning("Спонтанная инфо-реакция: %s", e)

    @dp.callback_query()
    async def on_callback(callback: CallbackQuery):
        be = get_backend()
        await handle_callback(callback, bot, be)

    @dp.message(F.video | F.document)
    async def on_video_with_caption(message: Message):
        """Обрабатывает видео, отправленное вместе с триггером в подписи (caption)."""
        caption = (message.caption or "").strip().lower()
        if CIRCLE_TRIGGER.lower() not in caption:
            return

        user_id = message.from_user.id if message.from_user else 0
        now  = time.time()
        last = _circle_cooldowns.get(user_id, 0)
        if now - last < CIRCLE_COOLDOWN_SEC:
            await message.reply(f"⏱️ Жди ещё {int(CIRCLE_COOLDOWN_SEC - (now - last))} сек.")
            return

        # Проверяем что это видео (video или document с mime video/*)
        if message.video:
            file_id = message.video.file_id
        elif message.document and message.document.mime_type and \
                message.document.mime_type.startswith("video"):
            file_id = message.document.file_id
        else:
            return

        _circle_cooldowns[user_id] = now
        status_msg = await message.reply("🎥 Обрабатываю видео...")
        try:
            from aiogram.types import BufferedInputFile
            tg_file  = await bot.get_file(file_id)
            tg_bytes = await bot.download_file(tg_file.file_path)
            raw_bytes = tg_bytes.read() if hasattr(tg_bytes, "read") else bytes(tg_bytes)
            logger.info("circle(caption): получен файл %dKB", len(raw_bytes) // 1024)

            video_bytes, warn = await _convert_to_circle_bytes(raw_bytes)
            if not video_bytes:
                await status_msg.edit_text(f"❌ Не получилось: {warn}")
                return

            MAX_BYTES = 49 * 1024 * 1024
            if len(video_bytes) > MAX_BYTES:
                await status_msg.edit_text(
                    f"❌ Видео слишком большое ({len(video_bytes) // (1024*1024)} МБ > 49 МБ)"
                )
                return

            vf = BufferedInputFile(video_bytes, filename="circle.mp4")
            await message.reply_video_note(video_note=vf)
            if warn:
                await message.reply(f"⚠️ {warn}")
            try:
                await status_msg.delete()
            except Exception:
                pass
            logger.info("circle(caption): отправлен кружок %dKB", len(video_bytes) // 1024)
        except Exception as e:
            logger.exception("Ошибка кружка из caption: %s", e)
            try:
                await status_msg.edit_text(f"❌ Ошибка: {str(e)[:150]}")
            except Exception:
                await message.reply(f"❌ Ошибка: {str(e)[:150]}")

    logger.info("Бот запущен")
    logger.info(
        "Токены: GEMINI=%s | TOGETHER=%s | REPLICATE=%s | HF=%s | MUKESH=%s",
        "✓" if GEMINI_API_KEY      else "✗",
        "✓" if TOGETHER_API_TOKEN  else "✗",
        "✓" if REPLICATE_API_TOKEN else "✗",
        "✓" if HUGGINGFACE_TOKEN   else "✗",
        "✓" if MUKESH_API_KEY      else "✗",
    )
    await dp.start_polling(bot)


def start_bot_in_thread(backend):
    import threading

    def _run():
        asyncio.run(run_bot(backend))

    threading.Thread(target=_run, daemon=True).start()
    logger.info("Поток бота запущен")


if __name__ == "__main__":
    asyncio.run(run_bot(backend=None))