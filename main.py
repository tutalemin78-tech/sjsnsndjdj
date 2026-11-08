#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram-юзербот (Telethon, async): /круг -> видео-кружок (video note).

Работает только с ИСХОДЯЩИМИ сообщениями в «Избранном».
Сценарий: /круг -> конвертация ffmpeg -> выбор чата -> режим -> подтверждение
-> статус «записывает видеосообщение» -> отправка (одноразовая или обычная).

Запуск на хостинге: SESSION_STRING + API_ID + API_HASH в переменных окружения.
"""
import asyncio
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any, List, Optional

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.helpers import generate_random_long
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendMediaRequest, SetTypingRequest
from telethon.tl.types import (
    Channel,
    DocumentAttributeVideo,
    InputMediaUploadedDocument,
    InputMessagesFilterVideo,
    SendMessageCancelAction,
    SendMessageRecordRoundAction,
    SendMessageUploadRoundAction,
    User,
)

# --------------------------------------------------------------------------- #
# Настройки
# --------------------------------------------------------------------------- #
MAX_SECONDS = 60            # максимум длительности кружка
SIZE = 640                  # сторона квадрата
IDLE_TIMEOUT = 5 * 60       # таймаут бездействия, сек
MAX_RESULTS = 8             # максимум чатов в списке
TTL_ONCE = 2147483647       # ttl_seconds для одноразового сообщения
ACTION_INTERVAL = 4.0       # период обновления статуса, сек
UPLOAD_ACTION_MIN = 2.5     # минимум показа «отправляет видеосообщение», сек

CANCEL_WORDS = {"/отмена", "/cancel"}
START_RE = re.compile(r"^/(круг|krug)(\s|$)")
YES_WORDS = {"да", "д", "yes", "y", "+", "ок", "ok"}
NO_WORDS = {"нет", "н", "no", "n", "-"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("krug")


# --------------------------------------------------------------------------- #
# Состояния и данные
# --------------------------------------------------------------------------- #
class State(Enum):
    IDLE = auto()
    PREPARING = auto()   # скачивание + конвертация
    ASK_CHAT = auto()    # ждём имя чата
    PICK_CHAT = auto()   # ждём номер из списка
    ASK_MODE = auto()    # ждём режим 1/2
    CONFIRM = auto()     # ждём да/нет
    SENDING = auto()     # «запись» + отправка


WAITING = {State.ASK_CHAT, State.PICK_CHAT, State.ASK_MODE, State.CONFIRM}


@dataclass
class Target:
    title: str
    entity: Any
    icon: str
    kind: str
    can_once: bool


def make_target(entity: Any, title: Optional[str] = None) -> Target:
    """Описание получателя + можно ли ему слать одноразовый кружок."""
    if isinstance(entity, User):
        if entity.bot:
            icon, kind, can_once = "🤖", "бот", False
        else:
            is_self = bool(getattr(entity, "is_self", False))
            icon, kind = "👤", "личный чат"
            can_once = not is_self and not entity.deleted
    elif isinstance(entity, Channel) and entity.broadcast:
        icon, kind, can_once = "📢", "канал", False
    else:
        icon, kind, can_once = "👥", "группа", False

    if not title:
        title = (
            getattr(entity, "title", None)
            or " ".join(filter(None, [getattr(entity, "first_name", ""), getattr(entity, "last_name", "")]))
            or getattr(entity, "username", None)
            or str(getattr(entity, "id", "?"))
        )
    return Target(title=title, entity=entity, icon=icon, kind=kind, can_once=can_once)


# --------------------------------------------------------------------------- #
# ffmpeg
# --------------------------------------------------------------------------- #
async def probe_duration(path: Path) -> float:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, _ = await proc.communicate()
    except asyncio.CancelledError:
        proc.kill()
        raise
    try:
        return float(out.decode().strip())
    except ValueError:
        return 0.0


async def convert_to_circle(src: Path, dst: Path) -> None:
    """Квадрат 640x640 (crop по центру), h264 + aac, до 60 сек, mp4."""
    vf = (
        "crop='min(iw,ih)':'min(iw,ih)',"
        f"scale={SIZE}:{SIZE}:flags=lanczos,setsar=1,fps=30,format=yuv420p"
    )
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-i", str(src),
        "-t", str(MAX_SECONDS),
        "-map", "0:v:0", "-map", "0:a:0?",
        "-vf", vf,
        "-c:v", "libx264", "-profile:v", "main", "-preset", "veryfast",
        "-crf", "23", "-maxrate", "1500k", "-bufsize", "3000k",
        "-c:a", "aac", "-b:a", "64k", "-ar", "44100", "-ac", "1",
        "-movflags", "+faststart",
        str(dst),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
    )
    try:
        _, err = await proc.communicate()
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        raise
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg: " + err.decode(errors="replace")[-400:].strip())


# --------------------------------------------------------------------------- #
# Утилиты
# --------------------------------------------------------------------------- #
def is_video(msg) -> bool:
    if msg is None or not msg.media:
        return False
    if msg.video or msg.video_note or msg.gif:
        return True
    doc = msg.document
    return bool(doc and doc.mime_type and doc.mime_type.startswith("video/"))


HINTS = {
    "VOICE_MESSAGES_FORBIDDEN": "Собеседник запретил голосовые/видеосообщения.",
    "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN": "В этом чате запрещены видеосообщения (кружки).",
    "CHAT_WRITE_FORBIDDEN": "Нет прав писать в этот чат.",
    "CHAT_ADMIN_REQUIRED": "Нужны права администратора.",
    "MEDIA_TTL_INVALID": "Одноразовый режим здесь не поддерживается. Выберите обычный.",
    "USER_IS_BLOCKED": "Пользователь вас заблокировал.",
    "USER_PRIVACY_RESTRICTED": "Настройки приватности собеседника запрещают отправку.",
    "SLOWMODE_WAIT": "В чате включён медленный режим, попробуйте позже.",
    "PEER_FLOOD": "Telegram ограничил отправку (PEER_FLOOD).",
}


def explain(e: Exception) -> str:
    if isinstance(e, FloodWaitError):
        return f"FloodWait: подождите {e.seconds} сек."
    code = (getattr(e, "message", None) or "") + " " + str(e)
    for key, text in HINTS.items():
        if key in code:
            return f"{text} ({key})"
    return f"{type(e).__name__}: {e}"


def fmt_dur(sec: float) -> str:
    return f"{sec:.1f} сек"


# --------------------------------------------------------------------------- #
# Основной класс
# --------------------------------------------------------------------------- #
class KrugBot:
    def __init__(self, client: TelegramClient):
        self.client = client
        self.lock = asyncio.Lock()        # сериализует обработку команд
        self.say_lock = asyncio.Lock()    # защита от «эха» собственных сообщений
        self.bot_ids: set = set()
        self.timer: Optional[asyncio.Task] = None
        self.task: Optional[asyncio.Task] = None
        self._clear()

    # ---------------- вспомогательное ---------------- #
    def _clear(self) -> None:
        self.state = State.IDLE
        self.workdir: Optional[Path] = None
        self.video_path: Optional[Path] = None
        self.duration = 0.0
        self.truncated = False
        self.candidates: List[Target] = []
        self.target: Optional[Target] = None
        self.once = True
        self.timer = None
        self.task = None

    async def say(self, text: str):
        """Пишет в «Избранное» и запоминает id, чтобы не реагировать на себя."""
        async with self.say_lock:
            msg = await self.client.send_message("me", text, parse_mode=None, link_preview=False)
            if len(self.bot_ids) > 1000:
                self.bot_ids.clear()
            self.bot_ids.add(msg.id)
            return msg

    async def reset(self) -> None:
        """Полный сброс: отменяет фоновые задачи, удаляет временные файлы."""
        cur = asyncio.current_task()
        for t in (self.timer, self.task):
            if t is not None and t is not cur and not t.done():
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)
        workdir = self.workdir
        self._clear()
        if workdir:
            shutil.rmtree(workdir, ignore_errors=True)
            log.info("Временные файлы удалены: %s", workdir)

    def arm_timeout(self) -> None:
        if self.timer is not None and self.timer is not asyncio.current_task():
            self.timer.cancel()
        self.timer = asyncio.create_task(self._timeout_job())

    async def _timeout_job(self) -> None:
        await asyncio.sleep(IDLE_TIMEOUT)
        async with self.lock:
            if self.timer is asyncio.current_task() and self.state in WAITING:
                log.info("Таймаут сценария")
                await self.reset()
                await self.say("⌛ Время ожидания (5 минут) истекло. Сценарий отменён.")

    # ---------------- вход ---------------- #
    async def on_message(self, event) -> None:
        # Дожидаемся, пока say() запишет id (событие может прийти раньше ответа API)
        async with self.say_lock:
            pass
        if event.message.id in self.bot_ids:
            return
        async with self.lock:
            try:
                await self.handle(event)
            except Exception as e:  # не роняем бота
                log.exception("Ошибка обработки сообщения")
                await self.reset()
                await self.say(f"❌ Внутренняя ошибка: {explain(e)}")

    async def handle(self, event) -> None:
        text = (event.raw_text or "").strip()
        low = text.casefold()

        if low in CANCEL_WORDS:
            if self.state == State.IDLE:
                await self.say("Нечего отменять.")
            else:
                await self.reset()
                await self.say("Отменено ❌")
            return

        if START_RE.match(low):
            if self.state != State.IDLE:
                await self.say("Сценарий уже идёт. Продолжайте или отправьте /отмена.")
            else:
                await self.begin(event)
            return

        st = self.state
        if st == State.IDLE or not text:
            return
        if st in (State.PREPARING, State.SENDING):
            await self.say("⏳ Идёт обработка/отправка. /отмена — прервать.")
            return

        if st == State.ASK_CHAT:
            await self.on_query(text)
        elif st == State.PICK_CHAT:
            await self.on_pick(text)
        elif st == State.ASK_MODE:
            await self.on_mode(low)
        elif st == State.CONFIRM:
            await self.on_confirm(low)

        if self.state in WAITING:
            self.arm_timeout()

    # ---------------- шаг 1–2: поиск видео, скачивание, конвертация ---------------- #
    async def find_video(self, msg):
        if is_video(msg):
            return msg
        if msg.is_reply:
            replied = await msg.get_reply_message()
            return replied if is_video(replied) else None
        async for m in self.client.iter_messages("me", limit=50, filter=InputMessagesFilterVideo):
            return m
        return None

    async def begin(self, event) -> None:
        src = await self.find_video(event.message)
        if src is None:
            await self.say("Не нашёл видео. Ответьте /круг на видео или отправьте видео в «Избранное».")
            return
        self.workdir = Path(tempfile.mkdtemp(prefix="krug_"))
        self.state = State.PREPARING
        self.task = asyncio.create_task(self._prepare(src))

    async def _prepare(self, src) -> None:
        try:
            await self.say("⏬ Скачиваю видео…")
            downloaded = await self.client.download_media(src, file=str(self.workdir))
            if not downloaded:
                raise RuntimeError("не удалось скачать видео")
            src_path = Path(downloaded)

            src_dur = await probe_duration(src_path) or float(getattr(src.file, "duration", 0) or 0)

            await self.say(f"🎞 Конвертирую в кружок ({SIZE}×{SIZE}, H.264 + AAC)…")
            out = self.workdir / "circle.mp4"
            await convert_to_circle(src_path, out)

            out_dur = await probe_duration(out)
            if out_dur <= 0:
                raise RuntimeError("ffmpeg создал пустой файл")
            size_mb = out.stat().st_size / 1024 / 1024
            log.info("Готово: %.1f сек, %.2f МБ", out_dur, size_mb)

            self.truncated = src_dur > MAX_SECONDS + 0.5 or (src_dur == 0 and out_dur >= MAX_SECONDS - 0.3)
            self.duration = min(out_dur, float(MAX_SECONDS))
            self.video_path = out
            try:
                src_path.unlink()
            except OSError:
                pass

            if self.truncated:
                shown = f" ({src_dur:.0f} с)" if src_dur else ""
                await self.say(f"⚠️ Видео длиннее {MAX_SECONDS} сек{shown}: обрезано до {MAX_SECONDS} сек.")

            self.state = State.ASK_CHAT
            await self.say("В какой чат отправить? Напишите имя или часть названия.\n(/отмена — отмена)")
            self.arm_timeout()
        except asyncio.CancelledError:
            raise
        except FileNotFoundError:
            await self.reset()
            await self.say("❌ ffmpeg/ffprobe не найден в PATH.")
        except Exception as e:
            log.exception("Ошибка подготовки")
            await self.reset()
            await self.say(f"❌ Ошибка подготовки видео: {explain(e)}")

    # ---------------- шаг 3: выбор чата ---------------- #
    async def search_dialogs(self, query: str) -> List[Target]:
        q = query.casefold().strip()
        found: List[Target] = []
        async for d in self.client.iter_dialogs():
            name = d.name or ""
            uname = (getattr(d.entity, "username", None) or "").casefold()
            if q in name.casefold() or (q.lstrip("@") and q.lstrip("@") in uname):
                found.append(make_target(d.entity, name or None))
        found.sort(key=lambda t: not t.title.casefold().startswith(q))  # сортировка стабильна
        found = found[:MAX_RESULTS]

        if not found and (query.startswith("@") or query.startswith("+")):
            try:
                ent = await self.client.get_entity(query)
                found = [make_target(ent)]
            except Exception as e:
                log.info("get_entity(%s) не удалось: %s", query, e)
        return found

    async def on_query(self, query: str) -> None:
        results = await self.search_dialogs(query)
        if not results:
            await self.say("Ничего не найдено. Попробуйте другое имя (или @username).")
            return
        self.candidates = results
        self.state = State.PICK_CHAT
        lines = [f"{i}. {t.icon} {t.title} ({t.kind})" for i, t in enumerate(results, 1)]
        await self.say(
            "Найдено:\n" + "\n".join(lines)
            + f"\n\nОтветьте номером (1–{len(results)}) или напишите другое имя для нового поиска."
        )

    async def on_pick(self, text: str) -> None:
        if not text.isdigit():
            await self.on_query(text)
            return
        idx = int(text)
        if not 1 <= idx <= len(self.candidates):
            await self.say(f"Нет такого номера. Введите число от 1 до {len(self.candidates)}.")
            return
        self.target = self.candidates[idx - 1]
        self.state = State.ASK_MODE
        note = ""
        if not self.target.can_once:
            note = f"\n⚠️ Для чата «{self.target.title}» ({self.target.kind}) одноразовый режим недоступен, будет обычный."
        await self.say(
            f"Чат: {self.target.icon} {self.target.title}\n\n"
            "Режим отправки:\n1 — одноразовый (по умолчанию, «-» = по умолчанию)\n2 — обычный"
            + note
        )

    # ---------------- шаг 4: режим ---------------- #
    async def on_mode(self, low: str) -> None:
        if low in {"1", "-", "одноразовый", "once"}:
            once = True
        elif low in {"2", "обычный", "normal"}:
            once = False
        else:
            await self.say("Ответьте 1 (одноразовый) или 2 (обычный).")
            return

        extra = ""
        if once and not self.target.can_once:
            once = False
            extra = "\n⚠️ Одноразовый режим доступен только в личных чатах с людьми, выбран обычный."
        self.once = once
        self.state = State.CONFIRM

        trunc = f" (обрезано до {MAX_SECONDS} сек)" if self.truncated else ""
        await self.say(
            "📋 Сводка:\n"
            f"• Чат: {self.target.icon} {self.target.title}\n"
            f"• Режим: {'1 — одноразовый' if once else '2 — обычный'}\n"
            f"• Длительность: {fmt_dur(self.duration)}{trunc}\n\n"
            "Отправить? (да / нет)" + extra
        )

    # ---------------- шаг 5: подтверждение ---------------- #
    async def on_confirm(self, low: str) -> None:
        if low in NO_WORDS:
            await self.reset()
            await self.say("Отменено ❌")
        elif low in YES_WORDS:
            if self.timer and self.timer is not asyncio.current_task():
                self.timer.cancel()
            self.timer = None
            self.state = State.SENDING
            self.task = asyncio.create_task(self._send())
        else:
            await self.say("Ответьте «да» или «нет».")

    # ---------------- шаг 6–7: статус и отправка ---------------- #
    async def _action_loop(self, peer, factory, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                await self.client(SetTypingRequest(peer=peer, action=factory()))
                try:
                    await asyncio.wait_for(stop.wait(), timeout=ACTION_INTERVAL)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as e:  # в каналах и т.п. статус недоступен — это не критично
            log.warning("Не удалось выставить статус: %s", e)

    @asynccontextmanager
    async def show_action(self, peer, factory):
        stop = asyncio.Event()
        task = asyncio.create_task(self._action_loop(peer, factory, stop))
        try:
            yield
        finally:
            stop.set()
            await asyncio.gather(task, return_exceptions=True)

    async def _cancel_action(self, peer) -> None:
        if peer is None:
            return
        try:
            await self.client(SetTypingRequest(peer=peer, action=SendMessageCancelAction()))
        except Exception:
            pass

    async def _send(self) -> None:
        target, once, path, dur = self.target, self.once, self.video_path, self.duration
        peer = None
        try:
            peer = await self.client.get_input_entity(target.entity)
            log.info("Отправка в «%s», once=%s, %.1f сек", target.title, once, dur)

            # 1) «записывает видеосообщение» на время, равное длительности кружка
            async with self.show_action(peer, SendMessageRecordRoundAction):
                await asyncio.sleep(dur)

            # 2) «отправляет видеосообщение» (минимум ~2.5 сек) на время загрузки
            async with self.show_action(peer, lambda: SendMessageUploadRoundAction(progress=0)):
                t0 = time.monotonic()
                uploaded = await self.client.upload_file(str(path), file_name="circle.mp4")
                rest = UPLOAD_ACTION_MIN - (time.monotonic() - t0)
                if rest > 0:
                    await asyncio.sleep(rest)

            # 3) отправка video note через raw API (нужен для ttl_seconds)
            media = InputMediaUploadedDocument(
                file=uploaded,
                mime_type="video/mp4",
                attributes=[
                    DocumentAttributeVideo(
                        duration=dur, w=SIZE, h=SIZE,
                        round_message=True, supports_streaming=True,
                    )
                ],
                ttl_seconds=TTL_ONCE if once else None,
            )
            await self.client(SendMediaRequest(
                peer=peer, media=media, message="", random_id=generate_random_long(),
            ))
        except asyncio.CancelledError:
            await self._cancel_action(peer)
            raise
        except Exception as e:
            log.exception("Ошибка отправки")
            await self._cancel_action(peer)
            await self.reset()
            await self.say(f"❌ Ошибка: {explain(e)}")
        else:
            await self.reset()   # удаляем временные файлы
            log.info("Отправлено")
            await self.say("Отправлено ✅")


# --------------------------------------------------------------------------- #
# Мини-сервер для Render (Web Service требует открытый порт)
# --------------------------------------------------------------------------- #
async def start_health_server(port: int) -> asyncio.AbstractServer:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await asyncio.wait_for(reader.read(1024), timeout=5)
            body = b"ok"
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
            )
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handler, "0.0.0.0", port)
    log.info("Health-сервер слушает порт %s", port)
    return server


# --------------------------------------------------------------------------- #
# Запуск
# --------------------------------------------------------------------------- #
async def main() -> None:
    load_dotenv()
    api_id = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    phone = os.getenv("PHONE", "").strip()
    session_string = os.getenv("SESSION_STRING", "").strip()
    session_name = os.getenv("SESSION_NAME", "krug_userbot").strip() or "krug_userbot"

    if not api_id.isdigit() or not api_hash:
        sys.exit("Укажите API_ID (число) и API_HASH в переменных окружения / .env")
    for tool in ("ffmpeg", "ffprobe"):
        if shutil.which(tool) is None:
            sys.exit(f"{tool} не найден в PATH. Установите ffmpeg.")

    port = os.getenv("PORT", "").strip()
    health = await start_health_server(int(port)) if port.isdigit() else None

    if session_string:
        client = TelegramClient(StringSession(session_string), int(api_id), api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            sys.exit("SESSION_STRING недействительна. Создайте новую через login.py")
    else:
        # локальный запуск: файл сессии, номер спросит Telethon
        client = TelegramClient(session_name, int(api_id), api_hash)
        await client.start(**({"phone": phone} if phone else {}))

    bot = KrugBot(client)
    client.add_event_handler(bot.on_message, events.NewMessage(outgoing=True, chats="me"))

    me = await client.get_me()
    log.info("Вход выполнен: %s (id=%s). Жду команду /круг в «Избранном».", me.first_name, me.id)
    try:
        await client.run_until_disconnected()
    finally:
        await bot.reset()
        if health:
            health.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
