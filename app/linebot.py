"""LINE Chat Bot สำหรับลูกค้า — ใช้ LINE OA คนละตัวกับ "แจ้งเตือนอนุมัติ" (line_service.py)

ลูกค้าทักมาทาง LINE OA ตัวนี้ -> เข้ากระบวนการเดียวกับแชท Telegram: บันทึกข้อความ, AI วิเคราะห์/ร่างคำตอบ,
ทีมงานอนุมัติ (หรือเลือกให้ตอบอัตโนมัติ) แล้วส่งกลับผ่าน LINE

ต้องตั้ง env (คนละชุดกับ LINE แจ้งเตือน):
  LINE_BOT_CHANNEL_ACCESS_TOKEN  ส่งข้อความ / ดึงโปรไฟล์ / ดึงรูป
  LINE_BOT_CHANNEL_SECRET        ตรวจลายเซ็น webhook
Webhook URL: <เว็บของระบบนี้>/linebot/webhook
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time

import httpx
from sqlalchemy import func, select

from . import chatbus
from .config import MEDIA_DIR
from .database import Chat, Message, SessionLocal, get_settings, utcnow

log = logging.getLogger("app.linebot")
API = "https://api.line.me/v2/bot"
API_DATA = "https://api-data.line.me/v2/bot"
ID_BASE = 9_000_000_000_000   # chat id ของ LINE = -(ID_BASE + n) ไม่ชนกับ id ของ Telegram
MAX_TEXT = 4800               # LINE: ข้อความละไม่เกิน 5,000 ตัวอักษร
MAX_PER_REQUEST = 5           # LINE: ส่งได้ครั้งละไม่เกิน 5 ข้อความ
REPLY_TOKEN_TTL = 25          # วินาที: replyToken ใช้ได้ในเวลาสั้นๆ (ใช้แล้วไม่นับโควตา push)
MAX_IMAGE_BYTES = 8 * 1024 * 1024
STICKER_TEXT = "(สติกเกอร์)"
PLACEHOLDERS = {"video": "(วิดีโอ)", "audio": "(เสียง)", "file": "(ไฟล์)", "location": "(ตำแหน่งที่ตั้ง)"}

on_message = None            # callback(chat_id) -> analyzer.schedule (ตั้งจาก main)
_reply_tokens: dict[str, tuple[str, float]] = {}  # ext_id -> (replyToken, เวลาที่ได้รับ)
_names: dict[str, str] = {}                        # cache ชื่อผู้ใช้/กลุ่ม
_tasks: set[asyncio.Task] = set()
_chat_lock = asyncio.Lock()


class LineBotError(Exception):
    pass


# ---------------------------------------------------------------- ตั้งค่า
def token() -> str:
    return (os.getenv("LINE_BOT_CHANNEL_ACCESS_TOKEN") or "").strip()


def secret() -> str:
    return (os.getenv("LINE_BOT_CHANNEL_SECRET") or "").strip()


def configured() -> bool:
    return bool(token() and secret())


def verify(body: bytes, signature: str) -> bool:
    """ตรวจ X-Line-Signature ว่ามาจาก LINE จริง (HMAC-SHA256 ของ body ด้วย Channel secret ของ OA ตัวนี้)"""
    s = secret()
    if not s or not signature:
        return False
    mac = hmac.new(s.encode("utf-8"), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(mac).decode(), signature)


# ---------------------------------------------------------------- HTTP (แยกไว้ให้ทดสอบแทนที่ได้)
async def _request(method: str, url: str, **kw) -> httpx.Response:
    headers = {"Authorization": f"Bearer {token()}", **kw.pop("headers", {})}
    async with httpx.AsyncClient(timeout=15) as client:
        return await client.request(method, url, headers=headers, **kw)


async def _json(method: str, url: str, **kw) -> dict:
    try:
        r = await _request(method, url, **kw)
    except httpx.HTTPError as e:
        raise LineBotError(f"เชื่อมต่อ LINE ไม่ได้: {e}") from e
    if r.status_code >= 300:
        raise LineBotError(f"LINE ตอบกลับผิดพลาด ({r.status_code}): {r.text[:200]}")
    try:
        return r.json() if r.content else {}
    except ValueError:
        return {}


async def bot_info() -> dict:
    """ข้อมูลบอทของ OA (ใช้ตรวจว่า token ถูกต้อง): displayName, basicId"""
    return await _json("GET", f"{API}/info")


# ---------------------------------------------------------------- แชท/ข้อความ
def ext_of(source: dict) -> tuple[str, str]:
    """source ของ event -> (ext_id, kind) kind = user / group / room"""
    if source.get("groupId"):
        return source["groupId"], "group"
    if source.get("roomId"):
        return source["roomId"], "room"
    return source.get("userId", ""), "user"


async def _display_name(source: dict, user_id: str) -> str:
    """ชื่อที่แสดงใน LINE ของผู้ส่ง (ดึงไม่ได้ = ว่าง)"""
    if not user_id:
        return ""
    key = ("u:" if not (source.get("groupId") or source.get("roomId")) else "m:") + user_id
    if key in _names:
        return _names[key]
    try:
        if source.get("groupId"):
            data = await _json("GET", f"{API}/group/{source['groupId']}/member/{user_id}")
        elif source.get("roomId"):
            data = await _json("GET", f"{API}/room/{source['roomId']}/member/{user_id}")
        else:
            data = await _json("GET", f"{API}/profile/{user_id}")
    except LineBotError:
        return ""
    name = (data.get("displayName") or "").strip()[:60]
    if name:
        _names[key] = name
    return name


async def _chat_title(source: dict, kind: str, sender_name: str) -> str:
    if kind == "user":
        return sender_name or "ลูกค้า LINE"
    if kind == "group":
        try:
            data = await _json("GET", f"{API}/group/{source['groupId']}/summary")
            return (data.get("groupName") or "").strip()[:120] or "กลุ่ม LINE"
        except LineBotError:
            return "กลุ่ม LINE"
    return "ห้องแชท LINE"


def get_chat(ext_id: str) -> Chat | None:
    with SessionLocal() as db:
        return db.scalar(select(Chat).where(Chat.channel == "line", Chat.ext_id == ext_id))


async def ensure_chat(source: dict, sender_name: str = "") -> int | None:
    """หาแชท LINE ของ source นี้ ยังไม่มีให้สร้าง (ติดตามอัตโนมัติ) · คืนค่า chat id"""
    ext_id, kind = ext_of(source)
    if not ext_id:
        return None
    chat = get_chat(ext_id)
    if chat:
        # ชื่อแชทส่วนตัวใช้ชื่อโปรไฟล์ ถ้าเพิ่งได้ชื่อจริงภายหลังให้อัปเดต
        if kind == "user" and sender_name and chat.title in ("", "ลูกค้า LINE") and chat.title != sender_name:
            with SessionLocal() as db:
                row = db.get(Chat, chat.id)
                row.title = sender_name
                db.commit()
        return chat.id
    title = await _chat_title(source, kind, sender_name)
    async with _chat_lock:
        chat = get_chat(ext_id)  # อีเวนต์ซ้อนกันอาจสร้างไปก่อน
        if chat:
            return chat.id
        with SessionLocal() as db:
            low = db.scalar(select(func.min(Chat.id)).where(Chat.channel == "line"))
            new_id = (low - 1) if low is not None else -(ID_BASE + 1)
            db.add(Chat(id=new_id, title=title, kind="user" if kind == "user" else "group", monitored=True,
                        channel="line", ext_id=ext_id))
            db.commit()
        return new_id


def _next_seq(db, chat_id: int) -> int:
    return (db.scalar(select(func.max(Message.tg_message_id)).where(Message.chat_id == chat_id)) or 0) + 1


def _store(chat_id: int, *, ext_id: str, sender_name: str, text: str, media_path: str = "", out: bool = False,
           sent_by: str = "") -> dict | None:
    """บันทึกข้อความลงฐานข้อมูล + กระจายสดให้หน้าเว็บ · None = เคยบันทึกแล้ว (LINE ส่ง webhook ซ้ำได้)"""
    with SessionLocal() as db:
        if ext_id and db.scalar(select(Message.id).where(Message.chat_id == chat_id, Message.ext_id == ext_id)):
            return None
        row = Message(chat_id=chat_id, tg_message_id=_next_seq(db, chat_id), ext_id=ext_id, sender_name=sender_name,
                      is_outgoing=out, sent_by=sent_by, text=text, media_path=media_path, date=utcnow(), analyzed=out)
        db.add(row)
        db.commit()
        data = chatbus.to_dict(row)
    chatbus.publish({"type": "message", "chat": chat_id, "message": data})
    return data


async def _download_image(message_id: str) -> str:
    """ดึงรูปที่ลูกค้าส่งมาเก็บในโฟลเดอร์รูป · คืนชื่อไฟล์ (ว่าง = ดึงไม่ได้)"""
    try:
        r = await _request("GET", f"{API_DATA}/message/{message_id}/content")
    except httpx.HTTPError:
        log.warning("download LINE image %s failed", message_id)
        return ""
    if r.status_code >= 300 or not r.content or len(r.content) > MAX_IMAGE_BYTES:
        return ""
    ctype = (r.headers.get("content-type") or "").lower()
    ext = ".png" if "png" in ctype else ".gif" if "gif" in ctype else ".webp" if "webp" in ctype else ".jpg"
    name = f"line_{re.sub(r'[^A-Za-z0-9]', '', message_id)[:40]}{ext}"
    try:
        (MEDIA_DIR / name).write_bytes(r.content)
    except OSError:
        log.exception("save LINE image failed")
        return ""
    return name


async def _handle_message(event: dict, settings: dict) -> None:
    source = event.get("source") or {}
    msg = event.get("message") or {}
    sender = await _display_name(source, source.get("userId", ""))
    chat_id = await ensure_chat(source, sender)
    if chat_id is None:
        return
    ext_id, _ = ext_of(source)
    if event.get("replyToken"):
        _reply_tokens[ext_id] = (event["replyToken"], time.monotonic())
    mtype = msg.get("type", "")
    text, media = "", ""
    if mtype == "text":
        text = (msg.get("text") or "").strip()
    elif mtype == "image":
        media = await _download_image(str(msg.get("id", "")))
        text = "" if media else "(รูปภาพ)"
    elif mtype == "sticker":
        text = STICKER_TEXT
    else:
        text = PLACEHOLDERS.get(mtype, f"({mtype or 'ข้อความ'})")
    data = _store(chat_id, ext_id=str(msg.get("id", "")) or f"{chat_id}-{time.time_ns()}",
                  sender_name=sender or "ลูกค้า LINE", text=text, media_path=media)
    if data is not None and on_message and settings.get("linebot_enabled") == "1":
        on_message(chat_id)


async def _handle_follow(event: dict, settings: dict) -> None:
    source = event.get("source") or {}
    sender = await _display_name(source, source.get("userId", ""))
    chat_id = await ensure_chat(source, sender)
    greeting = (settings.get("linebot_greeting") or "").strip()
    if chat_id is None or not greeting:
        return
    ext_id, _ = ext_of(source)
    try:
        await send_text(ext_id, greeting, reply_token=event.get("replyToken", ""))
    except LineBotError:
        log.warning("send LINE greeting failed")
        return
    _store(chat_id, ext_id=f"greet-{time.time_ns()}", sender_name="", text=greeting, out=True, sent_by="บอทต้อนรับ")


async def handle_events(events: list[dict]) -> None:
    with SessionLocal() as db:
        settings = get_settings(db)
    if settings.get("linebot_enabled") != "1":
        return
    for ev in events:
        try:
            etype = ev.get("type")
            if etype == "message":
                await _handle_message(ev, settings)
            elif etype in ("follow", "join"):
                await _handle_follow(ev, settings)
        except Exception:  # noqa: BLE001 - event เดียวพัง ไม่ให้ล้มทั้งชุด
            log.exception("handle LINE bot event failed")


def spawn(events: list[dict]) -> asyncio.Task:
    """ประมวลผลหลังตอบ LINE 200 แล้ว (ดึงโปรไฟล์/รูปใช้เวลา)"""
    task = asyncio.create_task(handle_events(events))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return task


# ---------------------------------------------------------------- ส่งข้อความกลับ
def _chunks(text: str) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    parts, rest = [], text
    while len(rest) > MAX_TEXT:
        cut = rest.rfind("\n", 0, MAX_TEXT)
        cut = cut if cut > MAX_TEXT // 2 else MAX_TEXT
        parts.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    parts.append(rest)
    return parts


async def _push_messages(ext_id: str, messages: list[dict], reply_token: str = "") -> None:
    """ส่งทีละไม่เกิน 5 ข้อความ · มี replyToken ที่ยังสดอยู่ใช้ก่อน (ไม่นับโควตา push) ไม่ได้ก็ push"""
    first = True
    for i in range(0, len(messages), MAX_PER_REQUEST):
        batch = messages[i:i + MAX_PER_REQUEST]
        token_info = _reply_tokens.pop(ext_id, None) if first else None
        rtoken = reply_token or (token_info[0] if token_info and time.monotonic() - token_info[1] < REPLY_TOKEN_TTL else "")
        first = False
        if rtoken:
            try:
                await _json("POST", f"{API}/message/reply", json={"replyToken": rtoken, "messages": batch})
                continue
            except LineBotError:
                pass  # token หมดอายุ/ถูกใช้แล้ว -> push แทน
        await _json("POST", f"{API}/message/push", json={"to": ext_id, "messages": batch})


async def send_text(ext_id: str, text: str, reply_token: str = "") -> None:
    msgs = [{"type": "text", "text": t} for t in _chunks(text)]
    if not msgs:
        raise LineBotError("ข้อความว่าง")
    await _push_messages(ext_id, msgs, reply_token)


def _image_messages(names: list[str]) -> list[dict]:
    from . import line_service
    out = []
    for name in names:
        url = line_service._image_url(name)  # ลิงก์ HTTPS ที่เซ็นลายเซ็น (ต้องมีโดเมนสาธารณะ)
        if url:
            out.append({"type": "image", "originalContentUrl": url, "previewImageUrl": url})
    return out


async def send_reply(chat_id: int, text: str, media: list[str] | None = None, by: str = "") -> int:
    """ส่งคำตอบ (ข้อความ + รูปจากคู่มือ) ถึงลูกค้าใน LINE แล้วบันทึกเป็นข้อความฝั่งทีมงาน · คืนจำนวนข้อความที่ส่ง"""
    chat = get_chat_by_id(chat_id)
    if not chat or not chat.ext_id:
        raise LineBotError("ไม่พบปลายทาง LINE ของแชทนี้")
    if not token():
        raise LineBotError("ยังไม่ได้ตั้งค่า LINE_BOT_CHANNEL_ACCESS_TOKEN")
    msgs = [{"type": "text", "text": t} for t in _chunks(text)] + _image_messages(media or [])
    if not msgs:
        raise LineBotError("ข้อความว่าง")
    await _push_messages(chat.ext_id, msgs)
    now = time.time_ns()
    if text.strip():
        _store(chat_id, ext_id=f"out-{now}", sender_name=_bot_name(), text=text.strip(), out=True, sent_by=by)
    for i, name in enumerate(media or []):
        _store(chat_id, ext_id=f"out-{now}-{i}", sender_name=_bot_name(), text="", media_path=name, out=True, sent_by=by)
    return len(msgs)


async def send_manual(chat_id: int, text: str, by: str = "") -> dict:
    """ทีมงานพิมพ์ตอบจากหน้าเว็บ -> ส่งเข้า LINE ทันที · คืน dict ข้อความ"""
    chat = get_chat_by_id(chat_id)
    if not chat or not chat.ext_id:
        raise LineBotError("ไม่พบปลายทาง LINE ของแชทนี้")
    if not token():
        raise LineBotError("ยังไม่ได้ตั้งค่า LINE_BOT_CHANNEL_ACCESS_TOKEN")
    await send_text(chat.ext_id, text)
    data = _store(chat_id, ext_id=f"out-{time.time_ns()}", sender_name=_bot_name(), text=text.strip(), out=True, sent_by=by)
    return data or {}


def get_chat_by_id(chat_id: int) -> Chat | None:
    with SessionLocal() as db:
        return db.get(Chat, chat_id)


_bot = {"name": ""}


def _bot_name() -> str:
    return _bot["name"]


def set_bot_name(name: str) -> None:
    _bot["name"] = (name or "").strip()[:60]
