"""ช่องกระจายข้อความแชทสด: ข้อความใหม่ที่บันทึกลงฐานข้อมูล -> ส่งต่อให้หน้าเว็บที่เปิดอยู่ทันที (WebSocket)

ทำงานในโปรเซสเดียวกับ Telegram (ผู้ติดตามแต่ละคน = asyncio.Queue หนึ่งคิว) คิวเต็ม = ทิ้งอีเวนต์
หน้าเว็บจะดึงส่วนที่ขาดจาก /api/chats/{id}/messages เองเมื่อเชื่อมต่อใหม่
เรียก publish() จากเธรดอื่นได้ (ส่งเข้าลูปของผู้ติดตามอย่างปลอดภัย)
"""

import asyncio
import re
from datetime import timezone

from .config import DISPLAY_TZ

QUEUE_SIZE = 300
_subs: dict[asyncio.Queue, asyncio.AbstractEventLoop | None] = {}


def subscribe() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    _subs[q] = loop
    return q


def unsubscribe(q: asyncio.Queue) -> None:
    _subs.pop(q, None)


def subscribers() -> int:
    return len(_subs)


def _put(q: asyncio.Queue, event: dict) -> None:
    try:
        q.put_nowait(event)
    except asyncio.QueueFull:
        pass  # ทิ้ง: หน้าเว็บจะดึงส่วนที่ขาดเองตอนเชื่อมต่อใหม่


def publish(event: dict) -> int:
    """ส่งอีเวนต์ให้ทุกผู้ติดตาม คืนจำนวนผู้ติดตามที่ถูกส่งให้"""
    try:
        here = asyncio.get_running_loop()
    except RuntimeError:
        here = None
    sent = 0
    for q, loop in list(_subs.items()):
        if q.full():
            continue
        if loop is None or loop is here:
            _put(q, event)
        else:
            try:
                loop.call_soon_threadsafe(_put, q, event)
            except RuntimeError:  # ลูปของผู้ติดตามปิดไปแล้ว
                _subs.pop(q, None)
                continue
        sent += 1
    return sent


_HANDLE_RE = re.compile(r"\s*\(@[^)]+\)\s*$")
_me_name = ""  # ชื่อบัญชี Telegram ที่เชื่อมต่อ (ใช้เป็นชื่อสำรองของข้อความทีมงานที่ไม่รู้ว่าใครส่ง)


def set_me(name: str) -> None:
    global _me_name
    _me_name = (name or "").strip()


def short_name(name) -> str:
    """"Way (@autosupportway)" -> "Way" (ถ้าตัดแล้วว่างใช้ชื่อเดิม)"""
    name = (name or "").strip()
    return _HANDLE_RE.sub("", name) or name


def sent_by_label(by) -> str:
    """พนักงานที่กดส่ง: "admin" -> "admin" · "LINE:สมชาย" -> "สมชาย (LINE)"."""
    by = (by or "").strip()
    if by[:5].upper() == "LINE:":
        return f"{by[5:].strip()} (LINE)"
    return by


def msg_label(m) -> str:
    """ชื่อที่แสดงบนข้อความ: ฝั่งทีมงาน = พนักงานที่กดส่งจากเว็บ/LINE > ชื่อผู้ส่งใน Telegram > ชื่อบัญชีที่เชื่อมต่อ"""
    if m.is_outgoing:
        return sent_by_label(getattr(m, "sent_by", "")) or short_name(m.sender_name) or _me_name or "ทีมงาน"
    return short_name(m.sender_name) or "ลูกค้า"


def to_dict(m) -> dict:
    """แถว Message -> dict สำหรับหน้าเว็บ (เวลาแสดงตามเวลาไทย)"""
    local = m.date.replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ) if m.date else None
    return {
        "id": m.id,
        "tg": m.tg_message_id,
        "chat": m.chat_id,
        "name": m.sender_name or "",
        "out": bool(m.is_outgoing),
        "text": m.text or "",
        "media": m.media_path or "",
        "by": getattr(m, "sent_by", "") or "",
        "label": msg_label(m),
        "hm": local.strftime("%H:%M") if local else "",
        "at": local.strftime("%d/%m/%Y %H:%M") if local else "",
    }
