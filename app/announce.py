"""ระบบประกาศ: ส่งข้อความ (และรูป) ถึงหลายแชท/กลุ่มในครั้งเดียว

ส่งทีละแชทเว้นช่วง (ANNOUNCE_DELAY วินาที ค่าเริ่มต้น 3) กัน Telegram จำกัดความถี่การส่งข้ามกลุ่ม · เก็บผลรายแชท
ยกเลิกกลางคันได้ · ส่งซ้ำเฉพาะที่ล้มเหลว/ที่ค้างได้ · ระบบรีสตาร์ทกลางคันจะหยุดไว้ให้กดส่งต่อ (ไม่ส่งซ้ำเองเด็ดขาด)
"""
import asyncio
import json
import logging
import os

from sqlalchemy import func, select, update

from . import linebot
from .config import MEDIA_DIR
from .database import Announcement, AnnouncementTarget, Chat, SessionLocal, utcnow
from .telegram_service import TelegramLoginError, telegram

log = logging.getLogger(__name__)

MAX_TARGETS = 300     # แชทสูงสุดต่อประกาศ
MAX_TEXT = 3500       # Telegram รับข้อความไม่เกิน 4096 · เผื่อไว้
CAPTION_MAX = 1000    # คำอธิบายใต้รูปของ Telegram ไม่เกิน 1,024 ตัวอักษร · ยาวกว่านี้ส่งข้อความแยกก่อนรูป
STATUS_LABELS = {"sending": "กำลังส่ง", "done": "ส่งครบแล้ว", "cancelled": "ยกเลิกแล้ว", "interrupted": "หยุดกลางคัน"}
TARGET_LABELS = {"pending": "รอส่ง", "sent": "ส่งแล้ว", "failed": "ส่งไม่สำเร็จ", "skipped": "ข้าม (ยกเลิก)"}

_tasks: dict[int, asyncio.Task] = {}


def delay() -> float:
    try:
        return max(0.0, float(os.getenv("ANNOUNCE_DELAY") or 3))
    except ValueError:
        return 3.0


def create(text: str, chat_ids: list[int], images: list[str], by: str) -> int:
    """สร้างประกาศ + รายการปลายทาง (ตัดแชทซ้ำ / แชทที่ไม่มีในระบบ) แล้วคืน id · ยังไม่เริ่มส่ง"""
    with SessionLocal() as db:
        chats = [c for c in (db.get(Chat, cid) for cid in dict.fromkeys(chat_ids)) if c is not None]
        chats.sort(key=lambda c: ((c.channel or "telegram") != "telegram", (c.title or "").lower()))
        ann = Announcement(text=text, images=json.dumps(images) if images else "", created_by=by[:64], status="sending")
        db.add(ann)
        db.flush()
        for c in chats:
            db.add(AnnouncementTarget(announcement_id=ann.id, chat_id=c.id, chat_title=(c.title or str(c.id))[:256],
                                      channel=c.channel or "telegram"))
        db.commit()
        return ann.id


async def send_one(chat_id: int, text: str, images: list[str], by: str) -> None:
    """ส่งประกาศถึงแชทเดียว (Telegram / LINE) และบันทึกเป็นข้อความทีมงานในแชทนั้น"""
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
        channel = (chat.channel if chat else "") or "telegram"
    if chat is None:
        raise RuntimeError("ไม่พบแชทนี้ในระบบแล้ว")
    if channel == "line":
        from . import line_service
        if images and not all(line_service._image_url(n) for n in images):
            raise linebot.LineBotError("ส่งรูปทาง LINE ไม่ได้: ยังไม่มีที่อยู่เว็บสาธารณะ (PUBLIC_URL)")
        if images:
            await linebot.send_reply(chat_id, text, images, by=by)
        else:
            await linebot.send_manual(chat_id, text, by=by)
    elif images:
        caption = text if len(text) <= CAPTION_MAX else ""
        if text and not caption:  # ข้อความยาวเกินคำอธิบายใต้รูปของ Telegram -> ส่งข้อความก่อน แล้วตามด้วยรูป
            await telegram.send_manual(chat_id, text, None, by=by)
        await telegram.send_manual_images(chat_id, [(str(MEDIA_DIR / n), n) for n in images], caption, None, by=by)
    else:
        await telegram.send_manual(chat_id, text, None, by=by)


def _status(aid: int) -> str:
    with SessionLocal() as db:
        ann = db.get(Announcement, aid)
        return ann.status if ann else ""


async def _run(aid: int) -> None:
    try:
        with SessionLocal() as db:
            ann = db.get(Announcement, aid)
            text, by = ann.text, ann.created_by
            images = json.loads(ann.images or "[]")
        first = True
        while True:
            if _status(aid) != "sending":
                break  # ยกเลิกแล้ว
            with SessionLocal() as db:
                row = db.scalar(select(AnnouncementTarget).where(
                    AnnouncementTarget.announcement_id == aid, AnnouncementTarget.status == "pending")
                    .order_by(AnnouncementTarget.id).limit(1))
                if row is None:
                    break
                tid, chat_id = row.id, row.chat_id
            if not first:
                await asyncio.sleep(delay())
                if _status(aid) != "sending":
                    break  # ยกเลิกระหว่างรอคิว
            first = False
            err = ""
            try:
                await send_one(chat_id, text, images, by)
            except (TelegramLoginError, linebot.LineBotError) as e:
                err = str(e)
            except Exception as e:  # noqa: BLE001 - แชทเดียวพังต้องไม่หยุดทั้งประกาศ
                log.exception("announce %s -> chat %s failed", aid, chat_id)
                err = f"{type(e).__name__}: {e}"
            with SessionLocal() as db:
                t = db.get(AnnouncementTarget, tid)
                t.status, t.error, t.sent_at = ("failed" if err else "sent"), err[:500], utcnow()
                db.commit()
        with SessionLocal() as db:
            ann = db.get(Announcement, aid)
            if ann and ann.status == "sending":
                ann.status, ann.finished_at = "done", utcnow()
                db.commit()
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        log.exception("announce %s crashed", aid)
        with SessionLocal() as db:
            db.execute(update(Announcement).where(Announcement.id == aid, Announcement.status == "sending").values(status="interrupted"))
            db.commit()
    finally:
        _tasks.pop(aid, None)


def start(aid: int) -> None:
    """เริ่ม (หรือเริ่มต่อ) ส่งประกาศเบื้องหลัง · กำลังส่งอยู่แล้วจะไม่เริ่มซ้ำ"""
    t = _tasks.get(aid)
    if t is not None and not t.done():
        return
    _tasks[aid] = asyncio.get_running_loop().create_task(_run(aid))


def cancel(aid: int) -> bool:
    """ยกเลิก: รายการที่ยังไม่ถึงคิวถูกข้าม · ที่ส่งไปแล้วยังคงอยู่"""
    with SessionLocal() as db:
        ann = db.get(Announcement, aid)
        if ann is None or ann.status != "sending":
            return False
        ann.status, ann.finished_at = "cancelled", utcnow()
        db.execute(update(AnnouncementTarget).where(AnnouncementTarget.announcement_id == aid, AnnouncementTarget.status == "pending")
                   .values(status="skipped"))
        db.commit()
    return True


def retry(aid: int) -> int:
    """ส่งต่อ: เปลี่ยนรายการที่ล้มเหลว/ถูกข้ามกลับเป็นรอส่ง แล้วเริ่มส่ง · คืนจำนวนที่จะส่ง (0 = ไม่มีอะไรค้าง/กำลังส่งอยู่)"""
    with SessionLocal() as db:
        ann = db.get(Announcement, aid)
        if ann is None or ann.status == "sending":
            return 0
        db.execute(update(AnnouncementTarget).where(AnnouncementTarget.announcement_id == aid, AnnouncementTarget.status.in_(("failed", "skipped")))
                   .values(status="pending", error=""))
        n = db.scalar(select(func.count(AnnouncementTarget.id)).where(
            AnnouncementTarget.announcement_id == aid, AnnouncementTarget.status == "pending")) or 0
        if n:
            ann.status, ann.finished_at = "sending", None
        db.commit()
    if n:
        start(aid)
    return n


def mark_interrupted() -> int:
    """เปิดระบบใหม่: ประกาศที่ค้างสถานะกำลังส่ง (เพราะรีสตาร์ท) -> หยุดกลางคัน ให้คนกดส่งต่อเอง ไม่ส่งซ้ำอัตโนมัติ"""
    with SessionLocal() as db:
        res = db.execute(update(Announcement).where(Announcement.status == "sending").values(status="interrupted"))
        db.commit()
        return res.rowcount or 0


def counts(db, ids: list[int]) -> dict[int, dict[str, int]]:
    """จำนวนรายแชทตามสถานะของแต่ละประกาศ"""
    out = {i: {"pending": 0, "sent": 0, "failed": 0, "skipped": 0, "total": 0} for i in ids}
    if not ids:
        return out
    for aid, st, n in db.execute(select(AnnouncementTarget.announcement_id, AnnouncementTarget.status, func.count(AnnouncementTarget.id))
                                 .where(AnnouncementTarget.announcement_id.in_(ids)).group_by(AnnouncementTarget.announcement_id, AnnouncementTarget.status)):
        out[aid][st] = n
        out[aid]["total"] += n
    return out