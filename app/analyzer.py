"""รับข้อความใหม่ -> รอลูกค้าพิมพ์ครบ -> ให้ AI วิเคราะห์ -> สร้างร่างคำตอบ (รออนุมัติ) + ticket"""

import asyncio
import json
import logging
import re
import time
from datetime import timedelta
from difflib import SequenceMatcher

from sqlalchemy import func, select

from . import ai_service, dev_bridge, learning, line_service
from .telegram_service import STICKER_TEXT
from .database import (
    Chat, Guide, GuideQuestion, Message, Reply, SessionLocal, Ticket, TicketAttachment, TicketEvent, get_settings,
    guide_images, int_setting, utcnow,
)
from urllib.parse import urlparse

from .site_checker import check_site, same_site

log = logging.getLogger(__name__)

OPEN_STATUSES = ("open", "in_progress")

SWEEP_INTERVAL = 60          # ตรวจหาข้อความค้างทุก 60 วินาที
RETRY_AFTER_ERROR = 5 * 60   # ถ้า AI ผิดพลาด รอ 5 นาทีก่อนลองใหม่ (กันโควตาฟรีหมดเร็ว)

# ขอลิงก์เว็บไซต์จากลูกค้าเมื่อแจ้งปัญหาแต่ไม่ได้ให้ลิงก์
ASK_LINK = "รบกวนขอลิงก์เว็บไซต์ที่พบปัญหาด้วยนะคะ"
ASK_LINK_FULL = "รับเรื่องแล้วค่ะ " + ASK_LINK
LINK_REQUEST_RE = re.compile(r"ลิงก์|ลิงค์|ลิ้งค์|ลิ้งก์|link|url", re.IGNORECASE)
# ทีมงานขอข้อมูลจากลูกค้า -> เมื่อลูกค้าส่งข้อมูลมา ให้ตอบรับ
INFO_REQUEST_RE = re.compile(
    r"(ขอ|รบกวน|แจ้ง|ส่ง|แนบ).{0,30}(ลิงก์|ลิงค์|ลิ้งค์|ลิ้งก์|link|url|เว็บ|ยูส|user|ไอดี|สลิป|รูป|ภาพ|หน้าจอ|ข้อมูล|เบอร์|ชื่อบัญชี|เลขบัญชี)",
    re.IGNORECASE)
ACK_INFO = "ได้รับข้อมูลแล้วค่ะ ทีมงานกำลังตรวจสอบให้นะคะ รอสักครู่นะคะ"
URL_RE = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)
# ลิงก์รูปภาพ / โซเชียล ไม่ใช่เว็บไซต์ของลูกค้า
NOT_SITE_HOSTS = ("imgur.com", "pic.in.th", "ibb.co", "postimg", "prnt.sc", "gyazo", "t.me", "telegram.",
                  "line.me", "lin.ee", "facebook.com", "fb.com", "youtube.com", "youtu.be", "google.com/drive",
                  "drive.google", "tiktok.com", "instagram.com")

_timers: dict[int, asyncio.Task] = {}
_last_attempt: dict[int, float] = {}
_locks: dict[int, asyncio.Lock] = {}
last_error: dict[int, str] = {}  # chat_id -> ข้อผิดพลาดล่าสุด (แสดงบนหน้าเว็บ)


def schedule(chat_id: int) -> None:
    """ลูกค้ามักส่งหลายข้อความติดกัน จึงรอจนเงียบไป N วินาทีแล้วค่อยวิเคราะห์ทีเดียว"""
    with SessionLocal() as db:
        delay = int_setting(get_settings(db), "debounce_seconds", 0, 600)
    if (task := _timers.get(chat_id)) and not task.done():
        task.cancel()
    _timers[chat_id] = asyncio.get_running_loop().create_task(_delayed(chat_id, delay))


async def _delayed(chat_id: int, delay: int) -> None:
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return
    try:
        await analyze(chat_id)
    except Exception:  # noqa: BLE001 - งานเบื้องหลัง ห้ามล้มทั้งระบบ
        log.exception("analyze chat %s failed", chat_id)


def is_waiting(chat_id: int) -> bool:
    task = _timers.get(chat_id)
    return bool(task and not task.done())


async def sweep_once() -> None:
    """หาแชทที่มีข้อความลูกค้ายังไม่ได้วิเคราะห์ แล้ววิเคราะห์ให้อัตโนมัติ"""
    with SessionLocal() as db:
        delay = int_setting(get_settings(db), "debounce_seconds", 0, 600)
        monitored = set(db.scalars(select(Chat.id).where(Chat.monitored)))
        rows = db.execute(
            select(Message.chat_id, func.max(Message.date))
            .where(Message.analyzed.is_(False), Message.is_outgoing.is_(False))
            .group_by(Message.chat_id)
        ).all()
    now = utcnow()
    for chat_id, last in rows:
        if chat_id not in monitored or is_waiting(chat_id) or (now - last).total_seconds() < delay:
            continue
        if chat_id in last_error and time.monotonic() - _last_attempt.get(chat_id, 0) < RETRY_AFTER_ERROR:
            continue
        try:
            await analyze(chat_id)
        except Exception:  # noqa: BLE001 - งานเบื้องหลัง ห้ามล้มทั้งระบบ
            log.exception("auto analyze chat %s failed", chat_id)


async def sweeper() -> None:
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        try:
            await sweep_once()
        except Exception:  # noqa: BLE001
            log.exception("sweeper failed")


async def analyze(chat_id: int) -> str:
    """วิเคราะห์ข้อความที่ยังไม่ได้วิเคราะห์ของแชทนี้ คืนค่าข้อความสรุปผล"""
    _last_attempt[chat_id] = time.monotonic()
    lock = _locks.setdefault(chat_id, asyncio.Lock())
    async with lock:
        with SessionLocal() as db:
            settings = get_settings(db)
            chat = db.get(Chat, chat_id)
            new_messages = list(db.scalars(
                select(Message).where(Message.chat_id == chat_id, Message.analyzed.is_(False))
                .order_by(Message.date, Message.id)
            ))
            if not new_messages:
                return "ไม่มีข้อความใหม่"
            limit = max(len(new_messages), int_setting(settings, "context_messages", 5, 100))
            history = list(db.scalars(
                select(Message).where(Message.chat_id == chat_id)
                .order_by(Message.date.desc(), Message.id.desc()).limit(limit)
            ))[::-1]
            open_tickets = list(db.scalars(
                select(Ticket).where(Ticket.chat_id == chat_id, Ticket.status.in_(OPEN_STATUSES))
            ))
            asked_info = team_asked_info(db, chat_id, new_messages)
            guides = list(db.scalars(select(Guide).order_by(Guide.id)))

        if str(chat_id) == settings.get("dev_group_id"):
            _mark_analyzed(new_messages)  # กลุ่มโปรแกรมเมอร์ไม่ใช่แชทลูกค้า
            return "กลุ่มโปรแกรมเมอร์"
        if settings.get("auto_draft") != "1" and settings.get("auto_ticket") != "1":
            _mark_analyzed(new_messages)
            return "ปิดการทำงานอัตโนมัติไว้"
        customer_new = [m for m in new_messages if not m.is_outgoing]
        if all(not m.media_path and (m.text or "").strip() in ("", STICKER_TEXT) for m in customer_new):
            _mark_analyzed(new_messages)  # มีแต่สติกเกอร์ / ข้อความว่าง ไม่ต้องเรียก AI
            return "มีแต่สติกเกอร์ ไม่ต้องวิเคราะห์"
        rejected = recent_rejected(chat_id)

        notes_text, note_ids = "", []
        if settings.get("learn_use_notes") == "1":  # โน้ตที่ AI เรียนรู้ไว้ (สมอง AI) · ดึงไม่ได้ก็วิเคราะห์ต่อได้ปกติ
            try:
                with SessionLocal() as db:
                    notes_text, note_ids = learning.notes_for_prompt(
                        db, chat_id, " ".join(m.text or "" for m in customer_new))
            except Exception:  # noqa: BLE001
                log.exception("load AI notes failed")

        try:
            result = await ai_service.analyze_chat(
                settings, chat.title if chat else str(chat_id), history, new_messages, open_tickets,
                rejected=[r for r, _ in rejected], chat_website=chat.website_url if chat else "", guides=guides,
                notes=notes_text,
            )
        except ai_service.AIError as e:
            last_error[chat_id] = str(e)
            log.warning("AI error on chat %s: %s", chat_id, e)
            return f"ผิดพลาด: {e}"
        last_error.pop(chat_id, None)
        if note_ids:  # บอกแอดมินว่าครั้งนี้ใช้บทเรียนข้อไหน (ตรวจสอบย้อนกลับได้ที่หน้า "สมอง AI")
            shown = ", ".join(f"#{i}" for i in note_ids[:6]) + (f" +{len(note_ids) - 6}" if len(note_ids) > 6 else "")
            result.note_for_admin = (result.note_for_admin + f" · ใช้บทเรียน AI {shown}").strip(" ·")

        ticket_id = None
        if result.is_issue and settings.get("auto_ticket") == "1":
            ticket_id = await _save_ticket(chat_id, result, new_messages, settings)
        ask_link = bool(ticket_id) and settings.get("ask_link") == "1" and needs_link_request(ticket_id)
        if ask_link:
            if result.needs_reply and result.reply_text.strip():
                if not LINK_REQUEST_RE.search(result.reply_text):
                    result.reply_text = result.reply_text.rstrip() + "\n" + ASK_LINK
            else:
                result.needs_reply, result.reply_text = True, ASK_LINK_FULL
            result.note_for_admin = (result.note_for_admin + " · ระบบขอลิงก์เว็บไซต์จากลูกค้า").strip(" ·")
        ack = asked_info and settings.get("ack_info") == "1" and has_content(customer_new)
        if ack:
            await _apply_customer_link(chat_id, customer_new, open_tickets, settings)
            if not (result.needs_reply and result.reply_text.strip()):
                result.needs_reply, result.reply_text = True, ACK_INFO
                result.note_for_admin = (result.note_for_admin + " · ลูกค้าส่งข้อมูลที่ขอไปแล้ว ระบบร่างคำตอบรับ").strip(" ·")
        if result.question.strip():
            _handle_question(chat_id, result, guides, customer_new)
        similar = next((text for text, _ in rejected if is_similar(result.reply_text, text)), None)
        if result.needs_reply and similar:
            log.info("ไม่สร้างร่างซ้ำกับที่แอดมินปฏิเสธไปแล้วในแชท %s", chat_id)
            result.needs_reply = False
        if result.needs_reply and result.reply_text.strip() and (settings.get("auto_draft") == "1" or ask_link or ack):
            draft_id = _save_draft(chat_id, result, new_messages, ticket_id)
            try:
                await line_service.notify_reply(draft_id)
            except Exception:  # noqa: BLE001 - แจ้ง LINE ไม่สำเร็จ ไม่ให้ล้มการวิเคราะห์
                log.exception("line notify failed for reply %s", draft_id)
        _mark_analyzed(new_messages)

        parts = []
        if ticket_id:
            parts.append(f"ticket #{ticket_id}")
        if result.needs_reply:
            parts.append("สร้างร่างคำตอบแล้ว")
        return ", ".join(parts) or "ไม่ต้องตอบ / ไม่ใช่การแจ้งปัญหา"


GUIDE_HOLD = "ขอบคุณที่สอบถามค่ะ ขอตรวจสอบข้อมูลสักครู่นะคะ แล้วจะรีบแจ้งกลับค่ะ"


def _handle_question(chat_id: int, result: ai_service.Analysis, guides: list[Guide], customer_msgs: list[Message]) -> None:
    """ลูกค้าถามวิธีใช้งานหลังบ้าน -> ตอบจากคู่มือ / ถ้าไม่มีในคู่มือ บันทึกคำถามให้แอดมินเพิ่มคู่มือ"""
    question = result.question.strip()
    guide = next((g for g in guides if g.id == result.guide_id), None) if result.answered_from_guide else None
    with SessionLocal() as db:
        if guide:
            db.get(Guide, guide.id).used_count += 1
            note = f"ตอบจากคู่มือ #{guide.id}: {guide.title}"
            result.media = guide_images(guide)
            if result.media:
                note += f" (แนบรูป {len(result.media)} รูป)"
            if not (result.needs_reply and result.reply_text.strip()):
                result.needs_reply, result.reply_text = True, guide.answer
        else:
            since = utcnow() - timedelta(hours=24)
            recent = db.scalars(select(GuideQuestion.question).where(
                GuideQuestion.chat_id == chat_id, GuideQuestion.status == "open", GuideQuestion.created_at >= since))
            if not any(is_similar(question, q) for q in recent):
                db.add(GuideQuestion(chat_id=chat_id, question=question,
                                     asked_by=customer_msgs[-1].sender_name if customer_msgs else ""))
            note = "คำถามนี้ยังไม่มีในคู่มือ ตรวจคำตอบก่อนส่ง · เพิ่มคู่มือได้ที่หน้า \"คู่มือตอบคำถาม\""
            if not (result.needs_reply and result.reply_text.strip()):
                result.needs_reply, result.reply_text = True, GUIDE_HOLD
        db.commit()
    result.note_for_admin = (f"คำถามการใช้งาน: {question} · {note} · " + result.note_for_admin).strip(" ·")


def team_asked_info(db, chat_id: int, new_messages: list[Message]) -> bool:
    """ข้อความล่าสุดของทีมงาน (ก่อนข้อความใหม่ของลูกค้า) เป็นการขอข้อมูลจากลูกค้าหรือไม่"""
    customer_new = [m for m in new_messages if not m.is_outgoing]
    if not customer_new:
        return False
    first = customer_new[0]
    last_team = db.scalar(select(Message.text).where(
        Message.chat_id == chat_id, Message.is_outgoing.is_(True),
        (Message.date < first.date) | ((Message.date == first.date) & (Message.id < first.id)),
    ).order_by(Message.date.desc(), Message.id.desc()).limit(1))
    if last_team is None:
        # ข้อความที่ส่งผ่านระบบแต่ Telegram ยังไม่ส่งกลับมาเก็บ ใช้ข้อความที่อนุมัติส่งล่าสุดแทน
        last_team = db.scalar(select(Reply.final_text).where(
            Reply.chat_id == chat_id, Reply.status == "sent", Reply.decided_at <= first.date,
        ).order_by(Reply.decided_at.desc()).limit(1))
    return bool(last_team and INFO_REQUEST_RE.search(last_team))


def has_content(messages: list[Message]) -> bool:
    """ลูกค้าส่งข้อมูลจริง (ข้อความ / รูป) ไม่ใช่แค่สติกเกอร์หรือคำขอบคุณสั้นๆ"""
    for m in messages:
        text = (m.text or "").strip()
        if m.media_path or URL_RE.search(text):
            return True
        if text and text != STICKER_TEXT and not re.fullmatch(r"(ok|โอเค|ครับ|ค่ะ|คะ|จ้า|ขอบคุณ\S*|thank\S*|👍|🙏)[\s!.]*", text, re.I):
            return True
    return False


async def _apply_customer_link(chat_id: int, customer_msgs: list[Message], open_tickets: list[Ticket], settings) -> None:
    """ลูกค้าส่งลิงก์มาตามที่ขอ -> ใส่ให้ ticket ที่ยังไม่มีลิงก์ แล้วตรวจเว็บ"""
    url = find_site_url(customer_msgs)
    if not url:
        return
    targets = [t.id for t in open_tickets if not t.website_url]
    for ticket_id in targets:
        with SessionLocal() as db:
            ticket = db.get(Ticket, ticket_id)
            if not ticket or ticket.website_url:
                continue
            ticket.website_url = url
            db.add(TicketEvent(ticket_id=ticket_id, kind="status", author="AI",
                               body=f"ลูกค้าส่งลิงก์เว็บไซต์มา: {url}"))
            db.commit()
        if settings.get("site_check") == "1":
            await run_site_check(ticket_id)


REJECT_MEMORY_HOURS = 24
SIMILAR_RATIO = 0.6


def recent_rejected(chat_id: int) -> list[tuple[str, str]]:
    """ร่างที่แอดมินปฏิเสธในแชทนี้ภายใน 24 ชม. -> [(ข้อความ, เหตุผล)]"""
    since = utcnow() - timedelta(hours=REJECT_MEMORY_HOURS)
    with SessionLocal() as db:
        rows = db.execute(select(Reply.final_text, Reply.reject_reason).where(
            Reply.chat_id == chat_id, Reply.status == "rejected", Reply.decided_at >= since
        ).order_by(Reply.decided_at.desc()).limit(5)).all()
    return [(text or "", reason or "") for text, reason in rows]


def is_similar(a: str, b: str) -> bool:
    a, b = re.sub(r"\s+", " ", a or "").strip(), re.sub(r"\s+", " ", b or "").strip()
    return bool(a and b) and SequenceMatcher(None, a, b).ratio() >= SIMILAR_RATIO


def find_site_url(messages: list[Message]) -> str:
    """ลิงก์เว็บไซต์แรกที่ลูกค้าพิมพ์มา (ข้ามลิงก์รูปภาพ/โซเชียล)"""
    for m in messages:
        for url in URL_RE.findall(m.text or ""):
            url = url.rstrip(".,;")
            if not any(host in url.lower() for host in NOT_SITE_HOSTS):
                return url
    return ""


def needs_link_request(ticket_id: int) -> bool:
    """ticket ยังไม่มีลิงก์เว็บไซต์ และยังไม่เคยขอลิงก์จากลูกค้า"""
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        if not ticket or ticket.website_url:
            return False
        # ร่าง AI ที่ยังไม่ส่งจะถูกแทนที่ด้วยร่างใหม่ จึงนับเฉพาะที่ส่งแล้ว หรือข้อความประเภทอื่นที่ยังรออยู่
        asked = db.scalars(select(Reply.final_text).where(
            Reply.ticket_id == ticket_id,
            Reply.status.in_(("sending", "sent")) | (Reply.status.in_(("pending", "failed")) & (Reply.kind != "ai"))))
        return not any(LINK_REQUEST_RE.search(text or "") for text in asked)


def _mark_analyzed(messages: list[Message]) -> None:
    with SessionLocal() as db:
        for m in messages:
            db.get(Message, m.id).analyzed = True
        db.commit()


def _save_draft(chat_id: int, result: ai_service.Analysis, new_messages: list[Message], ticket_id) -> int:
    valid_ids = {m.tg_message_id for m in new_messages if not m.is_outgoing}
    reply_to = result.reply_to_message_id if result.reply_to_message_id in valid_ids else None
    if reply_to is None and valid_ids:
        reply_to = max(valid_ids)
    with SessionLocal() as db:
        # ร่างเก่าที่ยังไม่อนุมัติของแชทนี้ล้าสมัยแล้ว เพราะมีข้อความใหม่เข้ามา
        for old in db.scalars(select(Reply).where(
                Reply.chat_id == chat_id, Reply.status == "pending", Reply.kind == "ai")):
            old.status = "superseded"
        draft = Reply(
            chat_id=chat_id,
            reply_to_tg_id=reply_to,
            ai_text=result.reply_text.strip(),
            final_text=result.reply_text.strip(),
            note=result.note_for_admin,
            ticket_id=ticket_id,
            media=json.dumps(result.media) if result.media else "",
        )
        db.add(draft)
        db.commit()
        return draft.id


def _host(url: str) -> str:
    url = (url or "").strip()
    return (urlparse(url if "://" in url else "https://" + url).hostname or "").lower()


def pick_website(chat_website: str, ai_url: str, customer_msgs: list[Message], chat_text: str = "") -> str:
    """เลือกลิงก์เว็บของ ticket: ลิงก์ที่ลูกค้าพิมพ์ (ถ้าอยู่ในเว็บเดียวกับที่แอดมินตั้งไว้) > เว็บประจำแชท > ลิงก์จาก AI

    ลิงก์จาก AI ใช้ได้เฉพาะเมื่อโดเมนนั้นมีอยู่จริงในข้อความแชท (กัน AI เดาจากชื่อกลุ่ม)"""
    ai_url = (ai_url or "").strip()
    text = (chat_text or " ".join(m.text or "" for m in customer_msgs)).lower()
    host = _host(ai_url)
    if ai_url and (not host or host.removeprefix("www.") not in text):
        log.info("ไม่ใช้ลิงก์ %s จาก AI เพราะไม่พบในข้อความแชท", ai_url)
        ai_url = ""
    found = ai_url or find_site_url(customer_msgs)
    if not chat_website:
        return found
    if found:
        host_a, host_b = _host(found), _host(chat_website)
        if host_a and host_b and same_site(host_a, host_b):
            return found  # ลิงก์เฉพาะหน้า (เช่น /login) ของเว็บเดียวกัน
    return chat_website


async def _save_ticket(chat_id: int, result: ai_service.Analysis, new_messages: list[Message], settings) -> int:
    customer_msgs = [m for m in new_messages if not m.is_outgoing]
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
        chat_website = chat.website_url if chat else ""
        # ข้อความในแชท (ไม่รวมชื่อกลุ่ม) ใช้ยืนยันว่าลิงก์จาก AI มีอยู่จริง
        chat_text = " ".join(t or "" for t in db.scalars(select(Message.text).where(Message.chat_id == chat_id)
                                        .order_by(Message.date.desc()).limit(300)))
        ticket = None
        created = False
        if result.existing_ticket_id:
            ticket = db.get(Ticket, result.existing_ticket_id)
            if not ticket or ticket.chat_id != chat_id or ticket.status not in OPEN_STATUSES:
                ticket = None
        if ticket is None:
            # AI ให้เปิดใหม่ แต่แชทนี้มี ticket ปัญหาเดียวกันที่ยังไม่แก้ -> ใช้ใบเดิม (ไม่ส่งเข้ากลุ่มซ้ำ แค่ตามเรื่อง)
            ticket = find_duplicate(db, chat_id, result)
            if ticket:
                log.info("ปัญหาเดิมของ ticket #%s ในแชท %s ไม่เปิดใบใหม่", ticket.id, chat_id)
        if ticket is None:
            ticket = Ticket(
                chat_id=chat_id,
                category=result.issue_category,
                title=result.issue_title or ai_service.CATEGORIES.get(result.issue_category, "ปัญหา"),
                summary=result.issue_summary,
                severity=result.severity,
                customer_name=result.customer_name or (customer_msgs[0].sender_name if customer_msgs else ""),
                website_url=pick_website(chat_website, result.website_url, customer_msgs, chat_text),
            )
            db.add(ticket)
            db.flush()
            created = True
            db.add(TicketEvent(ticket_id=ticket.id, kind="ai_summary", author="AI", body=result.issue_summary))
        else:
            db.add(TicketEvent(ticket_id=ticket.id, kind="ai_summary", author="AI",
                               body="ข้อมูลเพิ่มเติม: " + result.issue_summary))
            if not ticket.website_url:
                ticket.website_url = pick_website(chat_website, result.website_url, customer_msgs, chat_text)
            if ai_service.SEVERITIES.index(result.severity) > ai_service.SEVERITIES.index(ticket.severity):
                ticket.severity = result.severity

        for m in customer_msgs:
            db.add(TicketEvent(ticket_id=ticket.id, kind="customer_message", author=m.sender_name,
                               body=m.text or "", media_path=m.media_path or "", created_at=m.date))
            if m.media_path:
                db.add(TicketAttachment(ticket_id=ticket.id, media_path=m.media_path, caption=m.text))
        db.commit()
        ticket_id, url = ticket.id, ticket.website_url

    if url and settings.get("site_check") == "1":
        try:
            await run_site_check(ticket_id)
        except Exception:  # noqa: BLE001
            log.exception("site check failed for ticket %s", ticket_id)
    # แจ้งกลุ่มโปรแกรมเมอร์: ticket ใหม่ -> โพสต์รายละเอียด, ticket เดิม -> ส่งข้อมูลเพิ่มจากลูกค้า
    try:
        if created:
            await dev_bridge.queue_ticket(ticket_id)
        else:
            await dev_bridge.post_customer_update(ticket_id, customer_msgs)
    except Exception:  # noqa: BLE001
        log.exception("notify dev group failed for ticket %s", ticket_id)
    return ticket_id


# หมวดกว้างๆ ต้องดูหัวข้อด้วยว่าเป็นเรื่องเดียวกันไหม
BROAD_CATEGORIES = ("other", "payment_other")
DUPLICATE_TITLE_RATIO = 0.55


def find_duplicate(db, chat_id: int, result: ai_service.Analysis) -> Ticket | None:
    """ticket ที่ยังไม่แก้ในแชทเดียวกันที่เป็นปัญหาเดียวกัน (หมวดเดียวกัน หรือหัวข้อคล้ายกัน)"""
    title = re.sub(r"\s+", " ", result.issue_title or "").strip()
    best = None
    for t in db.scalars(select(Ticket).where(Ticket.chat_id == chat_id, Ticket.status.in_(OPEN_STATUSES))
                        .order_by(Ticket.created_at.desc())):
        same_cat = t.category == result.issue_category and t.category not in BROAD_CATEGORIES
        # หมวดเฉพาะคนละหมวด (เช่น ฝาก vs ถอน) ไม่ใช่เรื่องเดียวกัน ดูหัวข้อเฉพาะเมื่อมีฝั่งใดเป็นหมวดกว้าง
        broad = t.category in BROAD_CATEGORIES or result.issue_category in BROAD_CATEGORIES
        similar = broad and bool(title) and SequenceMatcher(None, title, t.title or "").ratio() >= DUPLICATE_TITLE_RATIO
        if similar or same_cat:
            if t.dev_status == "sent":
                return t  # ส่งให้ทีมงานแล้ว -> ตามเรื่องใบนี้
            best = best or t
    return best


async def run_site_check(ticket_id: int) -> dict | None:
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        url = ticket.website_url if ticket else ""
    if not url:
        return None
    result = await check_site(url)
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        ticket.site_check = json.dumps(result, ensure_ascii=False)
        status = "เข้าไม่ได้" if not result["ok"] else (
            f"ถูกพาไปโดเมนอื่น ({result.get('final_host')})" if result.get("other_host") else "เข้าได้")
        detail = f"HTTP {result['status_code']}" if result["status_code"] else result["error"]
        db.add(TicketEvent(ticket_id=ticket_id, kind="site_check", author="ระบบ",
                           body=f"ตรวจ {result['url']}: {status} ({detail}, {result['elapsed_ms']} ms)"))
        db.commit()
    return result
