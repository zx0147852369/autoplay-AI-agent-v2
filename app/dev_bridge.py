"""เชื่อมกับกลุ่มโปรแกรมเมอร์ (เช่น Autopay Support)

- ticket ใหม่ -> โพสต์รายละเอียดและรูปเข้ากลุ่ม
- ลูกค้าส่งข้อมูลเพิ่ม -> ตอบใต้โพสต์ ticket เดิม
- โปรแกรมเมอร์ตอบ (reply โพสต์ ticket หรือพิมพ์ #เลข) -> อัปเดตสถานะ และร่างข้อความถึงลูกค้า (รออนุมัติ)
"""

import asyncio
import json
import logging
import re
from datetime import timedelta
from pathlib import Path

from sqlalchemy import func, select

from . import ai_service, line_service
from .config import MEDIA_DIR
from .database import (
    Chat,
    Message,
    SessionLocal,
    Ticket,
    Setting,
    TicketEvent,
    TicketLink,
    get_settings,
    utcnow,
)
from .notices import draft_update, sync_resolved_notice
from .telegram_service import telegram

log = logging.getLogger(__name__)

SEVERITY_TH = {"low": "ต่ำ", "medium": "ปานกลาง", "high": "สูง", "critical": "วิกฤต"}
STATUS_TH = {"open": "เปิดใหม่", "in_progress": "กำลังแก้ไข", "resolved": "แก้ไขแล้ว", "closed": "ปิด"}
MAX_PHOTOS = 6

_tasks: set[asyncio.Task] = set()
last_error = ""  # ข้อผิดพลาดล่าสุดตอนส่งเข้ากลุ่ม (แสดงในหน้าภาพรวม)
BACKLOG_HOURS = 24


def configure() -> None:
    """โหลดการตั้งค่ากลุ่มโปรแกรมเมอร์ให้ telegram_service"""
    with SessionLocal() as db:
        settings = get_settings(db)
    telegram.set_dev(settings.get("dev_group_id"),
                     settings.get("dev_usernames", "") + "," + settings.get("bank_usernames", ""))
    telegram.set_staff(settings.get("staff_usernames", ""))
    fixed = telegram.mark_staff_history()
    if fixed:
        log.info("ปรับข้อความเก่าของทีมงาน %s ข้อความให้อยู่ฝั่งทีมงาน", fixed)
    telegram.set_ignore(settings.get("ignore_usernames", ""), settings.get("ignore_bots") == "1")
    removed = telegram.purge_ignored()
    if removed:
        log.info("ลบข้อความจากบัญชีที่ไม่รับข้อความ %s ข้อความ", removed)
    telegram.on_dev_message = _schedule
    telegram.on_connected = on_telegram_connected


async def auto_detect_group() -> int | None:
    """ยังไม่ได้เลือกกลุ่ม -> หากลุ่มชื่อ "Autopay Support" จากบัญชี Telegram ให้อัตโนมัติ"""
    with SessionLocal() as db:
        current = _group_id(get_settings(db))
    if current or not telegram.connected:
        return current
    dialogs = await telegram.list_dialogs()
    match = next((d for d in dialogs if d["kind"] != "user" and "autopay support" in d["title"].lower()), None)
    if not match:
        return None
    with SessionLocal() as db:
        chat = db.get(Chat, match["id"])
        if chat is None:
            db.add(Chat(id=match["id"], title=match["title"], kind=match["kind"]))
        db.merge(Setting(key="dev_group_id", value=str(match["id"])))
        db.commit()
    configure()
    log.info("ตั้งกลุ่ม %s เป็นกลุ่มโปรแกรมเมอร์อัตโนมัติ", match["title"])
    return match["id"]


async def post_backlog() -> int:
    """ticket ที่ยังไม่ปิดและยังไม่เคยส่งเข้ากลุ่ม (ภายใน 24 ชม.) -> เข้าคิวรออนุมัติ (หรือส่งเลยถ้าไม่ต้องอนุมัติ)"""
    since = utcnow() - timedelta(hours=BACKLOG_HOURS)
    with SessionLocal() as db:
        if get_settings(db).get("dev_forward") != "1":
            return 0
        posted = select(TicketLink.ticket_id)
        ids = list(db.scalars(select(Ticket.id).where(
            Ticket.status.in_(("open", "in_progress")), Ticket.created_at >= since, Ticket.id.not_in(posted),
            Ticket.dev_status == "",
        ).order_by(Ticket.created_at).limit(10)))
    done = 0
    for ticket_id in ids:
        if await queue_ticket(ticket_id):
            done += 1
    return done


async def queue_ticket(ticket_id: int) -> str:
    """ticket ใหม่ -> ถ้าต้องอนุมัติให้เข้าคิวรออนุมัติ ไม่งั้นส่งเข้ากลุ่มเลย คืนค่าสิ่งที่ทำ"""
    with SessionLocal() as db:
        settings = get_settings(db)
        ticket = db.get(Ticket, ticket_id)
        if not ticket or ticket.dev_status in ("pending", "sent", "skipped"):
            return ""
        if settings.get("dev_forward") != "1" or not _group_id(settings):
            return ""
        if settings.get("dev_require_approval") == "1":
            ticket.dev_status = "pending"
            db.add(TicketEvent(ticket_id=ticket_id, kind="dev", author="ระบบ",
                               body="รอแอดมินอนุมัติก่อนส่งเข้ากลุ่มโปรแกรมเมอร์"))
            db.commit()
        else:
            ticket = None
    if ticket is not None:
        try:
            await line_service.notify_dev_ticket(ticket_id)  # แจ้งการ์ดรออนุมัติเข้า LINE (ถ้าเปิดไว้)
        except Exception:  # noqa: BLE001 - แจ้ง LINE ไม่สำเร็จ ไม่ให้กระทบการเข้าคิว
            log.exception("line notify failed for dev ticket %s", ticket_id)
        return "pending"
    result = await post_ticket(ticket_id)
    return "sent" if result.startswith("ส่งเข้ากลุ่ม") else ""


def skip_ticket(ticket_id: int, username: str) -> bool:
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        if not ticket or ticket.dev_status != "pending":
            return False
        ticket.dev_status = "skipped"
        db.add(TicketEvent(ticket_id=ticket_id, kind="dev", author=username, body="เลือกไม่ส่งเข้ากลุ่มโปรแกรมเมอร์"))
        db.commit()
    return True


def pending_count(db) -> int:
    return db.scalar(select(func.count(Ticket.id)).where(Ticket.dev_status == "pending")) or 0


async def on_telegram_connected() -> None:
    try:
        if await auto_detect_group():
            await post_backlog()
    except Exception:  # noqa: BLE001
        log.exception("dev group setup on connect failed")


def schedule_backlog() -> None:
    task = asyncio.get_running_loop().create_task(_safe_backlog())
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def _safe_backlog() -> None:
    try:
        await post_backlog()
    except Exception:  # noqa: BLE001
        log.exception("post backlog failed")


def _schedule(info: dict) -> None:
    task = asyncio.get_running_loop().create_task(_safe_handle(info))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def _safe_handle(info: dict) -> None:
    try:
        await handle_dev_message(info)
    except Exception:  # noqa: BLE001 - งานเบื้องหลัง ห้ามล้มทั้งระบบ
        log.exception("handle dev message failed")


def _group_id(settings: dict[str, str]) -> int | None:
    try:
        return int(settings.get("dev_group_id") or 0) or None
    except ValueError:
        return None


def _save_links(ticket_id: int, chat_id: int, message_ids: list[int]) -> None:
    with SessionLocal() as db:
        for mid in message_ids:
            db.add(TicketLink(ticket_id=ticket_id, chat_id=chat_id, tg_message_id=mid))
        db.commit()


BANK_CATEGORIES = ("bank_connect",)


def parse_usernames(text: str) -> list[str]:
    return [u.strip().lstrip("@") for u in (text or "").replace("\n", ",").split(",") if u.strip()]


def handler_mentions(category: str) -> str:
    """เรื่องเชื่อมบัญชีธนาคาร -> แท็กทีมงานฝ่ายเชื่อมบัญชีแทนโปรแกรมเมอร์"""
    if category not in BANK_CATEGORIES:
        return ""
    with SessionLocal() as db:
        users = parse_usernames(get_settings(db).get("bank_usernames", ""))
    return " ".join(f"@{u}" for u in users)


def format_ticket(ticket: Ticket, chat_title: str) -> str:
    lines = [
        f"Ticket #{ticket.id} · {ai_service.CATEGORIES.get(ticket.category, ticket.category)}",
    ]
    if mentions := handler_mentions(ticket.category):
        lines.append(f"ฝากทีมเชื่อมบัญชีดำเนินการ: {mentions}")
    lines += [
        f"ความรุนแรง: {SEVERITY_TH.get(ticket.severity, ticket.severity)}",
        f"ลูกค้า: {ticket.customer_name or '-'} · แชท: {chat_title}",
    ]
    if ticket.website_url:
        site = ""
        if ticket.site_check:
            sc = json.loads(ticket.site_check)
            if not sc.get("ok"):
                site = f" (เข้าไม่ได้: {sc.get('error') or sc.get('status_code')})"
            elif sc.get("other_host"):
                site = f" (ถูกพาไป {sc.get('final_host')} โดเมนอาจหมดอายุ)"
            else:
                site = " (เข้าได้)"
        lines.append(f"เว็บไซต์: {ticket.website_url}{site}")
    else:
        lines.append("เว็บไซต์: ยังไม่มีลิงก์ (ขอจากลูกค้าแล้ว)")
    lines += ["", f"ปัญหา: {ticket.title}", ticket.summary]
    lines += ["", "ตอบกลับ (reply) ข้อความนี้เพื่ออัปเดต เช่น \"กำลังแก้\" \"แก้เสร็จแล้ว\" หรือขอข้อมูลลูกค้าเพิ่ม"]
    return "\n".join(lines)


async def post_ticket(ticket_id: int, force: bool = False, text: str | None = None, approver: str = "") -> str:
    """โพสต์ ticket เข้ากลุ่มโปรแกรมเมอร์ คืนค่าข้อความสรุปผล (text = ข้อความที่แอดมินแก้ก่อนอนุมัติ)"""
    custom_text = (text or "").strip()
    with SessionLocal() as db:
        settings = get_settings(db)
        ticket = db.get(Ticket, ticket_id)
        if not ticket:
            return "ไม่พบ ticket"
        posted = db.scalar(select(TicketLink.id).where(TicketLink.ticket_id == ticket_id).limit(1))
        chat = db.get(Chat, ticket.chat_id)
        photos = [str(MEDIA_DIR / a.media_path) for a in ticket.attachments][:MAX_PHOTOS]
        text = custom_text or format_ticket(ticket, chat.title if chat else str(ticket.chat_id))
    group_id = _group_id(settings)
    if not group_id:
        return "ยังไม่ได้เลือกกลุ่มโปรแกรมเมอร์ในหน้าตั้งค่า"
    if not force and (settings.get("dev_forward") != "1" or posted):
        return ""
    if not telegram.connected:
        return "ยังไม่ได้เชื่อมต่อ Telegram"
    global last_error
    try:
        root = await telegram.send_text(group_id, text)
    except Exception as e:  # noqa: BLE001 - เช่น บัญชีไม่ได้อยู่ในกลุ่ม หรือไม่มีสิทธิ์ส่งข้อความ
        log.exception("post ticket %s to dev group failed", ticket_id)
        last_error = f"ส่ง ticket #{ticket_id} เข้ากลุ่มไม่สำเร็จ: {e}"
        with SessionLocal() as db:
            db.add(TicketEvent(ticket_id=ticket_id, kind="dev", author="ระบบ", body=last_error))
            db.commit()
        return last_error
    ids = [root]
    photos = [p for p in photos if Path(p).exists()]
    if photos:
        try:
            ids += await telegram.send_files(group_id, photos, reply_to=root)
        except Exception:  # noqa: BLE001 - ส่งรูปไม่ได้ ไม่ต้องล้มทั้งหมด
            log.exception("post ticket photos failed")
    _save_links(ticket_id, group_id, ids)
    last_error = ""
    with SessionLocal() as db:
        db.get(Ticket, ticket_id).dev_status = "sent"
        body = f"อนุมัติและส่งเข้ากลุ่มโปรแกรมเมอร์แล้ว (โดย {approver})" if approver else "ส่งรายละเอียดเข้ากลุ่มโปรแกรมเมอร์แล้ว"
        db.add(TicketEvent(ticket_id=ticket_id, kind="dev", author=approver or "ระบบ", body=body))
        db.commit()
    return "ส่งเข้ากลุ่มโปรแกรมเมอร์แล้ว"


async def post_customer_update(ticket_id: int, messages: list[Message]) -> None:
    """ลูกค้าส่งข้อมูลเพิ่มเรื่อง ticket เดิม -> ตอบใต้โพสต์ ticket ในกลุ่ม"""
    if not messages or not telegram.connected:
        return
    with SessionLocal() as db:
        settings = get_settings(db)
        root = db.scalar(select(TicketLink.tg_message_id).where(TicketLink.ticket_id == ticket_id)
                         .order_by(TicketLink.id).limit(1))
    group_id = _group_id(settings)
    if not group_id:
        return
    if not root:
        # ยังไม่เคยส่งเข้ากลุ่ม: ถ้ารออนุมัติอยู่ ข้อมูลใหม่จะรวมไปตอนอนุมัติเอง / ถ้ายังไม่เข้าคิว -> เข้าคิว
        await queue_ticket(ticket_id)
        return
    lines = [followup_header(ticket_id)]
    lines += [f"- {m.sender_name or 'ลูกค้า'}: {m.text or '(รูปภาพ)'}" for m in messages]
    ids = [await telegram.send_text(group_id, "\n".join(lines), reply_to=root)]
    photos = [str(MEDIA_DIR / m.media_path) for m in messages if m.media_path][:MAX_PHOTOS]
    if photos:
        ids += await telegram.send_files(group_id, photos, reply_to=ids[0])
    _save_links(ticket_id, group_id, ids)


def followup_header(ticket_id: int) -> str:
    """หัวข้อความตามเรื่อง: ticket ไหน ส่งไปนานแค่ไหนแล้ว ตามมากี่ครั้ง (+ แท็กผู้ดูแล)"""
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        sent_at = db.scalar(select(func.min(TicketEvent.created_at)).where(
            TicketEvent.ticket_id == ticket_id, TicketEvent.kind == "dev",
            TicketEvent.body.like("%ส่ง%เข้ากลุ่มโปรแกรมเมอร์แล้ว%")))
        followups = db.scalar(select(func.count(TicketEvent.id)).where(
            TicketEvent.ticket_id == ticket_id, TicketEvent.kind == "dev", TicketEvent.body.like("ลูกค้าตามเรื่อง%")))
        db.add(TicketEvent(ticket_id=ticket_id, kind="dev", author="ระบบ",
                           body="ลูกค้าตามเรื่อง/ส่งข้อมูลเพิ่ม ส่งใต้โพสต์เดิมในกลุ่ม (ไม่ส่ง ticket ซ้ำ)"))
        db.commit()
        title, category = (ticket.title, ticket.category) if ticket else ("", "")
    head = f"ลูกค้าตามเรื่อง · Ticket #{ticket_id}" + (f" {title}" if title else "")
    if followups:
        head += f" (ตามครั้งที่ {followups + 1})"
    if sent_at:
        hours = (utcnow() - sent_at).total_seconds() / 3600
        head += f"\nส่งเรื่องไปแล้ว {int(hours)} ชม. {int(hours * 60) % 60} นาที ยังไม่ได้แก้ไข" if hours >= 1 else \
            f"\nส่งเรื่องไปแล้ว {int(hours * 60)} นาที ยังไม่ได้แก้ไข"
    if mentions := handler_mentions(category):
        head += f"\n{mentions}"
    return head


# ---------------------------------------------------------------- ข้อความจากโปรแกรมเมอร์
def find_ticket(db, info: dict) -> Ticket | None:
    if info.get("reply_to"):
        ticket_id = db.scalar(select(TicketLink.ticket_id).where(
            TicketLink.chat_id == info["chat_id"], TicketLink.tg_message_id == info["reply_to"]).limit(1))
        if ticket_id:
            return db.get(Ticket, ticket_id)
    match = re.search(r"#\s?(\d+)", info.get("text") or "")
    if match:
        return db.get(Ticket, int(match.group(1)))
    return None


FALLBACK_MESSAGES = {
    "in_progress": "ทีมงานได้รับเรื่องและกำลังดำเนินการแก้ไขให้ค่ะ รบกวนรอสักครู่นะคะ",
    "resolved": "",  # ใช้ข้อความแจ้งแก้ไขเสร็จจากหน้าตั้งค่า
    # ไม่ใส่ข้อความโปรแกรมเมอร์ตรงๆ (อาจมีศัพท์ภายใน) แอดมินแก้ให้ตรงกับที่ต้องการก่อนอนุมัติ ดูได้จากหมายเหตุ
    "need_info": "รบกวนขอข้อมูลเพิ่มเติมเพื่อให้ทีมงานตรวจสอบได้ค่ะ เช่น ยูสเซอร์ที่ใช้งาน และภาพหน้าจอที่พบปัญหา",
}


def guess_intent(text: str) -> str:
    """ใช้เมื่อ AI ใช้งานไม่ได้ (เช่น โควตาเต็ม): จัดประเภทจากคำสำคัญ"""
    t = text.lower()
    if re.search(r"เสร็จ|เรียบร้อย|แก้แล้ว|แก้ให้แล้ว|ใช้ได้แล้ว|fixed|done|ลองใหม่", t):
        return "resolved"
    if re.search(r"ขอ\s*(ยูส|user|สลิป|รูป|ข้อมูล|เบอร์|ไอดี|id|ลิงก์|link)|ส่ง.*มา", t):
        return "need_info"
    if re.search(r"รับ|กำลัง|ดูให้|ตรวจสอบ|เช็ค|เช็ก|check|ok|โอเค", t):
        return "in_progress"
    return "comment"


async def handle_dev_message(info: dict) -> str:
    """ประมวลผลข้อความโปรแกรมเมอร์ คืนค่า intent ที่ใช้ (หรือค่าว่างถ้าไม่เกี่ยวกับ ticket)"""
    with SessionLocal() as db:
        settings = get_settings(db)
        if settings.get("dev_watch") != "1":
            return ""
        ticket = find_ticket(db, info)
        if not ticket:
            log.info("dev message %s not linked to any ticket", info.get("message_id"))
            return ""
        ticket_id = ticket.id
        db.add(TicketEvent(ticket_id=ticket_id, kind="dev", author=f"@{info['username']}", body=info["text"]))
        db.commit()
        db.refresh(ticket)
        db.expunge(ticket)

    try:
        result = await ai_service.classify_dev_message(settings, ticket, info["text"])
    except ai_service.AIError as e:
        intent = guess_intent(info["text"])
        result = ai_service.DevIntent(intent, FALLBACK_MESSAGES.get(intent, ""),
                                      f"จัดประเภทจากคำสำคัญ (AI ใช้งานไม่ได้: {e})")

    who = f"@{info['username']}"
    note = f"จากโปรแกรมเมอร์ {who}: {info['text']}"
    ack = []
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        old = ticket.status
        if result.intent == "in_progress" and ticket.status == "open":
            ticket.status = "in_progress"
        elif result.intent == "resolved" and ticket.status in ("open", "in_progress"):
            ticket.status = "resolved"
        if ticket.status != old:
            db.add(TicketEvent(ticket_id=ticket_id, kind="status", author=who,
                               body=f"สถานะ: {STATUS_TH[old]} → {STATUS_TH[ticket.status]}"))
            ack.append(f"สถานะ → {STATUS_TH[ticket.status]}")
        db.commit()

        drafted = False
        if result.intent == "resolved" and ticket.status != old:
            drafted = sync_resolved_notice(db, ticket, who, text=result.customer_message or None) == "created"
        elif result.intent == "need_info" and result.customer_message:
            draft_update(db, ticket, result.customer_message, note)
            drafted = True
        elif result.intent == "in_progress" and ticket.status != old and result.customer_message:
            draft_update(db, ticket, result.customer_message, note)
            drafted = True
        if drafted:
            ack.append("ร่างข้อความถึงลูกค้าแล้ว รอแอดมินอนุมัติ")

    if ack and telegram.connected:
        try:
            mid = await telegram.send_text(info["chat_id"], f"Ticket #{ticket_id}: " + " · ".join(ack),
                                           reply_to=info["message_id"])
            _save_links(ticket_id, info["chat_id"], [mid])
        except Exception:  # noqa: BLE001
            log.exception("send ack to dev group failed")
    return result.intent
