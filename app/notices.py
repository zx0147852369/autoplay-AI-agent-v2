"""ร่างข้อความถึงลูกค้าที่เกิดจากการอัปเดต ticket (ทุกข้อความต้องรออนุมัติก่อนส่ง)"""

from sqlalchemy import func, select

from .database import DEFAULT_SETTINGS, Reply, Ticket, get_settings

# kind ของ Reply ที่เป็นการอัปเดตความคืบหน้าให้ลูกค้า (ไม่ใช่ร่างคำตอบจาก AI)
UPDATE_KINDS = ("resolved", "dev_update")


def resolved_message(settings: dict[str, str], ticket: Ticket) -> str:
    customer = (ticket.customer_name or "").split(" (@")[0].strip() or "ลูกค้า"
    template = settings.get("resolved_message") or DEFAULT_SETTINGS["resolved_message"]
    try:
        return template.format(customer=customer, title=ticket.title, ticket_id=ticket.id)
    except (KeyError, IndexError, ValueError):
        return template  # รูปแบบข้อความผิด ใช้ข้อความตามที่พิมพ์ไว้


def customer_reply_to(db, ticket_id: int) -> int | None:
    """ข้อความที่ลูกค้าแจ้งปัญหาไว้ (ดูจากคำตอบก่อนหน้าของ ticket นี้) ใช้ตอบกลับให้อยู่ในเรื่องเดียวกัน"""
    return db.scalar(select(Reply.reply_to_tg_id).where(
        Reply.ticket_id == ticket_id, Reply.reply_to_tg_id.is_not(None)).order_by(Reply.created_at).limit(1))


def _pending(db, ticket_id: int, kinds) -> list[Reply]:
    return list(db.scalars(select(Reply).where(
        Reply.ticket_id == ticket_id, Reply.kind.in_(kinds), Reply.status.in_(("pending", "failed")))))


def sync_resolved_notice(db, ticket: Ticket, username: str, text: str | None = None) -> str:
    """เรียกเมื่อสถานะ ticket เปลี่ยน: เปลี่ยนเป็น "แก้ไขแล้ว" -> ร่างข้อความแจ้งลูกค้า (รออนุมัติ)
    เปลี่ยนกลับเป็นยังไม่เสร็จ -> ยกเลิกข้อความแจ้งแก้ไขเสร็จที่ยังไม่ได้ส่ง"""
    pending = _pending(db, ticket.id, ("resolved",))
    if ticket.status in ("open", "in_progress"):
        for r in pending:
            r.status = "superseded"
        db.commit()
        return "cancelled" if pending else ""
    if ticket.status != "resolved" or pending:
        return ""
    settings = get_settings(db)
    if settings.get("notify_resolved") != "1":
        return ""
    already_sent = db.scalar(select(func.count(Reply.id)).where(
        Reply.ticket_id == ticket.id, Reply.kind == "resolved", Reply.status == "sent"))
    if already_sent:
        return ""
    # ข้อความความคืบหน้าเก่าที่ยังไม่ได้ส่ง (เช่น "กำลังแก้ไข") ไม่จำเป็นแล้ว
    for r in _pending(db, ticket.id, ("dev_update",)):
        r.status = "superseded"
    text = text or resolved_message(settings, ticket)
    db.add(Reply(chat_id=ticket.chat_id, reply_to_tg_id=customer_reply_to(db, ticket.id), ai_text=text,
                 final_text=text, kind="resolved", ticket_id=ticket.id,
                 note=f"แจ้งลูกค้าว่า ticket #{ticket.id} แก้ไขเรียบร้อยแล้ว (โดย {username})"))
    db.commit()
    return "created"


def draft_update(db, ticket: Ticket, text: str, note: str) -> None:
    """ร่างข้อความความคืบหน้าถึงลูกค้า (เช่น กำลังแก้ไข / ขอข้อมูลเพิ่ม) แทนที่ข้อความความคืบหน้าเดิมที่ยังไม่ส่ง"""
    for r in _pending(db, ticket.id, ("dev_update",)):
        r.status = "superseded"
    db.add(Reply(chat_id=ticket.chat_id, reply_to_tg_id=customer_reply_to(db, ticket.id), ai_text=text,
                 final_text=text, kind="dev_update", ticket_id=ticket.id, note=note))
    db.commit()
