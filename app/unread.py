"""ข้อความลูกค้าที่ยังไม่ได้อ่าน (นับรายคน): ข้อความฝั่งลูกค้าที่ใหม่กว่าตำแหน่งที่พนักงานคนนั้นอ่านแชทถึง"""

from sqlalchemy import and_, func, select

from .database import ChatRead, Message, utcnow


def ensure_baseline(db, user_id: int) -> None:
    """ผู้ใช้ที่ยังไม่เคยมีตำแหน่งอ่านเลย (เช่น เพิ่งสร้างบัญชี) -> ข้อความเก่าทั้งหมดนับเป็นอ่านแล้ว นับเฉพาะที่เข้ามาหลังจากนี้"""
    if db.scalar(select(ChatRead.id).where(ChatRead.user_id == user_id).limit(1)) is not None:
        return
    latest = db.execute(select(Message.chat_id, func.max(Message.id)).group_by(Message.chat_id)).all()
    if latest:
        db.add_all([ChatRead(user_id=user_id, chat_id=c, last_id=m or 0) for c, m in latest])
        db.commit()


def counts(db, user_id: int, chat_ids=None) -> dict[int, int]:
    """chat_id -> จำนวนข้อความลูกค้าที่ยังไม่ได้อ่าน (เฉพาะแชทที่มี)"""
    ensure_baseline(db, user_id)
    query = (
        select(Message.chat_id, func.count(Message.id))
        .select_from(Message)
        .outerjoin(ChatRead, and_(ChatRead.chat_id == Message.chat_id, ChatRead.user_id == user_id))
        .where(Message.is_outgoing.is_(False), Message.id > func.coalesce(ChatRead.last_id, 0))
        .group_by(Message.chat_id)
    )
    if chat_ids is not None:
        ids = list(chat_ids)
        if not ids:
            return {}
        query = query.where(Message.chat_id.in_(ids))
    return {chat: n for chat, n in db.execute(query).all()}


def total(db, user_id: int) -> int:
    return sum(counts(db, user_id).values())


def mark_read(db, user_id: int, chat_id: int, upto: int | None = None) -> int:
    """ทำเครื่องหมายว่าอ่านแชทนี้ถึงข้อความ upto (ว่าง = ล่าสุดตอนนี้) · ไม่ย้อนกลับ · คืนค่าตำแหน่งที่อ่านถึง"""
    ensure_baseline(db, user_id)
    newest = db.scalar(select(func.max(Message.id)).where(Message.chat_id == chat_id)) or 0
    target = newest if upto is None else min(max(int(upto), 0), newest)
    row = db.scalar(select(ChatRead).where(ChatRead.user_id == user_id, ChatRead.chat_id == chat_id))
    if row is None:
        db.add(ChatRead(user_id=user_id, chat_id=chat_id, last_id=target))
    elif target > row.last_id:
        row.last_id, row.updated_at = target, utcnow()
    db.commit()
    return max(target, row.last_id if row else 0)