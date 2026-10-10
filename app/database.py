import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

from .config import DATABASE_URL

IS_SQLITE = DATABASE_URL.startswith("sqlite")
engine = create_engine(
    DATABASE_URL,
    # timeout = รอ lock ของ SQLite สูงสุดกี่วินาที (ค่าเดิม 5 วินาที ระหว่างรอทั้งเว็บจะค้าง)
    connect_args={"check_same_thread": False, "timeout": 2} if IS_SQLITE else {},
)


if IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):
        # WAL: อ่านและเขียนพร้อมกันได้ ไม่บล็อกกัน / NORMAL: เขียนเร็วขึ้นแต่ยังปลอดภัยกับ WAL
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=2000")
        cur.close()
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def utcnow() -> datetime:
    """เก็บเวลาเป็น UTC แบบ naive ในฐานข้อมูล"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True)
    password_hash: Mapped[str] = mapped_column(String(256))
    # admin = ทุกอย่าง, agent = อนุมัติข้อความ + ticket, programmer = ticket อย่างเดียว
    role: Mapped[str] = mapped_column(String(16), default="agent")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="")


class TelegramAccount(Base):
    """บัญชี Telegram ของผู้ใช้ (มีได้บัญชีเดียวต่อระบบ, id = 1)"""

    __tablename__ = "telegram_account"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    api_hash_enc: Mapped[str] = mapped_column(Text, default="")
    phone: Mapped[str] = mapped_column(String(32), default="")
    session_enc: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="disconnected")
    me_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    me_name: Mapped[str] = mapped_column(String(128), default="")
    last_error: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Chat(Base):
    __tablename__ = "chats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)  # Telegram chat id (marked)
    title: Mapped[str] = mapped_column(String(256), default="")
    kind: Mapped[str] = mapped_column(String(16), default="group")  # group / channel / user
    monitored: Mapped[bool] = mapped_column(Boolean, default=False)
    # เว็บไซต์ของลูกค้าแชทนี้ (แอดมินตั้งเอง) ใช้แทนการให้ AI เดา
    website_url: Mapped[str] = mapped_column(String(1024), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    tg_message_id: Mapped[int] = mapped_column(Integer)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    sender_name: Mapped[str] = mapped_column(String(256), default="")
    is_outgoing: Mapped[bool] = mapped_column(Boolean, default=False)
    text: Mapped[str] = mapped_column(Text, default="")
    media_path: Mapped[str] = mapped_column(String(512), default="")  # ชื่อไฟล์ใน data/media
    sent_by: Mapped[str] = mapped_column(String(64), default="", server_default="")  # พนักงานที่กดส่งจากเว็บ/LINE (ว่าง = ไม่ทราบ)
    date: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    analyzed: Mapped[bool] = mapped_column(Boolean, default=False)


class ChatRead(Base):
    """ตำแหน่งที่พนักงานแต่ละคนอ่านแชทถึง (Message.id ล่าสุดที่อ่านแล้ว) ใช้นับข้อความลูกค้าที่ยังไม่ได้อ่านรายคน"""

    __tablename__ = "chat_reads"
    __table_args__ = (UniqueConstraint("user_id", "chat_id", name="uq_chat_reads_user_chat"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    last_id: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Ticket(Base):
    __tablename__ = "tickets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    category: Mapped[str] = mapped_column(String(32), default="other")
    title: Mapped[str] = mapped_column(String(256), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    severity: Mapped[str] = mapped_column(String(16), default="medium")
    status: Mapped[str] = mapped_column(String(16), default="open")
    customer_name: Mapped[str] = mapped_column(String(256), default="")
    website_url: Mapped[str] = mapped_column(String(1024), default="")
    site_check: Mapped[str] = mapped_column(Text, default="")  # JSON ผลตรวจเว็บไซต์ล่าสุด
    assignee_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    # การส่งเข้ากลุ่มโปรแกรมเมอร์: "" ยังไม่ส่ง / pending รออนุมัติ / sent ส่งแล้ว / skipped แอดมินเลือกไม่ส่ง
    dev_status: Mapped[str] = mapped_column(String(16), default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    assignee: Mapped[User | None] = relationship()
    events: Mapped[list["TicketEvent"]] = relationship(
        back_populates="ticket", order_by="TicketEvent.created_at", cascade="all, delete-orphan"
    )
    attachments: Mapped[list["TicketAttachment"]] = relationship(
        back_populates="ticket", cascade="all, delete-orphan"
    )


class TicketEvent(Base):
    __tablename__ = "ticket_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticket_id: Mapped[int] = mapped_column(ForeignKey("tickets.id"), index=True)
    kind: Mapped[str] = mapped_column(String(24))  # customer_message / ai_summary / note / status
    author: Mapped[str] = mapped_column(String(128), default="")
    body: Mapped[str] = mapped_column(Text, default="")
    media_path: Mapped[str] = mapped_column(String(512), default="", server_default="")  # รูปที่ลูกค้าส่งมากับข้อความ
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    ticket: Mapped[Ticket] = relationship(back_populates="events")


class AiUsage(Base):
    """บันทึกการเรียก AI ทุกครั้ง (ใช้นับโควตาที่เหลือ)"""

    __tablename__ = "ai_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    model: Mapped[str] = mapped_column(String(64), index=True)
    ok: Mapped[bool] = mapped_column(Boolean, default=True)
    code: Mapped[str] = mapped_column(String(16), default="ok")  # ok / 429 / 503 / ...
    key_slot: Mapped[int] = mapped_column(Integer, default=1, server_default="1")  # คีย์ Gemini ลำดับที่ (1 = หลัก)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)


class TicketLink(Base):
    """ข้อความในกลุ่มโปรแกรมเมอร์ที่ผูกกับ ticket (ใช้รู้ว่าโปรแกรมเมอร์ตอบเรื่อง ticket ไหน)"""

    __tablename__ = "ticket_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticket_id: Mapped[int] = mapped_column(ForeignKey("tickets.id"), index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    tg_message_id: Mapped[int] = mapped_column(Integer, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class TicketAttachment(Base):
    __tablename__ = "ticket_attachments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticket_id: Mapped[int] = mapped_column(ForeignKey("tickets.id"), index=True)
    media_path: Mapped[str] = mapped_column(String(512))
    caption: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    ticket: Mapped[Ticket] = relationship(back_populates="attachments")


class Guide(Base):
    """คู่มือการใช้งานหลังบ้าน ให้ AI ใช้ตอบคำถามลูกค้า (ไม่ใช่การแจ้งปัญหา)"""

    __tablename__ = "guides"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(255))  # คำถาม / หัวข้อ
    keywords: Mapped[str] = mapped_column(Text, default="")  # คำที่ลูกค้ามักใช้ถาม คั่นด้วยจุลภาค
    answer: Mapped[str] = mapped_column(Text, default="")  # คำตอบ / ขั้นตอน / ลิงก์คู่มือ
    images: Mapped[str] = mapped_column(Text, default="", server_default="")  # JSON รายชื่อไฟล์รูปประกอบใน MEDIA_DIR
    used_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_by: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


def guide_images(guide) -> list[str]:
    try:
        return [x for x in json.loads(guide.images or "[]") if isinstance(x, str)]
    except (ValueError, TypeError):
        return []


class GuideQuestion(Base):
    """คำถามการใช้งานที่ลูกค้าถามแต่ยังไม่มีในคู่มือ (ให้แอดมินเพิ่มคู่มือภายหลัง)"""

    __tablename__ = "guide_questions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    question: Mapped[str] = mapped_column(Text)
    asked_by: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)  # open / added / dismissed
    guide_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Reply(Base):
    """ข้อความตอบกลับที่ AI ร่างไว้ รอแอดมินอนุมัติก่อนส่ง"""

    __tablename__ = "replies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    reply_to_tg_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ai_text: Mapped[str] = mapped_column(Text, default="")
    final_text: Mapped[str] = mapped_column(Text, default="")
    note: Mapped[str] = mapped_column(Text, default="")  # เหตุผล/บันทึกจาก AI ถึงแอดมิน
    media: Mapped[str] = mapped_column(Text, default="", server_default="")  # JSON รูปที่จะส่งพร้อมข้อความ (เช่น รูปจากคู่มือ)
    # ai = ร่างคำตอบจาก AI, resolved = แจ้งลูกค้าว่าแก้ไขปัญหาเรียบร้อยแล้ว
    kind: Mapped[str] = mapped_column(String(16), default="ai", server_default="ai")
    # pending / sending / sent / rejected / failed / superseded
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    ticket_id: Mapped[int | None] = mapped_column(ForeignKey("tickets.id"), nullable=True)
    decided_by: Mapped[str] = mapped_column(String(64), default="")
    decided_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    reject_reason: Mapped[str] = mapped_column(Text, default="")
    error: Mapped[str] = mapped_column(Text, default="")
    # JSON รายการคำสั่งที่แอดมินสั่งให้ AI เขียนใหม่ ["สั้นลง", "ขอสลิปด้วย", ...] ใช้เป็นสัญญาณให้ AI เรียนรู้
    edit_log: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class AiNote(Base):
    """โน้ตความจำของ AI: บทเรียนที่ได้จากการทำงานจริง (AI จดเอง หรือแอดมินเขียน) แล้วนำกลับไปใช้ตอนวิเคราะห์"""
    __tablename__ = "ai_notes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(8), default="global", server_default="global")  # global / chat
    chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(16), default="lesson", server_default="lesson")  # lesson/style/fact/pattern/warning
    title: Mapped[str] = mapped_column(String(160), default="")
    body: Mapped[str] = mapped_column(Text, default="")
    source: Mapped[str] = mapped_column(String(8), default="auto", server_default="auto")  # auto = AI จดเอง / manual = แอดมิน
    status: Mapped[str] = mapped_column(String(8), default="active", server_default="active", index=True)  # active / disabled
    pinned: Mapped[bool] = mapped_column(Boolean, default=False)  # ปักหมุด = ใช้เสมอ และ AI ห้ามแก้/ปิดเอง
    confidence: Mapped[int] = mapped_column(Integer, default=3, server_default="3")  # 1-5
    uses: Mapped[int] = mapped_column(Integer, default=0, server_default="0")  # ถูกนำไปใช้วิเคราะห์กี่ครั้ง
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    evidence: Mapped[str] = mapped_column(Text, default="")  # ที่มา เช่น "ร่าง #12 แอดมินแก้ · ticket #7"
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class LearnRun(Base):
    """ประวัติรอบที่ AI เรียนรู้ (แสดงในหน้า "สมอง AI")"""
    __tablename__ = "learn_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    trigger: Mapped[str] = mapped_column(String(8), default="auto")  # auto / manual
    model: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(8), default="ok")  # ok / skip / error
    signals: Mapped[int] = mapped_column(Integer, default=0)
    added: Mapped[int] = mapped_column(Integer, default=0)
    updated: Mapped[int] = mapped_column(Integer, default=0)
    disabled: Mapped[int] = mapped_column(Integer, default=0)
    ticket_notes: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[str] = mapped_column(Text, default="")


DEFAULT_SETTINGS = {
    "ai_model": "gemini-3.8-flash",
    "ai_effort": "medium",
    "gemini_limits": "",  # JSON เพดานโควตาต่อรุ่น {"model": {"rpm": n, "rpd": n}} ว่าง = ใช้ค่าเริ่มต้น
    "business_context": (
        "เราเป็นทีมซัพพอร์ตของเว็บไซต์ให้บริการลูกค้า ลูกค้าจะทักเข้ามาในกลุ่ม Telegram "
        "เพื่อสอบถามหรือแจ้งปัญหา เช่น เว็บไซต์เข้าไม่ได้ เข้าสู่ระบบไม่ได้ ฝากเงินแล้วยอดไม่เข้าอัตโนมัติ"
    ),
    "knowledge_base": (
        "- ถ้าฝากเงินแล้วยอดไม่เข้า ให้ขอสลิปการโอน และยูสเซอร์ของลูกค้า\n"
        "- ถ้าเข้าสู่ระบบไม่ได้ ให้ขอยูสเซอร์ และภาพหน้าจอข้อความ error\n"
        "- ถ้าเว็บไซต์เข้าไม่ได้ ให้ขอลิงก์ที่ลูกค้าใช้ และภาพหน้าจอ"
    ),
    "reply_style": "สุภาพ เป็นกันเอง กระชับ ลงท้ายด้วย ค่ะ",
    "debounce_seconds": "15",
    "context_messages": "20",
    "auto_draft": "1",
    "auto_ticket": "1",
    "site_check": "1",
    "ask_link": "1",
    "ack_info": "1",  # ตอบรับเมื่อลูกค้าส่งข้อมูลที่ทีมงานขอ (ลิงก์ ยูส สลิป)
    "notify_resolved": "1",
    "resolved_message": (
        "สวัสดีค่ะ คุณ{customer} ปัญหา \"{title}\" ที่แจ้งไว้ ทีมงานได้แก้ไขเรียบร้อยแล้วค่ะ "
        "รบกวนลองใช้งานอีกครั้ง หากยังพบปัญหาแจ้งทีมงานได้เลยนะคะ ขอบคุณค่ะ"
    ),
    # กลุ่มโปรแกรมเมอร์ (เช่น Autopay Support): ส่ง ticket เข้ากลุ่ม และอ่านข้อความโปรแกรมเมอร์
    "dev_group_id": "",
    "dev_usernames": "yuopa9",
    "bank_usernames": "",  # ทีมงานที่ดูแลเรื่องเชื่อมบัญชีธนาคาร (แท็กในกลุ่มแทนโปรแกรมเมอร์)
    "staff_usernames": "",  # ทีมงานคนอื่นในกลุ่มลูกค้า คั่นด้วยจุลภาค
    "ignore_usernames": "nsbmw_prod_bot",  # บัญชีที่ไม่รับข้อความเลย คั่นด้วยจุลภาค
    "ignore_bots": "1",  # ไม่รับข้อความจากบอท Telegram ทุกตัว
    "dev_forward": "1",
    "dev_require_approval": "1",  # ต้องอนุมัติก่อนส่ง ticket เข้ากลุ่มโปรแกรมเมอร์
    "dev_watch": "1",
    "line_enabled": "0",   # ส่งการ์ดรออนุมัติเข้า LINE
    "line_target": "",     # ปลายทาง LINE (จับอัตโนมัติเมื่อมีคนทัก/เพิ่ม OA)
    "line_approve": "1",   # อนุญาตให้กดอนุมัติจากปุ่มใน LINE
    "line_allowed": "[]",  # JSON รายชื่อผู้ที่กดอนุมัติ/ไม่ส่งจาก LINE ได้ [{"id": LINE userId, "name": ..., "added": ...}]
    "line_pair": "",       # รหัสเชื่อมผู้อนุมัติที่รอใช้งาน "รหัส|หมดอายุ|จำนวนครั้งที่ใส่ผิด" (ว่าง = ไม่มี)
    # สมอง AI: ให้ AI เรียนรู้จากการทำงานจริงแล้วจดโน้ตไว้ใช้ครั้งต่อไป
    "auto_learn": "1",                        # เรียนรู้และจดโน้ตอัตโนมัติเป็นรอบๆ
    "learn_use_notes": "1",                   # นำโน้ตไปใช้ตอนวิเคราะห์/ร่างคำตอบ
    "learn_ticket_notes": "1",                # เขียนโน้ต "สรุปการแก้ไข" ใส่ ticket ที่แก้เสร็จแล้วอัตโนมัติ
    "learn_model": "gemini-3.5-flash-lite",   # โมเดลที่ใช้เรียนรู้ (แยกจากโมเดลตอบลูกค้า จะได้ไม่แย่งโควตา)
    "learn_interval_hours": "6",              # เรียนรู้ทุกกี่ชั่วโมง
    "learn_min_signals": "3",                 # ต้องมีสัญญาณใหม่อย่างน้อยกี่รายการถึงจะเรียนรู้ (ประหยัดโควตา)
    "learn_max_notes": "150",                 # จำนวนโน้ตที่ใช้งานสูงสุด (เกินแล้วปิดโน้ตที่ใช้น้อย/มั่นใจต่ำ)
    "learn_cursor": "",                       # JSON จุดที่เรียนรู้ถึงแล้ว {"reply_id", "ticket_ts", "last_try", "last_ok"}
    # เชื่อมต่อ AI ภายนอกผ่าน API รูปแบบ OpenAI (API key อยู่ที่ตัวแปร CUSTOM_AI_API_KEY เท่านั้น ไม่เก็บในฐานข้อมูล)
    "custom_ai_name": "AI ภายนอก",            # ชื่อผู้ให้บริการที่แสดงหน้าเว็บ
    "custom_ai_base_url": "",                 # เช่น https://api.example.com/v1
    "custom_ai_models": "",                   # รายชื่อโมเดล บรรทัดละ 1 รุ่น ("id | ชื่อที่แสดง")
    "custom_ai_json_mode": "auto",            # auto / schema / object / prompt = วิธีบังคับให้ตอบเป็น JSON
    "custom_ai_vision": "no",                 # no / auto / yes = ส่งรูปให้โมเดลไหม (ค่าเริ่มต้นไม่ส่ง: รูปสลิป/หน้าจอปกปิดไม่ได้)
    "custom_ai_mask": "on",                   # on / off = ปกปิดเบอร์ อีเมล ลิงก์ เลขบัญชี ชื่ออื่นๆ ก่อนส่งให้ AI ภายนอก (privacy.py)
    "custom_ai_mask_level": "strict",         # strict = ไม่ส่งข้อมูลธุรกิจ/ฐานความรู้/คู่มือ/บทเรียน + ปกปิดจำนวนเงินและชื่อแบรนด์ด้วย · standard = ปกปิดเฉพาะข้อมูลส่วนตัว
    "custom_ai_mask_terms": "",               # คำที่ต้องปกปิดเพิ่ม บรรทัดละ 1 คำ (เช่น ชื่อแบรนด์ ชื่อระบบภายใน)
    "custom_ai_max_tokens": "0",              # 0 = ไม่กำหนด (บางเจ้าไม่รับพารามิเตอร์นี้)
    "custom_ai_timeout": "120",               # รอคำตอบสูงสุดกี่วินาที
    "custom_ai_caps": "",                     # JSON ผลทดสอบความสามารถของแต่ละโมเดล
}


def _add_missing_columns() -> None:
    """ฐานข้อมูลเก่า (อยู่ใน Volume) จะไม่มีคอลัมน์ที่เพิ่มทีหลัง -> เพิ่มให้อัตโนมัติ"""
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table in Base.metadata.sorted_tables:
            if not inspector.has_table(table.name):
                continue
            existing = {c["name"] for c in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in existing:
                    continue
                ddl = f'ALTER TABLE "{table.name}" ADD COLUMN "{column.name}" {column.type.compile(engine.dialect)}'
                if column.server_default is not None:
                    ddl += f" DEFAULT '{column.server_default.arg}'"
                conn.execute(text(ddl))


def backfill_sent_by() -> int:
    """ข้อความทีมงานที่ส่งผ่านเว็บ/LINE ก่อนจะมีการบันทึกชื่อผู้ส่ง -> จับคู่กับร่างที่อนุมัติ
    (แชทเดียวกัน ข้อความตรงกัน เวลาห่างกันไม่กี่นาที) แล้วใส่ชื่อผู้อนุมัติให้ · คืนค่าจำนวนที่แก้"""
    fixed = 0
    with SessionLocal() as db:
        replies = db.scalars(select(Reply).where(Reply.status == "sent", Reply.decided_by != "", Reply.decided_at.is_not(None)))
        for r in list(replies):
            text = (r.final_text or "").strip()
            if not text:
                continue
            m = db.scalar(select(Message).where(
                Message.chat_id == r.chat_id, Message.is_outgoing.is_(True), Message.sent_by == "", Message.text == text,
                Message.date >= r.decided_at - timedelta(minutes=2), Message.date <= r.decided_at + timedelta(minutes=15),
            ).order_by(Message.date).limit(1))
            if m is not None:
                m.sent_by = r.decided_by[:64]
                fixed += 1
        db.commit()
    return fixed


def merge_usernames(current: str, extra: list[str]) -> str:
    """รวมรายชื่อ username (คั่นด้วยจุลภาค/บรรทัดใหม่) ไม่ซ้ำ (ไม่สนตัวพิมพ์ ไม่สน @) โดยคงของเดิมไว้ก่อน"""
    items = [x.strip() for x in (current or "").replace("\n", ",").split(",") if x.strip()]
    seen = {x.lstrip("@").lower() for x in items}
    for name in extra:
        if name.lstrip("@").lower() not in seen:
            items.append(name)
            seen.add(name.lstrip("@").lower())
    return ", ".join(items)


def init_db() -> None:
    Base.metadata.create_all(engine)
    _add_missing_columns()
    with SessionLocal() as db:
        for key, value in DEFAULT_SETTINGS.items():
            if db.get(Setting, key) is None:
                db.add(Setting(key=key, value=value))
        # ครั้งเดียว: เดิมค่าเริ่มต้นของ AI ภายนอกคือส่งรูปอัตโนมัติ -> เปลี่ยนเป็นไม่ส่ง (รูปสลิปปกปิดข้อมูลไม่ได้) ยกเว้นแอดมินเลือก "ส่งเสมอ" ไว้เอง
        if db.get(Setting, "custom_ai_privacy_v1") is None:
            row = db.get(Setting, "custom_ai_vision")
            if row is not None and row.value == "auto":
                row.value = "no"
            db.add(Setting(key="custom_ai_privacy_v1", value="1"))
        # ครั้งเดียว: เพิ่มแอดมินกลุ่มลูกค้าเข้ารายชื่อทีมงาน (รวมกับที่ตั้งไว้เดิม ไม่ทับ · ลบออกที่หน้าตั้งค่าได้ ไม่ถูกเติมซ้ำ)
        if db.get(Setting, "staff_usernames_v1") is None:
            row = db.get(Setting, "staff_usernames")
            row.value = merge_usernames(row.value if row is not None else "", ["Prime2499", "autosupportway"])
            db.add(Setting(key="staff_usernames_v1", value="1"))
        # ครั้งเดียว: ข้อความเก่าทั้งหมดนับเป็น "อ่านแล้ว" ของผู้ใช้ที่มีอยู่ (นับเฉพาะข้อความใหม่หลังจากนี้)
        if db.get(Setting, "chat_reads_v1") is None:
            db.flush()
            latest = db.execute(select(Message.chat_id, func.max(Message.id)).group_by(Message.chat_id)).all()
            for uid in list(db.scalars(select(User.id))):
                db.add_all([ChatRead(user_id=uid, chat_id=c, last_id=m or 0) for c, m in latest])
            db.add(Setting(key="chat_reads_v1", value="1"))
        if db.get(TelegramAccount, 1) is None:
            db.add(TelegramAccount(id=1))
        db.commit()


def int_setting(settings: dict[str, str], key: str, low: int, high: int) -> int:
    """อ่านค่าตัวเลขจากตั้งค่า ถ้าผิดรูปแบบใช้ค่าเริ่มต้น และบังคับให้อยู่ในช่วง"""
    try:
        value = int(settings.get(key) or DEFAULT_SETTINGS[key])
    except ValueError:
        value = int(DEFAULT_SETTINGS[key])
    return max(low, min(high, value))


def get_settings(db) -> dict[str, str]:
    values = dict(DEFAULT_SETTINGS)
    for row in db.scalars(select(Setting)):
        values[row.key] = row.value
    return values
