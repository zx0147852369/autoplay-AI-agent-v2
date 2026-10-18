import asyncio
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from urllib.parse import urlparse

from fastapi import FastAPI, File, Form, Request, UploadFile, WebSocket
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape
from pydantic import BaseModel
from sqlalchemy import func, select
from starlette.middleware.sessions import SessionMiddleware

from . import ai_service, analyzer, announce, chatbus, dev_bridge, learning, line_service, linebot, quota, sysinfo, unread
from .config import ADMIN_PASSWORD, ADMIN_USERNAME, DATA_DIR, DISPLAY_TZ, EPHEMERAL_STORAGE, MEDIA_DIR, SECRET_KEY
from .database import (
    DEFAULT_SETTINGS,
    Announcement,
    AnnouncementTarget,
    AiNote,
    AiUsage,
    Chat,
    Guide,
    GuideQuestion,
    guide_images,
    LearnRun,
    LineContent,
    LineQuestion,
    Message,
    Reply,
    SessionLocal,
    Setting,
    TelegramAccount,
    Ticket,
    TicketAttachment,
    TicketEvent,
    TicketLink,
    User,
    backfill_sent_by,
    get_settings,
    init_db,
    int_setting,
    utcnow,
)
from .notices import sync_resolved_notice
from .security import decrypt, hash_password, verify_password
from .telegram_service import TelegramLoginError, telegram

# คอนโซล Windows ไม่ใช่ UTF-8 โดยค่าเริ่มต้น ทำให้ log ภาษาไทยพัง
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
from . import logbuf
logbuf.install()
log = logging.getLogger("app")

APP_DIR = Path(__file__).resolve().parent
ROLES = {"admin": "ผู้ดูแลระบบ", "agent": "แอดมินตอบแชท", "programmer": "โปรแกรมเมอร์"}
TICKET_STATUSES = {"open": "เปิดใหม่", "in_progress": "กำลังแก้ไข", "resolved": "แก้ไขแล้ว", "closed": "ปิด"}
SEVERITY_LABELS = {"low": "ต่ำ", "medium": "ปานกลาง", "high": "สูง", "critical": "วิกฤต"}
REPLY_STATUSES = {"pending": "รออนุมัติ", "sent": "ส่งแล้ว", "rejected": "ปฏิเสธ",
                  "failed": "ส่งไม่สำเร็จ", "superseded": "ถูกแทนที่", "sending": "กำลังส่ง"}


def ensure_admin() -> None:
    """ADMIN_USERNAME / ADMIN_PASSWORD ใน env คือบัญชีผู้ดูแลหลัก: สร้างถ้ายังไม่มี และรีเซ็ตรหัสให้ตรงกับ env
    ทุกครั้งที่เปิดโปรแกรม (ใช้กู้บัญชีได้ด้วยการเปลี่ยน ADMIN_PASSWORD แล้วรีสตาร์ท)"""
    if not ADMIN_PASSWORD:
        log.warning("ยังไม่ได้ตั้งค่า ADMIN_PASSWORD: ตั้งค่าใน .env / Variables แล้วรีสตาร์ท")
        return
    with SessionLocal() as db:
        user = db.scalar(select(User).where(User.username == ADMIN_USERNAME))
        if user is None:
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password(ADMIN_PASSWORD), role="admin"))
            log.info("สร้างผู้ใช้ผู้ดูแลระบบ '%s' แล้ว", ADMIN_USERNAME)
        elif not verify_password(ADMIN_PASSWORD, user.password_hash) or user.role != "admin":
            user.password_hash, user.role = hash_password(ADMIN_PASSWORD), "admin"
            log.info("อัปเดตรหัสผ่านผู้ดูแลระบบ '%s' ตาม ADMIN_PASSWORD แล้ว", ADMIN_USERNAME)
        db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    ensure_admin()
    with SessionLocal() as db:
        acc = db.get(TelegramAccount, 1)
        chatbus.set_me(acc.me_name if acc else "")
    stuck = announce.mark_interrupted()
    if stuck:
        log.warning("ประกาศที่ค้างสถานะกำลังส่ง %s รายการ ถูกหยุดไว้ (ระบบรีสตาร์ท) · กดส่งต่อได้ที่หน้า ประกาศ", stuck)
    fixed = backfill_sent_by()
    if fixed:
        log.info("ใส่ชื่อพนักงานที่กดส่งให้ข้อความเก่า %s ข้อความ", fixed)
    cleaned = fix_json_drafts()
    if cleaned:
        log.info("แก้ร่างคำตอบที่เป็น JSON ดิบ %s รายการให้เป็นข้อความปกติ", cleaned)
    log.info("เก็บข้อมูลที่ %s", DATA_DIR)
    if EPHEMERAL_STORAGE:
        log.warning("ข้อมูลไม่ได้อยู่ใน Railway Volume: ตั้งค่าและข้อมูลทั้งหมดจะหายเมื่อ deploy ใหม่")
    telegram.on_message = analyzer.schedule
    linebot.on_message = analyzer.schedule
    analyzer.on_draft = _auto_send_bot
    asyncio.create_task(_load_bot_name())
    dev_bridge.configure()
    startup = asyncio.create_task(telegram.start_from_db())
    sweeper = asyncio.create_task(analyzer.sweeper())
    learner = asyncio.create_task(learning.learner())  # สมอง AI: เรียนรู้เป็นรอบๆ เบื้องหลัง
    lag_watch = asyncio.create_task(_watch_loop_lag())
    yield
    startup.cancel()
    sweeper.cancel()
    learner.cancel()
    lag_watch.cancel()
    await telegram.stop()


app = FastAPI(title="Telegram AI Support", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, same_site="lax", max_age=60 * 60 * 12)

# ---------------------------------------------------------------- วัดความเร็ว (ใช้หาสาเหตุเว็บช้า)
_loop_lag = {"max": 0.0, "since": time.monotonic()}


async def _watch_loop_lag() -> None:
    """ทุก 0.5 วินาที วัดว่า event loop ค้างนานเท่าไร (ถ้ามีงานแบบ sync บล็อก ทั้งเว็บจะช้าตาม)"""
    while True:
        start = time.monotonic()
        await asyncio.sleep(0.5)
        lag = time.monotonic() - start - 0.5
        if time.monotonic() - _loop_lag["since"] > 60:
            _loop_lag.update(max=0.0, since=time.monotonic())
        _loop_lag["max"] = max(_loop_lag["max"], lag)
        if lag > 1:
            log.warning("event loop ค้าง %.1f วินาที", lag)


@app.middleware("http")
async def _timing(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    ms = (time.perf_counter() - started) * 1000
    # app = เวลาที่ระบบใช้จริง, lag = event loop ค้างสูงสุดใน 1 นาทีล่าสุด (ดูได้ใน DevTools > Network > Timing)
    response.headers["Server-Timing"] = f"app;dur={ms:.0f}, lag;dur={_loop_lag['max'] * 1000:.0f}"
    if ms > 1000:
        log.warning("ช้า %s %s %.0f ms", request.method, request.url.path, ms)
    return response


class CachedStatic(StaticFiles):
    """ไฟล์ static ให้เบราว์เซอร์แคชไว้ ไม่ต้องถามเซิร์ฟเวอร์ซ้ำทุกครั้งที่เปลี่ยนหน้า
    (เซิร์ฟเวอร์อยู่ไกล ทุกคำขอ 304 เสียเวลาไป-กลับ ~250 ms · หน้าเดียวมีไอคอนกว่า 25 ไฟล์)
    ลิงก์ที่มี ?v=เลขเวอร์ชัน (style.css) แคชถาวร เปลี่ยนเลขเมื่อแก้ไฟล์ · ไฟล์อื่น (ไอคอน โลโก้) แคช 7 วัน"""

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if response.status_code in (200, 304):
            versioned = b"v=" in scope.get("query_string", b"")
            response.headers["Cache-Control"] = ("public, max-age=31536000, immutable" if versioned
                                                 else "public, max-age=604800, stale-while-revalidate=86400")
        return response


app.mount("/static", CachedStatic(directory=APP_DIR / "static"), name="static")


# เบราว์เซอร์ขอไอคอนที่ราก (/favicon.ico) เองเมื่อเปิดหน้าที่ไม่ใช่ HTML, บุ๊กมาร์ก หรือประวัติ -> ตอบด้วยโลโก้ ไม่ใช่ 404
def _brand_file(name: str, media_type: str):
    path = APP_DIR / "static" / "brand" / name
    return FileResponse(path, media_type=media_type, headers={"Cache-Control": "public, max-age=604800"})


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return _brand_file("favicon.ico", "image/x-icon")


@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
async def apple_touch_icon():
    return _brand_file("mark-180.png", "image/png")
templates = Jinja2Templates(directory=APP_DIR / "templates")


def _localtime(dt):
    if not dt:
        return "-"
    return dt.replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ).strftime("%d/%m/%Y %H:%M")


THAI_DAYS = ["จันทร์", "อังคาร", "พุธ", "พฤหัสบดี", "ศุกร์", "เสาร์", "อาทิตย์"]
THAI_MONTHS = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน", "กรกฎาคม",
               "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]


def thai_today() -> str:
    d = utcnow().replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ)
    return f"วัน{THAI_DAYS[d.weekday()]}ที่ {d.day} {THAI_MONTHS[d.month - 1]} {d.year + 543}"


def _iso_localtime(value: str) -> str:
    try:
        return _localtime(datetime.fromisoformat(value))
    except (TypeError, ValueError):
        return "-"


templates.env.filters["localtime"] = _localtime
templates.env.filters["localtime_hm"] = lambda dt: dt.replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ).strftime("%H:%M") if dt else "-"
templates.env.filters["isolocal"] = _iso_localtime

_TH_MONTHS = ["ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]


def _ago(dt) -> str:
    """เวลาแบบอ่านง่ายในรายการ: เมื่อสักครู่ / 5 นาทีที่แล้ว / วันนี้ 13:05 / เมื่อวาน 09:30 / 8 ต.ค. 17:20"""
    if not dt:
        return "-"
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    secs = (utcnow() - dt).total_seconds()
    if secs < 60:
        return "เมื่อสักครู่"
    if secs < 3600:
        return f"{int(secs // 60)} นาทีที่แล้ว"
    local = dt.replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ)
    today = utcnow().replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ).date()
    if local.date() == today:
        return f"วันนี้ {local:%H:%M}"
    if local.date() == today - timedelta(days=1):
        return f"เมื่อวาน {local:%H:%M}"
    return f"{local.day} {_TH_MONTHS[local.month - 1]} {local:%H:%M}"


templates.env.filters["ago"] = _ago

templates.env.filters["short_name"] = chatbus.short_name
templates.env.globals["msg_label"] = chatbus.msg_label  # ชื่อที่แสดงบนข้อความ (ฝั่งทีมงาน = พนักงานที่กดส่ง)
templates.env.filters["fromjson"] = lambda s: json.loads(s) if s else None

_URL_RE = re.compile(r"(https?://[^\s<>\"']+)")


def _linkify(text: str) -> Markup:
    """ข้อความธรรมดา -> HTML ที่ escape แล้ว และทำลิงก์ให้กดได้"""
    parts = _URL_RE.split(text or "")
    out = []
    for i, part in enumerate(parts):
        if i % 2:
            url = part.rstrip(".,;)")
            tail = part[len(url):]
            out.append(Markup('<a href="{0}" target="_blank" rel="noopener noreferrer">{0}</a>').format(url) + escape(tail))
        else:
            out.append(escape(part))
    return Markup("").join(out)


templates.env.filters["linkify"] = _linkify


def _mask_phone(phone: str) -> str:
    """ซ่อนเบอร์โทรตรงกลาง เช่น +66633717388 -> +66 ••••• 388"""
    phone = (phone or "").strip()
    digits = phone.lstrip("+")
    if len(digits) < 6:
        return "•" * len(phone)
    prefix = ("+" if phone.startswith("+") else "") + digits[:2]
    return f"{prefix} {'•' * (len(digits) - 5)} {digits[-3:]}"


templates.env.filters["mask_phone"] = _mask_phone

TH_DAYS = ["จันทร์", "อังคาร", "พุธ", "พฤหัสบดี", "ศุกร์", "เสาร์", "อาทิตย์"]
TH_MONTHS = ["ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]
NAME_COLORS = ["#0b7d52", "#1d4ed8", "#a15c07", "#9d174d", "#6d28d9", "#0e7490", "#b42318", "#4d7c0f"]
_SENDER_RE = re.compile(r"^(.*?)\s*\(@([\w\d_]+)\)\s*$")


def build_chat_view(messages: list[Message]) -> list[dict]:
    """จัดข้อความเป็นแบบแอปแชท: คั่นวัน, รวมข้อความติดกันของคนเดียวกัน, สีชื่อคงที่ต่อคน"""
    today = datetime.now(DISPLAY_TZ).date()
    items, prev = [], None
    for m in messages:
        local = m.date.replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ)
        day = local.date()
        match = _SENDER_RE.match(m.sender_name or "")
        name, username = (match.group(1), match.group(2)) if match else ((m.sender_name or "").strip(), "")
        name = name or (username and "@" + username) or ("ทีมงาน" if m.is_outgoing else "ลูกค้า")
        if m.is_outgoing:
            name = chatbus.msg_label(m)  # พนักงานที่กดส่งจากเว็บ/LINE > ชื่อใน Telegram > ชื่อบัญชีที่เชื่อมต่อ
        key = (m.is_outgoing, m.sender_name, getattr(m, "sent_by", ""))
        new_day = prev is None or prev["day"] != day
        first = new_day or prev["key"] != key or (local - prev["local"]).total_seconds() > 300
        if new_day:
            label = "วันนี้" if day == today else "เมื่อวาน" if (today - day).days == 1 else \
                f"{TH_DAYS[day.weekday()]} {day.day} {TH_MONTHS[day.month - 1]} {day.year}"
        text = (m.text or "").strip()
        items.append({
            "m": m, "day": day, "day_label": label if new_day else "", "local": local, "key": key, "first": first,
            "name": name, "username": username, "initial": (name.lstrip("@") or "?")[:1].upper(),
            "color": NAME_COLORS[sum(map(ord, m.sender_name or name)) % len(NAME_COLORS)],
            "sticker": text == "(สติกเกอร์)", "text": text,
        })
        if prev is not None and first:
            prev["last"] = True
        prev = items[-1]
    if prev is not None:
        prev["last"] = True
    return items
templates.env.globals.update(
    CATEGORIES=ai_service.CATEGORIES, TICKET_STATUSES=TICKET_STATUSES, SEVERITY_LABELS=SEVERITY_LABELS,
    REPLY_STATUSES=REPLY_STATUSES, ROLES=ROLES, ENV_ADMIN=ADMIN_USERNAME, EPHEMERAL_STORAGE=EPHEMERAL_STORAGE,
    GEMINI_MODELS=ai_service.GEMINI_MODELS, CLAUDE_MODELS=ai_service.CLAUDE_MODELS,
)


# ---------------------------------------------------------------- auth helpers
class NeedLogin(Exception):
    pass


class Forbidden(Exception):
    pass


@app.exception_handler(NeedLogin)
async def _need_login(request: Request, exc: NeedLogin):
    if request.url.path.startswith("/api/"):  # เรียกจากสคริปต์ในหน้าเว็บ: ตอบ JSON ไม่เด้งไปหน้า login (HTML)
        return JSONResponse({"ok": False, "error": "เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่"}, status_code=401)
    return RedirectResponse("/login", status_code=303)


@app.exception_handler(Forbidden)
async def _forbidden(request: Request, exc: Forbidden):
    if request.url.path.startswith("/api/"):
        return JSONResponse({"ok": False, "error": "คุณไม่มีสิทธิ์ทำรายการนี้"}, status_code=403)
    flash(request, "คุณไม่มีสิทธิ์เข้าถึงหน้านี้", "error")
    # ทุกบทบาทเข้าหน้า Tickets ได้ แต่กันวนซ้ำไว้เผื่อหน้า Tickets เองไม่มีสิทธิ์
    target = "/account" if request.url.path.startswith("/tickets") else "/tickets"
    return RedirectResponse(target, status_code=303)


def current_user(request: Request, *roles: str) -> User:
    user_id = request.session.get("user_id")
    if not user_id:
        raise NeedLogin()
    with SessionLocal() as db:
        user = db.get(User, user_id)
    if not user:
        request.session.clear()
        raise NeedLogin()
    if roles and user.role not in roles and user.role != "admin":
        raise Forbidden()
    return user


def flash(request: Request, message: str, kind: str = "ok") -> None:
    # เก็บไว้แค่ 3 ข้อความล่าสุด กันคุกกี้ session ใหญ่เกินจนเบราว์เซอร์ทิ้ง
    request.session["flash"] = (request.session.get("flash", []) + [{"message": message, "kind": kind}])[-3:]


def render(request: Request, name: str, user: User | None, **context):
    nav_path = context.pop("nav_path", None)  # ใช้ไฮไลต์เมนูข้างเมื่อหน้านี้เป็นของอีกระบบ (เช่น รายละเอียดแชท LINE)
    context.update(user=user, flashes=request.session.pop("flash", []), path=nav_path or request.url.path)
    if user and "quota_info" not in context:
        try:
            with SessionLocal() as db:
                context["quota_info"] = quota.snapshot(get_settings(db))
        except Exception:  # noqa: BLE001 - หลอดโควตาพังต้องไม่ทำให้ทั้งหน้าพัง
            log.exception("quota snapshot failed")
    return templates.TemplateResponse(request, name, context)


def back(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)


# ---------------------------------------------------------------- login
@app.get("/login")
async def login_page(request: Request):
    # หน้า login แสดงข้อผิดพลาดในฟอร์มเลย แทนการเด้งเป็น toast
    flashes = request.session.pop("flash", [])
    return render(request, "login.html", None, need_setup=not ADMIN_PASSWORD,
                  login_errors=list(dict.fromkeys(f["message"] for f in flashes if f["kind"] == "error")))


# กันการเดารหัสผ่าน: ผิดเกิน LOGIN_MAX_FAILS ครั้งใน LOGIN_WINDOW วินาที ต่อ IP -> ต้องรอ
LOGIN_MAX_FAILS = 8
LOGIN_WINDOW = 10 * 60
_login_fails: dict[str, deque] = defaultdict(deque)


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    ip = request.client.host if request.client else "?"
    fails, now = _login_fails[ip], time.monotonic()
    while fails and now - fails[0] > LOGIN_WINDOW:
        fails.popleft()
    if len(fails) >= LOGIN_MAX_FAILS:
        wait = int(LOGIN_WINDOW - (now - fails[0])) // 60 + 1
        flash(request, f"ใส่รหัสผ่านผิดบ่อยเกินไป กรุณาลองใหม่ในอีก {wait} นาที", "error")
        return back("/login")
    with SessionLocal() as db:
        user = db.scalar(select(User).where(User.username == username.strip()))
    if not user or not verify_password(password, user.password_hash):
        fails.append(now)
        flash(request, "ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง", "error")
        return back("/login")
    fails.clear()
    request.session.clear()
    request.session["user_id"] = user.id
    return back("/" if user.role != "programmer" else "/tickets")


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return back("/login")


# ---------------------------------------------------------------- dashboard
def _line_ids():
    """subquery: id ของแชท LINE Chat Bot"""
    return select(Chat.id).where(Chat.channel == "line")


def _in_scope(column, scope: str):
    """เงื่อนไขกรองตามระบบ: scope="line" = เฉพาะ LINE Chat Bot · อื่นๆ = Telegram (ไม่รวม LINE) · ใช้กับคอลัมน์ chat_id"""
    cond = column.in_(_line_ids())
    return cond if scope == "line" else ~cond


def _chat_scope(chat_id: int) -> str:
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
    return "line" if chat and chat.channel == "line" else "telegram"


def _chats_home(chat_id: int) -> str:
    return "/linebot/chats" if _chat_scope(chat_id) == "line" else "/chats"


def _replies_home(reply_id: int) -> str:
    """หน้ารออนุมัติที่ควรกลับไปหลังตัดสินใจร่างนี้ (ร่างของ LINE กลับไปคิว LINE ไม่ปนกับ Telegram)"""
    with SessionLocal() as db:
        reply = db.get(Reply, reply_id)
        chat_id = reply.chat_id if reply else None
    return "/linebot/replies" if chat_id is not None and _chat_scope(chat_id) == "line" else "/replies"


def local_day_start_utc():
    """เวลาเริ่มต้นของ "วันนี้" ตามเวลาไทย แปลงเป็น UTC (naive) สำหรับค้นในฐานข้อมูล"""
    now_local = utcnow().replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ)
    start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc).replace(tzinfo=None)


def model_key_ready(model: str, settings: dict | None = None) -> bool:
    if ai_service.is_gemini(model):
        return bool(quota.gemini_keys())
    return bool(os.getenv("ANTHROPIC_API_KEY"))


@app.get("/")
async def dashboard(request: Request):
    user = current_user(request, "agent")
    day = local_day_start_utc()
    with SessionLocal() as db:
        settings = get_settings(db)
        account = db.get(TelegramAccount, 1)
        tg_reply = _in_scope(Reply.chat_id, "telegram")
        pending = db.scalar(select(func.count(Reply.id)).where(Reply.status == "pending", tg_reply)) + dev_bridge.pending_count(db)
        open_by_cat = dict(db.execute(
            select(Ticket.category, func.count(Ticket.id))
            .where(Ticket.status.in_(analyzer.OPEN_STATUSES)).group_by(Ticket.category)
        ).all())
        by_status = dict(db.execute(select(Ticket.status, func.count(Ticket.id)).group_by(Ticket.status)).all())
        recent = list(db.scalars(select(Ticket).order_by(Ticket.created_at.desc()).limit(6)))
        pending_list = list(db.scalars(
            select(Reply).where(Reply.status == "pending", tg_reply).order_by(Reply.created_at.desc()).limit(5)))
        monitored = db.scalar(select(func.count(Chat.id)).where(Chat.monitored, Chat.channel != "line"))
        chat_titles = {c.id: c.title for c in db.scalars(select(Chat))}
        dev_title = chat_titles.get(telegram.dev_group_id, "") if telegram.dev_group_id else ""
        today = {
            "messages": db.scalar(select(func.count(Message.id))
                                  .where(Message.date >= day, Message.is_outgoing.is_(False),
                                         _in_scope(Message.chat_id, "telegram"))),
            "tickets": db.scalar(select(func.count(Ticket.id)).where(Ticket.created_at >= day)),
            "resolved": db.scalar(select(func.count(Ticket.id))
                                  .where(Ticket.updated_at >= day, Ticket.status.in_(("resolved", "closed")))),
            "sent": db.scalar(select(func.count(Reply.id))
                              .where(Reply.status == "sent", Reply.decided_at >= day, tg_reply)),
        }
    model = settings.get("ai_model", "")
    model_label = ai_service.all_models(settings).get(model, model)
    key_ready = model_key_ready(model, settings)
    setup = [
        ("เชื่อมต่อบัญชี Telegram", telegram.connected, "/telegram"),
        ("เลือกแชทลูกค้าที่จะติดตาม", bool(monitored), "/chats"),
        ("ใส่ API key ของโมเดล AI", key_ready, "/settings"),
        ("กรอกข้อมูลธุรกิจและคำตอบมาตรฐาน",
         settings.get("knowledge_base") != DEFAULT_SETTINGS["knowledge_base"], "/settings"),
    ]
    system = [
        ("Telegram", telegram.connected, account.me_name or "ยังไม่ได้เชื่อมต่อ"),
        ("โมเดล AI", key_ready, model_label + ("" if key_ready else " · ยังไม่มี API key")
         + (f" · ตอนนี้ใช้รุ่นสำรอง {ai_service.last_model_used} (รุ่นหลักล่มชั่วคราว)"
            if ai_service.last_model_used and ai_service.last_model_used != model else "")
         + (f" · ตอนนี้ใช้{quota.slot_label(ai_service.last_key_slot)} (คีย์หลักโควตาเต็ม)"
            if ai_service.is_gemini(model) and ai_service.last_key_slot > 1 else "")),
        ("คีย์ Gemini", len(quota.gemini_keys()) > 1 or not ai_service.is_gemini(model),
         (f"{len(quota.gemini_keys())} คีย์ (หลัก + สำรอง {len(quota.gemini_keys()) - 1})" if len(quota.gemini_keys()) > 1
          else "คีย์เดียว ยังไม่มีคีย์สำรอง (เพิ่ม GEMINI_API_KEY_2 ใน Railway)" if quota.gemini_keys() else "ยังไม่ได้ตั้ง")),
        ("การเก็บข้อมูล", not EPHEMERAL_STORAGE, "ถาวร (Volume)" if not EPHEMERAL_STORAGE else "ชั่วคราว หายเมื่อ deploy"),
        ("กลุ่มโปรแกรมเมอร์", bool(dev_title) and not dev_bridge.last_error,
         dev_bridge.last_error or dev_title or "ยังไม่ได้เลือกกลุ่ม (ตั้งค่า → กลุ่มแจ้งปัญหาโปรแกรมเมอร์)"),
        ("ร่างคำตอบอัตโนมัติ", settings.get("auto_draft") == "1", "เปิด" if settings.get("auto_draft") == "1" else "ปิด"),
        ("เปิด ticket อัตโนมัติ", settings.get("auto_ticket") == "1", "เปิด" if settings.get("auto_ticket") == "1" else "ปิด"),
        ("ตรวจเว็บไซต์อัตโนมัติ", settings.get("site_check") == "1", "เปิด" if settings.get("site_check") == "1" else "ปิด"),
    ]
    return render(request, "dashboard.html", user, account=account, pending=pending, open_by_cat=open_by_cat,
                  by_status=by_status, recent=recent, pending_list=pending_list, monitored=monitored,
                  chat_titles=chat_titles, ai_errors=analyzer.last_error, connected=telegram.connected,
                  today=thai_today(), today_counts=today, setup=setup, system=system,
                  quota_info=quota.snapshot(settings),
                  setup_done=sum(1 for item in setup if item[1]))


@app.get("/logs")
async def logs_page(request: Request, level: str = "", day: str = ""):
    user = current_user(request, "admin")
    days = logbuf.available_days()
    if day and day in days:  # ดูไฟล์ย้อนหลังของวันนั้น (ไม่ใช่ live)
        recs = logbuf.read_day(day, level=level)
        return render(request, "logs.html", user, logs=recs, last=0, total=len(recs),
                      level=level, days=days, day=day, live=False)
    data = logbuf.records(level=level)
    return render(request, "logs.html", user, logs=data["records"], last=data["last"],
                  total=data["total"], level=level, days=days, day="", live=True)


@app.get("/api/logs")
async def api_logs(request: Request, after: int = 0, level: str = ""):
    current_user(request, "admin")
    return JSONResponse(logbuf.records(after=after, level=level))


@app.get("/logs/download")
async def logs_download(request: Request, day: str = ""):
    current_user(request, "admin")
    path = logbuf.day_path(day)
    if not path:
        return back("/logs")
    return FileResponse(path, media_type="text/plain; charset=utf-8", filename=f"log-{day}.log")


@app.get("/system")
async def system_page(request: Request):
    user = current_user(request, "admin")
    return render(request, "system.html", user, sysinfo=sysinfo.snapshot())


@app.get("/api/sysinfo")
async def api_sysinfo(request: Request):
    current_user(request, "admin")
    return JSONResponse(sysinfo.snapshot())


@app.get("/api/badge")
async def badge(request: Request):
    user = current_user(request)
    with SessionLocal() as db:
        pending = db.scalar(select(func.count(Reply.id)).where(
            Reply.status == "pending", _in_scope(Reply.chat_id, "telegram"))) + dev_bridge.pending_count(db)
        line_pending = db.scalar(select(func.count(Reply.id)).where(
            Reply.status == "pending", _in_scope(Reply.chat_id, "line"))) or 0
        line_questions = db.scalar(select(func.count(LineQuestion.id)).where(LineQuestion.status == "open")) or 0
        open_tickets = db.scalar(select(func.count(Ticket.id)).where(Ticket.status.in_(analyzer.OPEN_STATUSES)))
        unread_total = line_unread = 0
        if user.role in ("admin", "agent"):  # นับข้อความที่ยังไม่ได้อ่านแยกระบบ
            line_ids = set(db.scalars(_line_ids()))
            for chat_id, n in unread.counts(db, user.id).items():
                if chat_id in line_ids:
                    line_unread += n
                else:
                    unread_total += n
    return JSONResponse({"pending": pending, "line_pending": line_pending, "line_questions": line_questions,
                         "open_tickets": open_tickets, "unread": unread_total, "line_unread": line_unread})


# ---------------------------------------------------------------- telegram account
@app.get("/telegram")
async def telegram_page(request: Request):
    user = current_user(request, "admin")
    with SessionLocal() as db:
        account = db.get(TelegramAccount, 1)
    return render(request, "telegram.html", user, account=account, connected=telegram.connected,
                  has_api_hash=bool(decrypt(account.api_hash_enc)))


@app.post("/telegram/send-code")
async def telegram_send_code(request: Request, api_id: str = Form(...), api_hash: str = Form(""),
                             phone: str = Form(...)):
    current_user(request, "admin")
    if not api_hash.strip():  # ใช้ค่าเดิมที่บันทึกไว้
        with SessionLocal() as db:
            api_hash = decrypt(db.get(TelegramAccount, 1).api_hash_enc)
    try:
        await telegram.send_code(int(api_id), api_hash.strip(), phone.strip().replace(" ", ""))
        flash(request, "ส่งรหัสยืนยันไปที่แอป Telegram ของคุณแล้ว")
    except ValueError:
        flash(request, "API ID ต้องเป็นตัวเลข", "error")
    except TelegramLoginError as e:
        flash(request, str(e), "error")
    except Exception as e:  # noqa: BLE001
        log.exception("send code failed")
        flash(request, f"เชื่อมต่อ Telegram ไม่ได้: {e}", "error")
    return back("/telegram")


@app.post("/telegram/verify-code")
async def telegram_verify_code(request: Request, code: str = Form(...)):
    current_user(request, "admin")
    try:
        result = await telegram.verify_code(code)
        if result == "password_needed":
            flash(request, "บัญชีนี้เปิดการยืนยันสองขั้นตอน กรุณาใส่รหัสผ่าน Telegram")
        else:
            flash(request, "เชื่อมต่อ Telegram สำเร็จ")
    except TelegramLoginError as e:
        flash(request, str(e), "error")
    return back("/telegram")


@app.post("/telegram/verify-password")
async def telegram_verify_password(request: Request, password: str = Form(...)):
    current_user(request, "admin")
    try:
        await telegram.verify_password(password)  # ใช้ครั้งเดียว ไม่บันทึกรหัสผ่านลงระบบ
        flash(request, "เชื่อมต่อ Telegram สำเร็จ")
    except TelegramLoginError as e:
        flash(request, str(e), "error")
    return back("/telegram")


@app.post("/telegram/logout")
async def telegram_logout(request: Request):
    current_user(request, "admin")
    await telegram.logout()
    flash(request, "ออกจากระบบ Telegram แล้ว")
    return back("/telegram")


# ---------------------------------------------------------------- chats
@app.get("/chats")
async def chats_page(request: Request):
    return _chats_view(request, current_user(request, "agent"), "telegram")


@app.get("/linebot/chats")
async def linebot_chats_page(request: Request):
    return _chats_view(request, current_user(request, "agent"), "line")


def _chats_view(request: Request, user: User, scope: str):
    """รายชื่อแชท: scope="telegram" = แชท Telegram (ไม่รวม LINE) · scope="line" = แชท LINE Chat Bot เท่านั้น"""
    day = local_day_start_utc()
    with SessionLocal() as db:
        chats = list(db.scalars(
            select(Chat).where(Chat.channel == "line" if scope == "line" else Chat.channel != "line")
            .order_by(Chat.monitored.desc(), Chat.title)))
        counts = dict(db.execute(select(Message.chat_id, func.count(Message.id)).group_by(Message.chat_id)).all())
        today_counts = dict(db.execute(
            select(Message.chat_id, func.count(Message.id))
            .where(Message.date >= day, Message.is_outgoing.is_(False)).group_by(Message.chat_id)).all())
        last_at = dict(db.execute(select(Message.chat_id, func.max(Message.date)).group_by(Message.chat_id)).all())
        open_tickets = dict(db.execute(
            select(Ticket.chat_id, func.count(Ticket.id))
            .where(Ticket.status.in_(analyzer.OPEN_STATUSES)).group_by(Ticket.chat_id)).all())
        pending = dict(db.execute(
            select(Reply.chat_id, func.count(Reply.id)).where(Reply.status == "pending").group_by(Reply.chat_id)).all())
        unanalyzed = dict(db.execute(
            select(Message.chat_id, func.count(Message.id))
            .where(Message.analyzed.is_(False), Message.is_outgoing.is_(False)).group_by(Message.chat_id)).all())
        unread_by_chat = unread.counts(db, user.id)
    summary = {
        "total": len(chats),
        "monitored": sum(1 for c in chats if c.monitored),
        "today": sum(today_counts.values()),
        "open_tickets": sum(open_tickets.values()),
    }
    return render(request, "chats.html", user, chats=chats, counts=counts, today_counts=today_counts,
                  last_at=last_at, open_tickets=open_tickets, pending=pending, summary=summary, unanalyzed=unanalyzed,
                  unread_by_chat=unread_by_chat, scope=scope,
                  approvals_url="/linebot/replies" if scope == "line" else "/replies",
                  dev_group_id=telegram.dev_group_id, line_ready=linebot.configured(),
                  connected=telegram.connected, ai_errors=analyzer.last_error)


@app.post("/chats/sync")
async def chats_sync(request: Request):
    current_user(request, "admin")
    dialogs = await telegram.list_dialogs()
    with SessionLocal() as db:
        for d in dialogs:
            chat = db.get(Chat, d["id"])
            if chat is None:
                db.add(Chat(id=d["id"], title=d["title"], kind=d["kind"]))
            else:
                chat.title, chat.kind = d["title"], d["kind"]
        db.commit()
        dev_set = get_settings(db).get("dev_group_id")
        match = next((d for d in dialogs if d["kind"] != "user" and "autopay support" in d["title"].lower()), None)
        if not dev_set and match:
            db.merge(Setting(key="dev_group_id", value=str(match["id"])))
            db.commit()
            flash(request, f"ตั้งกลุ่ม \"{match['title']}\" เป็นกลุ่มแจ้งปัญหาโปรแกรมเมอร์แล้ว (เปลี่ยนได้ที่หน้าตั้งค่า)")
    dev_bridge.configure()
    flash(request, f"ดึงรายชื่อแชท {len(dialogs)} รายการ" if dialogs else "ยังไม่ได้เชื่อมต่อ Telegram",
          "ok" if dialogs else "error")
    return back("/chats")


@app.post("/chats/{chat_id}/toggle")
async def chats_toggle(request: Request, chat_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
        if chat:
            chat.monitored = not chat.monitored
            db.commit()
        enabled = bool(chat and chat.monitored)
        limit = int_setting(get_settings(db), "context_messages", 5, 100)
    telegram.reload_monitored()
    with SessionLocal() as db:
        is_line = ((db.get(Chat, chat_id).channel if db.get(Chat, chat_id) else '') == 'line')
    if enabled and telegram.connected and not is_line:
        try:
            waiting = await telegram.backfill(chat_id, limit=limit)
        except Exception as e:  # noqa: BLE001
            log.exception("backfill failed")
            flash(request, f"เปิดติดตามแล้ว แต่ดึงข้อความย้อนหลังไม่สำเร็จ: {e}", "error")
            return back(_chats_home(chat_id))
        if waiting:
            analyzer.schedule(chat_id)
            flash(request, f"เปิดติดตามแล้ว พบข้อความลูกค้าที่ยังไม่ได้ตอบ {waiting} ข้อความ กำลังวิเคราะห์ให้อัตโนมัติ")
        else:
            flash(request, "เปิดติดตามแล้ว ระบบจะวิเคราะห์ข้อความใหม่ให้อัตโนมัติ")
    return back(_chats_home(chat_id))


@app.post("/chats/{chat_id}/analyze")
async def chats_analyze(request: Request, chat_id: int):
    current_user(request, "agent")
    result = await analyzer.analyze(chat_id)
    flash(request, f"วิเคราะห์แล้ว: {result}", "error" if result.startswith("ผิดพลาด") else "ok")
    return back(_chats_home(chat_id))


@app.get("/chats/{chat_id}")
async def chat_detail(request: Request, chat_id: int):
    user = current_user(request, "agent")
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
        messages = list(db.scalars(
            select(Message).where(Message.chat_id == chat_id)
            .order_by(Message.date.desc(), Message.tg_message_id.desc()).limit(200)
        ))[::-1]
    if not chat:
        return back("/chats")
    with SessionLocal() as db:
        unread.mark_read(db, user.id, chat_id)  # เปิดแชทนี้ดู = อ่านแล้ว
    view = build_chat_view(messages)
    customers = {i["key"] for i in view if not i["m"].is_outgoing}
    is_line = chat.channel == "line"
    return render(request, "chat_detail.html", user, chat=chat, messages=messages, view=view,
                  dev_group=chat_id == telegram.dev_group_id,
                  nav_path="/linebot/chats" if is_line else None,
                  back_url="/linebot/chats" if is_line else "/chats",
                  back_label="แชทลูกค้า LINE" if is_line else "แชทลูกค้า",
                  waiting=sum(1 for i in view if not i["m"].is_outgoing and not i["m"].analyzed),
                  people=len(customers))


# ---------------------------------------------------------------- แชทสด (WebSocket) + พิมพ์ตอบจากหน้าเว็บ
@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    """ข้อความแชทใหม่ถูกส่งมาที่หน้าเว็บทันที (ต้องล็อกอินเป็นแอดมิน/ทีมงาน และเรียกจากหน้าเว็บของระบบเองเท่านั้น)"""
    origin = ws.headers.get("origin", "")
    if origin and urlparse(origin).netloc != ws.headers.get("host", ""):
        await ws.close(code=1008)  # กัน cross-site websocket hijacking
        return
    uid = ws.session.get("user_id")
    role = None
    if uid:
        with SessionLocal() as db:
            user = db.get(User, uid)
            role = user.role if user else None
    if role not in ("admin", "agent"):
        await ws.close(code=1008)
        return
    await ws.accept()
    queue = chatbus.subscribe()

    async def pump() -> None:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=25)
            except asyncio.TimeoutError:
                event = {"type": "ping"}  # กันพร็อกซีตัดการเชื่อมต่อที่เงียบเกินไป
            await ws.send_json(event)

    async def drain() -> None:
        while True:
            await ws.receive_text()  # ไม่รับคำสั่งจากหน้าเว็บ แค่รอให้รู้ว่าถูกปิด

    tasks = [asyncio.create_task(pump()), asyncio.create_task(drain())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        chatbus.unsubscribe(queue)  # ถอนก่อนเสมอ (ไม่ต้องรอ await) กันผู้ติดตามค้างเมื่อถูกยกเลิกกลางทาง
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await ws.close()
        except Exception:  # noqa: BLE001 - ปิดไปแล้ว
            pass


@app.get("/api/chats/{chat_id}/messages")
async def api_chat_messages(request: Request, chat_id: int, after: int = 0, limit: int = 100, tail: int = 0):
    """ข้อความของแชทที่ใหม่กว่า id ที่ระบุ (ใช้ดึงส่วนที่ขาดตอนเชื่อมต่อใหม่ / สำรองเมื่อ WebSocket ใช้ไม่ได้)
    · tail=N: ข้อความล่าสุด N ข้อความ (เรียงเก่า -> ใหม่) ใช้เปิดดูแชทย้อนหลังในหน้าประวัติของ รออนุมัติ"""
    current_user(request, "agent")
    with SessionLocal() as db:
        if tail > 0:
            rows = list(db.scalars(
                select(Message).where(Message.chat_id == chat_id)
                .order_by(Message.id.desc()).limit(min(tail, 200))))[::-1]
        else:
            rows = list(db.scalars(
                select(Message).where(Message.chat_id == chat_id, Message.id > max(after, 0))
                .order_by(Message.id).limit(min(max(limit, 1), 200))))
    return JSONResponse({"messages": [chatbus.to_dict(m) for m in rows]})


class SayIn(BaseModel):
    text: str
    reply_to: int | None = None


@app.post("/api/chats/{chat_id}/say")
async def api_chat_say(request: Request, chat_id: int, body: SayIn):
    """ทีมงานพิมพ์ตอบลูกค้าจากหน้า รออนุมัติ: ส่งเข้าแชท Telegram ทันที (ไม่ผ่านขั้นอนุมัติ เพราะคนพิมพ์เอง)"""
    user = current_user(request, "agent")
    text = body.text.strip()
    if not text:
        return JSONResponse({"ok": False, "error": "ข้อความว่าง"}, status_code=400)
    if len(text) > 4000:
        return JSONResponse({"ok": False, "error": "ข้อความยาวเกิน 4,000 ตัวอักษร"}, status_code=400)
    with SessionLocal() as db:
        if not db.get(Chat, chat_id):
            return JSONResponse({"ok": False, "error": "ไม่พบแชทนี้"}, status_code=404)
    with SessionLocal() as db:
        channel = db.get(Chat, chat_id).channel or "telegram"
    try:
        if channel == "line":
            data = await linebot.send_manual(chat_id, text, by=user.username)
        else:
            data = await telegram.send_manual(chat_id, text, body.reply_to, by=user.username)
    except (TelegramLoginError, linebot.LineBotError) as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=503)
    except Exception as e:  # noqa: BLE001
        log.exception("manual message to chat %s failed", chat_id)
        return JSONResponse({"ok": False, "error": f"ส่งไม่สำเร็จ: {e}"}, status_code=502)
    log.info("%s พิมพ์ตอบในแชท %s จากหน้าเว็บ", user.username, chat_id)
    with SessionLocal() as db:
        unread.mark_read(db, user.id, chat_id)  # ตอบแล้ว = อ่านแล้ว
    return JSONResponse({"ok": True, "message": data})


SAY_MAX_IMAGES = 5
SAY_CAPTION_MAX = 1000


@app.post("/api/chats/{chat_id}/say-image")
async def api_chat_say_image(request: Request, chat_id: int, images: list[UploadFile] = File([]),
                             text: str = Form(""), reply_to: int = Form(0)):
    """ทีมงานวางรูป (Ctrl+V) / แนบรูปในช่องพิมพ์ตอบ -> ส่งรูป (+คำอธิบายที่พิมพ์ไว้) ถึงลูกค้าทันที
    ต้องเรียกจากหน้าเว็บของระบบเองเท่านั้น (ใส่ header X-Requested-With: rv-say) · ตรวจชนิดรูปจากเนื้อไฟล์ ไม่เกิน 8MB ต่อรูป"""
    user = current_user(request, "agent")
    if request.headers.get("x-requested-with") != "rv-say":
        return JSONResponse({"ok": False, "error": "คำขอไม่ถูกต้อง"}, status_code=400)
    text = (text or "").strip()
    if len(text) > SAY_CAPTION_MAX:
        return JSONResponse({"ok": False, "error": f"คำอธิบายรูปยาวเกิน {SAY_CAPTION_MAX:,} ตัวอักษร"}, status_code=400)
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
        channel = ((chat.channel if chat else "") or "telegram")
    if not chat:
        return JSONResponse({"ok": False, "error": "ไม่พบแชทนี้"}, status_code=404)
    saved, errors = await _save_guide_images(images, SAY_MAX_IMAGES, prefix="say", max_total=SAY_MAX_IMAGES)
    if not saved:
        return JSONResponse({"ok": False, "error": "; ".join(errors) or "ไม่พบรูปที่จะส่ง"}, status_code=400)

    def _drop() -> None:
        for n in saved:
            (MEDIA_DIR / n).unlink(missing_ok=True)

    try:
        if channel == "line":
            if not all(line_service._image_url(n) for n in saved):
                _drop()
                return JSONResponse({"ok": False, "error": "ส่งรูปทาง LINE ไม่ได้: ยังไม่มีที่อยู่เว็บสาธารณะ (PUBLIC_URL) ให้ LINE ดึงรูป"},
                                    status_code=503)
            await linebot.send_reply(chat_id, text, saved, by=user.username)
            with SessionLocal() as db:  # ข้อความที่เพิ่งส่ง (ข้อความอธิบาย + รูป) = ข้อความฝั่งทีมงานล่าสุด
                rows = list(db.scalars(select(Message).where(Message.chat_id == chat_id, Message.is_outgoing.is_(True))
                                       .order_by(Message.id.desc()).limit(len(saved) + (1 if text else 0))))[::-1]
                messages = [chatbus.to_dict(m) for m in rows]
        else:
            items = [(str(MEDIA_DIR / n), n) for n in saved]
            messages = await telegram.send_manual_images(chat_id, items, text, reply_to or None, by=user.username)
    except (TelegramLoginError, linebot.LineBotError) as e:
        _drop()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=503)
    except Exception as e:  # noqa: BLE001
        log.exception("manual image to chat %s failed", chat_id)
        _drop()
        return JSONResponse({"ok": False, "error": f"ส่งรูปไม่สำเร็จ: {e}"}, status_code=502)
    log.info("%s ส่งรูป %s รูปในแชท %s จากหน้าเว็บ", user.username, len(saved), chat_id)
    with SessionLocal() as db:
        unread.mark_read(db, user.id, chat_id)  # ตอบแล้ว = อ่านแล้ว
    return JSONResponse({"ok": True, "messages": messages, "skipped": errors})


class ReadIn(BaseModel):
    upto: int | None = None


@app.post("/api/chats/{chat_id}/read")
async def api_chat_read(request: Request, chat_id: int, body: ReadIn):
    """ทำเครื่องหมายว่าอ่านแชทนี้แล้ว (ถึงข้อความ upto หรือล่าสุด) · นับแยกรายคน"""
    user = current_user(request, "agent")
    with SessionLocal() as db:
        last = unread.mark_read(db, user.id, chat_id, body.upto)
        total = unread.total(db, user.id)
    return JSONResponse({"ok": True, "last": last, "unread": total})


# ---------------------------------------------------------------- ประกาศ: ส่งข้อความถึงหลายกลุ่มพร้อมกัน
def _announce_chats(db) -> list[Chat]:
    """แชทที่ประกาศถึงได้: แชทลูกค้าทั้งหมด ยกเว้นกลุ่มโปรแกรมเมอร์ (กลุ่มภายใน) และแชทส่วนตัวของ Telegram"""
    dev = telegram.dev_group_id
    return [c for c in db.scalars(select(Chat)) if c.id != dev and not ((c.channel or "telegram") != "line" and c.kind == "user")]


def _announce_info(db, aid: int):
    ann = db.get(Announcement, aid)
    if not ann:
        return None, [], {}
    targets = list(db.scalars(select(AnnouncementTarget).where(AnnouncementTarget.announcement_id == aid).order_by(AnnouncementTarget.id)))
    return ann, targets, announce.counts(db, [aid])[aid]


@app.get("/announce")
async def announce_page(request: Request):
    user = current_user(request, "agent")
    with SessionLocal() as db:
        chats = sorted(_announce_chats(db), key=lambda c: ((c.channel or "telegram") == "line", not c.monitored, (c.title or "").lower()))
        recent = list(db.scalars(select(Announcement).order_by(Announcement.id.desc()).limit(20)))
        cnt = announce.counts(db, [a.id for a in recent])
    return render(request, "announce.html", user, chats=chats, recent=recent, counts=cnt, connected=telegram.connected,
                  line_ready=bool(linebot.token()), max_images=SAY_MAX_IMAGES, max_text=announce.MAX_TEXT,
                  max_targets=announce.MAX_TARGETS, delay=announce.delay(), status_labels=announce.STATUS_LABELS)


@app.post("/announce/send")
async def announce_send(request: Request, text: str = Form(""), chat_ids: list[int] = Form([]),
                        images: list[UploadFile] = File([])):
    user = current_user(request, "agent")
    text = text.strip()
    saved, notes = await _save_guide_images(images, SAY_MAX_IMAGES, prefix="announce", max_total=SAY_MAX_IMAGES)

    def refuse(message: str):
        for n in saved:
            (MEDIA_DIR / n).unlink(missing_ok=True)
        flash(request, message, "error")
        return back("/announce")

    if not text and not saved:
        return refuse("พิมพ์ข้อความประกาศหรือแนบรูปอย่างน้อยอย่างใดอย่างหนึ่ง" + (" · " + "; ".join(notes) if notes else ""))
    if len(text) > announce.MAX_TEXT:
        return refuse(f"ข้อความประกาศยาวเกิน {announce.MAX_TEXT:,} ตัวอักษร")
    with SessionLocal() as db:
        allowed = {c.id: c for c in _announce_chats(db)}
    targets = [cid for cid in dict.fromkeys(chat_ids) if cid in allowed]
    if not targets:
        return refuse("เลือกกลุ่มที่จะประกาศอย่างน้อย 1 แชท")
    if len(targets) > announce.MAX_TARGETS:
        return refuse(f"ประกาศได้ครั้งละไม่เกิน {announce.MAX_TARGETS} แชท (เลือกไว้ {len(targets)})")
    if any((allowed[c].channel or "telegram") != "line" for c in targets) and not telegram.connected:
        return refuse("ยังไม่ได้เชื่อมต่อ Telegram จึงประกาศถึงกลุ่ม Telegram ไม่ได้")
    if any((allowed[c].channel or "telegram") == "line" for c in targets) and not linebot.token():
        return refuse("ยังไม่ได้ตั้งค่า LINE_BOT_CHANNEL_ACCESS_TOKEN จึงประกาศถึงแชท LINE ไม่ได้")
    aid = announce.create(text, targets, saved, user.username)
    announce.start(aid)
    log.info("%s ประกาศถึง %s แชท (ประกาศ #%s)", user.username, len(targets), aid)
    flash(request, f"เริ่มประกาศถึง {len(targets)} แชทแล้ว" + (" · ข้ามบางรูป: " + "; ".join(notes) if notes else ""))
    return back(f"/announce/{aid}")


@app.get("/announce/{aid}")
async def announce_detail(request: Request, aid: int):
    user = current_user(request, "agent")
    with SessionLocal() as db:
        ann, targets, cnt = _announce_info(db, aid)
    if not ann:
        flash(request, "ไม่พบประกาศนี้", "error")
        return back("/announce")
    return render(request, "announce_detail.html", user, ann=ann, targets=targets, cnt=cnt, images=json.loads(ann.images or "[]"),
                  status_labels=announce.STATUS_LABELS, target_labels=announce.TARGET_LABELS, delay=announce.delay())


@app.get("/api/announce/{aid}")
async def api_announce(request: Request, aid: int):
    current_user(request, "agent")
    with SessionLocal() as db:
        ann, targets, cnt = _announce_info(db, aid)
    if not ann:
        return JSONResponse({"ok": False, "error": "ไม่พบประกาศนี้"}, status_code=404)
    return JSONResponse({"ok": True, "status": ann.status, "label": announce.STATUS_LABELS.get(ann.status, ann.status), "counts": cnt,
                         "targets": [{"id": t.id, "status": t.status, "label": announce.TARGET_LABELS.get(t.status, t.status), "error": t.error}
                                     for t in targets]})


@app.post("/announce/{aid}/cancel")
async def announce_cancel(request: Request, aid: int):
    user = current_user(request, "agent")
    if announce.cancel(aid):
        log.info("%s ยกเลิกประกาศ #%s", user.username, aid)
        flash(request, "ยกเลิกประกาศแล้ว · แชทที่ส่งไปแล้วยังคงอยู่ ที่ยังไม่ถึงคิวถูกข้าม")
    return back(f"/announce/{aid}")


@app.post("/announce/{aid}/retry")
async def announce_retry(request: Request, aid: int):
    user = current_user(request, "agent")
    if not telegram.connected and not linebot.token():
        flash(request, "ยังไม่ได้เชื่อมต่อ Telegram / LINE", "error")
        return back(f"/announce/{aid}")
    n = announce.retry(aid)
    flash(request, f"ส่งต่อที่ค้าง/ล้มเหลว {n} แชท" if n else "ไม่มีแชทที่ต้องส่งต่อ", "ok" if n else "error")
    if n:
        log.info("%s สั่งส่งต่อประกาศ #%s (%s แชท)", user.username, aid, n)
    return back(f"/announce/{aid}")

# ---------------------------------------------------------------- replies (approval queue)
@app.get("/replies")
async def replies_page(request: Request, status: str = "pending", box: str = ""):
    return _replies_view(request, current_user(request, "agent"), "telegram", status, box)


@app.get("/linebot/replies")
async def linebot_replies_page(request: Request, status: str = "pending"):
    return _replies_view(request, current_user(request, "agent"), "line", status, "customer")


def _replies_view(request: Request, user: User, scope: str, status: str, box: str):
    """คิวรออนุมัติ: scope="telegram" = ร่างของแชท Telegram (+ ticket รอส่งกลุ่มโปรแกรมเมอร์) · scope="line" = ร่างของลูกค้า LINE เท่านั้น"""
    is_line = scope == "line"
    in_scope = _in_scope(Reply.chat_id, scope)
    with SessionLocal() as db:
        # ประวัติเรียงตามเวลาที่ดำเนินการ (ส่ง/ปฏิเสธ) ล่าสุดก่อน · คิวรออนุมัติเรียงตามเวลาที่ร่าง
        sort_at = Reply.created_at if status == "pending" else func.coalesce(Reply.decided_at, Reply.created_at)
        query = select(Reply).where(in_scope).order_by(sort_at.desc()).limit(100)
        if status != "all":
            query = query.where(Reply.status == status)
        replies = list(db.scalars(query))
        status_counts = dict(db.execute(
            select(Reply.status, func.count(Reply.id)).where(in_scope).group_by(Reply.status)).all())
        all_chats = list(db.scalars(select(Chat)))
        chat_titles = {c.id: c.title for c in all_chats}
        chat_channels = {c.id: c.channel for c in all_chats}
        linebot_mode = get_settings(db).get("linebot_mode", "approve")
        dev_queue = []
        if not is_line:  # ticket รอส่งเข้ากลุ่มโปรแกรมเมอร์เป็นของฝั่ง Telegram
            for t in db.scalars(select(Ticket).where(Ticket.dev_status == "pending").order_by(Ticket.created_at)):
                dev_queue.append({"ticket": t, "photos": [a.media_path for a in t.attachments][:dev_bridge.MAX_PHOTOS],
                                  "text": dev_bridge.format_ticket(t, chat_titles.get(t.chat_id, str(t.chat_id)))})
        unread_by_chat = unread.counts(db, user.id, {r.chat_id for r in replies}) if status == "pending" else {}
        context = {}
        for r in replies:
            if r.status == "pending":
                context[r.id] = list(db.scalars(
                    select(Message).where(Message.chat_id == r.chat_id)
                    .order_by(Message.date.desc(), Message.tg_message_id.desc()).limit(8)
                ))[::-1]
    status_counts["all"] = sum(status_counts.values())
    status_counts["pending"] = status_counts.get("pending", 0) + len(dev_queue)
    if box not in ("customer", "dev"):
        box = "dev" if dev_queue and not (status == "pending" and replies) else "customer"
    return render(request, "replies.html", user, replies=replies, status=status, chat_titles=chat_titles,
                  context=context, status_counts=status_counts, dev_queue=dev_queue, box=box, unread_by_chat=unread_by_chat,
                  chat_channels=chat_channels, scope=scope, linebot_mode=linebot_mode, max_photos=dev_bridge.MAX_PHOTOS,
                  dev_group_title=chat_titles.get(telegram.dev_group_id, "กลุ่มโปรแกรมเมอร์"))


def _load_pending(reply_id: int) -> Reply | None:
    with SessionLocal() as db:
        reply = db.get(Reply, reply_id)
    return reply if reply and reply.status in ("pending", "failed") else None


async def deliver_reply(reply_id: int, text: str, by: str, media: list[str] | None = None) -> tuple[str, str]:
    """ส่งคำตอบที่อนุมัติแล้วถึงลูกค้า (ใช้ร่วมกันทั้งหน้าเว็บและ LINE) คืนค่า (status, ข้อความแจ้งผล)"""
    text = ai_service.plain_reply(text)  # กันเหตุสุดท้าย: ห้ามส่ง JSON ดิบ/code fence ถึงลูกค้า
    if not text:
        return "failed", "ข้อความว่าง ส่งไม่ได้"
    with SessionLocal() as db:
        reply = db.get(Reply, reply_id)
        allowed = json.loads(reply.media or "[]")
        photos = allowed if media is None else [p for p in allowed if p in media]
        reply.status, reply.final_text = "sending", text
        reply.media = json.dumps(photos) if photos else ""
        reply.decided_by, reply.decided_at = by, utcnow()
        db.commit()
        chat_id, reply_to, kind, ticket_id = reply.chat_id, reply.reply_to_tg_id, reply.kind, reply.ticket_id
        channel = (db.get(Chat, chat_id).channel if db.get(Chat, chat_id) else "telegram") or "telegram"
    try:
        if channel == "line":  # LINE Chat Bot: ส่งผ่าน LINE OA ของลูกค้า (ข้อความ + รูปจากคู่มือ)
            sent = await linebot.send_reply(chat_id, text, photos, by=by)
            status, error = "sent", ""
            msg = "อนุมัติและส่งข้อความทาง LINE แล้ว" + (f" พร้อมรูป {len(photos)} รูป" if photos else "")
            files = photos
        else:
            sent_id = await telegram.send_reply(chat_id, text, reply_to)
            telegram.tag_sent_by(chat_id, [sent_id], by)  # จดว่าใครกดส่ง (แสดงชื่อพนักงานในบทสนทนา)
            status, error = "sent", ""
            files = [str(MEDIA_DIR / p) for p in photos if (MEDIA_DIR / p).is_file()]
            if files:
                try:
                    telegram.tag_sent_by(chat_id, await telegram.send_files(chat_id, files, reply_to=sent_id), by)
                except Exception as e:  # noqa: BLE001 - ข้อความส่งแล้ว แจ้งเฉพาะรูปที่ส่งไม่ได้
                    log.exception("send reply photos failed")
                    error = f"ส่งข้อความแล้ว แต่ส่งรูปไม่สำเร็จ: {e}"
            msg = error or ("อนุมัติและส่งข้อความแล้ว" + (f" พร้อมรูป {len(files)} รูป" if files else ""))
    except linebot.LineBotError as e:
        status, error, msg = "failed", str(e), f"ส่งข้อความทาง LINE ไม่สำเร็จ: {e}"
    except TelegramLoginError as e:
        status, error, msg = "failed", str(e), f"ส่งข้อความไม่สำเร็จ: {e}"
    except Exception as e:  # noqa: BLE001
        log.exception("send reply failed")
        status, error, msg = "failed", str(e), f"ส่งข้อความไม่สำเร็จ: {e}"
    with SessionLocal() as db:
        reply = db.get(Reply, reply_id)
        reply.status, reply.error = status, error
        if status == "sent" and ticket_id and db.get(Ticket, ticket_id):
            label = "แจ้งลูกค้าว่าแก้ไขเรียบร้อยแล้ว" if kind == "resolved" else "ตอบลูกค้า"
            db.add(TicketEvent(ticket_id=ticket_id, kind="status", author=by, body=f"{label}: {text}"))
        db.commit()
    return status, msg


def fix_json_drafts() -> int:
    """ร่างที่ค้างรออนุมัติเป็น JSON ดิบ (บั๊กเดิมตอนกด "เขียนใหม่") -> ดึงเฉพาะข้อความตอบกลับออกมา · คืนค่าจำนวนที่แก้"""
    fixed = 0
    with SessionLocal() as db:
        for r in db.scalars(select(Reply).where(Reply.status.in_(("pending", "failed")))):
            for attr in ("final_text", "ai_text"):
                value = getattr(r, attr) or ""
                if value.lstrip().startswith(("{", "```")):
                    cleaned = ai_service.plain_reply(value)
                    if cleaned and cleaned != value:
                        setattr(r, attr, cleaned)
                        fixed += 1
        db.commit()
    return fixed


def reject_reply(reply_id: int, reason: str, by: str) -> bool:
    with SessionLocal() as db:
        reply = db.get(Reply, reply_id)
        if reply and reply.status in ("pending", "failed"):
            reply.status, reply.reject_reason = "rejected", (reason or "").strip()
            reply.decided_by, reply.decided_at = by, utcnow()
            db.commit()
            return True
    return False


@app.post("/replies/{reply_id}/approve")
async def replies_approve(request: Request, reply_id: int, text: str = Form(...), media: list[str] = Form([])):
    user = current_user(request, "agent")
    home = _replies_home(reply_id)
    if not _load_pending(reply_id):
        flash(request, "ข้อความนี้ถูกดำเนินการไปแล้ว", "error")
        return back(home)
    status, msg = await deliver_reply(reply_id, text, user.username, media=media)
    with SessionLocal() as db:
        row = db.get(Reply, reply_id)
        if row:
            unread.mark_read(db, user.id, row.chat_id)
    flash(request, msg, "ok" if status == "sent" else "error")
    return back(home)


@app.post("/replies/{reply_id}/reject")
async def replies_reject(request: Request, reply_id: int, reason: str = Form("")):
    user = current_user(request, "agent")
    home = _replies_home(reply_id)
    if reject_reply(reply_id, reason, user.username):
        with SessionLocal() as db:
            row = db.get(Reply, reply_id)
            if row:
                unread.mark_read(db, user.id, row.chat_id)
        flash(request, "ปฏิเสธการส่งข้อความแล้ว (ไม่ได้ส่งถึงลูกค้า)")
    return back(home)


# ---------------------------------------------------------------- LINE webhook (กดอนุมัติจาก LINE)
@app.post("/line/webhook")
async def line_webhook(request: Request):
    body = await request.body()
    if not line_service.verify(body, request.headers.get("x-line-signature", "")):
        return JSONResponse({"error": "bad signature"}, status_code=403)
    try:
        events = json.loads(body).get("events", [])
    except json.JSONDecodeError:
        events = []
    with SessionLocal() as db:
        settings = get_settings(db)
    target = settings.get("line_target", "")
    allow_approve = settings.get("line_approve", "1") == "1"

    def save(**values: str) -> None:
        with SessionLocal() as db:
            for key, value in values.items():
                db.merge(Setting(key=key, value=value))
            db.commit()
        settings.update(values)

    for ev in events:
        source = ev.get("source") or {}
        src = line_service.source_id(source)  # แชทที่ส่ง event (user/group/room) = ปลายทางที่ส่งการ์ดไป
        user_id = source.get("userId", "")    # คนที่ส่ง/กดจริงๆ (ในกลุ่มคือสมาชิกคนนั้น) ใช้เช็กสิทธิ์
        rtoken = ev.get("replyToken", "")
        etype = ev.get("type")
        allowed = line_service.load_allowed(settings.get("line_allowed", ""))
        member = line_service.find_allowed(allowed, user_id)  # None = ไม่ได้รับอนุญาต

        # รหัสเชื่อมผู้อนุมัติ: แอดมินสร้างรหัสบนเว็บ คนที่จะเป็นผู้อนุมัติพิมพ์รหัสส่งหา OA (หรือในกลุ่ม)
        message = ev.get("message") or {}
        if etype == "message" and message.get("type") == "text":
            old_pair = settings.get("line_pair", "")
            state, new_pair = line_service.check_pair(old_pair, message.get("text", ""))
            if new_pair != old_pair:
                save(line_pair=new_pair)
            if state == "ok":
                if not user_id:
                    out = ("ระบบอ่านไอดีผู้ใช้ LINE ของคุณไม่ได้ · ลองเพิ่ม OA เป็นเพื่อนก่อน แล้วส่งรหัสในแชทส่วนตัวกับ OA", "warn")
                else:
                    name = await line_service.display_name(source)
                    users = [u for u in allowed if u["id"] != user_id]
                    users.append({"id": user_id, "name": name, "added": utcnow().strftime("%Y-%m-%d %H:%M")})
                    save(line_allowed=line_service.dump_allowed(users))
                    note = ""
                    if not target and src:  # ยังไม่มีปลายทางแจ้งเตือน -> ใช้แชทที่ส่งรหัสมาเป็นปลายทาง
                        save(line_target=src, line_enabled="1")
                        target, note = src, " และตั้งแชทนี้เป็นปลายทางแจ้งเตือน"
                    out = (f"เพิ่ม {name or 'คุณ'} เป็นผู้อนุมัติแล้ว{note}", "ok")
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message(*out))
                continue
            if state == "wrong":
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message("รหัสไม่ถูกต้องหรือหมดอายุ · ขอรหัสใหม่จากผู้ดูแลระบบ", "warn"))
                continue

        if etype in ("follow", "join", "message") and src:
            # เปลี่ยนปลายทางแจ้งเตือนได้เฉพาะผู้อนุมัติ (กันคนแปลกหน้าที่ทัก OA แย่งรับข้อมูลลูกค้า) · คนอื่นไม่ตอบอะไร
            if not member:
                continue
            if src != target:
                save(line_target=src, line_enabled="1")
                target = src
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message(
                        "เชื่อมต่อ LINE สำหรับแจ้งเตือนรออนุมัติเรียบร้อยแล้วค่ะ เมื่อมีข้อความรออนุมัติจะส่งมาที่นี่", "ok"))
            elif etype == "message" and rtoken:
                await line_service.reply(rtoken, line_service.notice_message("ระบบแจ้งเตือนรออนุมัติพร้อมใช้งานแล้วค่ะ", "info"))
        elif etype == "postback" and src:
            data = ev.get("postback", {}).get("data", "")
            action, _, rid = data.partition(":")
            if target and src != target:
                continue  # รับคำสั่งเฉพาะจากปลายทางที่เชื่อมไว้
            if not rid.isdigit():
                continue
            rid = int(rid)
            if not allow_approve:
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message("ปิดการอนุมัติจาก LINE ไว้ · อนุมัติได้ที่หน้าเว็บ", "warn"))
                continue
            if not allowed:
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message(
                        "ยังไม่ได้ตั้งผู้ที่อนุมัติจาก LINE ได้ · ให้ผู้ดูแลระบบเพิ่มที่หน้า แจ้งเตือน LINE ในเว็บ", "warn"))
                continue
            if not member:  # ผู้กดไม่อยู่ในรายชื่อ (เช่น สมาชิกคนอื่นในกลุ่ม) ห้ามอนุมัติ/ปฏิเสธ
                log.warning("LINE postback จากผู้ที่ไม่ได้รับอนุญาต: …%s", user_id[-6:])
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message(
                        "คุณไม่ได้รับอนุญาตให้อนุมัติหรือไม่ส่งจาก LINE · ติดต่อผู้ดูแลระบบ", "warn"))
                continue
            by = f"LINE:{line_service.user_label(member)}"[:60]  # บันทึกว่าใครกด (แสดงในประวัติ "ดำเนินการโดย")
            if action in ("postdev", "skipdev"):  # การ์ด ticket รอส่งเข้ากลุ่มโปรแกรมเมอร์ (rid = เลข ticket)
                with SessionLocal() as db:
                    t = db.get(Ticket, rid)
                    is_pending = bool(t and t.dev_status == "pending")
                if not is_pending:
                    out, tone = f"ticket #{rid} ถูกดำเนินการไปแล้ว", "info"
                elif action == "postdev":
                    try:
                        result = await dev_bridge.post_ticket(rid, force=True, approver=by)
                    except Exception as e:  # noqa: BLE001
                        log.exception("post ticket from LINE failed")
                        result = f"ส่งไม่สำเร็จ: {e}"
                    out, tone = ((f"ส่ง ticket #{rid} เข้ากลุ่มโปรแกรมเมอร์แล้ว", "ok") if result.startswith("ส่งเข้ากลุ่ม")
                                 else (result or "ส่งไม่สำเร็จ ลองที่หน้าเว็บอีกครั้ง", "bad"))
                else:
                    dev_bridge.skip_ticket(rid, by)
                    out, tone = f"ไม่ส่ง ticket #{rid} เข้ากลุ่มโปรแกรมเมอร์", "bad"
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message(out, tone))
                continue
            pending = _load_pending(rid)
            if not pending:
                if rtoken:
                    await line_service.reply(rtoken, line_service.notice_message("รายการนี้ถูกดำเนินการไปแล้ว", "info"))
                continue
            if action == "approve":
                status, _ = await deliver_reply(rid, pending.final_text, by)
                out, tone = ("ส่งข้อความถึงลูกค้าแล้ว", "ok") if status == "sent" else ("ส่งไม่สำเร็จ ลองที่หน้าเว็บอีกครั้ง", "bad")
            elif action == "reject":
                reject_reply(rid, "ปฏิเสธจาก LINE", by)
                out, tone = "ไม่ส่งข้อความนี้แล้ว", "bad"
            else:
                out, tone = "", "info"
            if out and rtoken:
                await line_service.reply(rtoken, line_service.notice_message(out, tone))
    return JSONResponse({"ok": True})


@app.get("/line")
async def line_page(request: Request):
    user = current_user(request, "admin")
    with SessionLocal() as db:
        settings = get_settings(db)
    return render(request, "line.html", user, settings=settings,
                  line_configured=line_service.configured(), line_has_token=bool(line_service.token()),
                  line_has_secret=bool(line_service.secret()), line_target=settings.get("line_target", ""),
                  line_webhook=str(request.base_url).rstrip("/") + "/line/webhook",
                  line_public_url=line_service.public_url(),
                  line_allowed=line_service.load_allowed(settings.get("line_allowed", "")),
                  line_pair=line_service.pair_state(settings.get("line_pair", "")))


@app.post("/line")
async def line_save(request: Request):
    current_user(request, "admin")
    form = await request.form()
    with SessionLocal() as db:
        for key in ("line_enabled", "line_approve"):
            row = db.get(Setting, key) or Setting(key=key)
            row.value = "1" if form.get(key) else "0"
            db.merge(row)
        db.commit()
    flash(request, "บันทึกการตั้งค่า LINE แล้ว")
    return back("/line")


@app.post("/line/pair")
async def line_pair_new(request: Request):
    """สร้างรหัสเชื่อมผู้อนุมัติ (6 หลัก หมดอายุ 10 นาที ใช้ได้ครั้งเดียว) · คนที่จะเป็นผู้อนุมัติพิมพ์รหัสส่งหา LINE OA"""
    current_user(request, "admin")
    _, stored = line_service.new_pair_code()
    with SessionLocal() as db:
        db.merge(Setting(key="line_pair", value=stored))
        db.commit()
    flash(request, "สร้างรหัสเชื่อมแล้ว · ดูรหัสในกล่องด้านล่าง ให้ผู้อนุมัติพิมพ์ส่งใน LINE ภายใน 10 นาที")
    return back("/line")


@app.post("/line/pair/cancel")
async def line_pair_cancel(request: Request):
    current_user(request, "admin")
    with SessionLocal() as db:
        db.merge(Setting(key="line_pair", value=""))
        db.commit()
    flash(request, "ยกเลิกรหัสเชื่อมแล้ว")
    return back("/line")


@app.post("/line/users/remove")
async def line_user_remove(request: Request, uid: str = Form("")):
    """ลบผู้อนุมัติออกจากรายชื่อ (คนนั้นจะกดอนุมัติ/ไม่ส่งจาก LINE ไม่ได้อีก)"""
    current_user(request, "admin")
    with SessionLocal() as db:
        users = line_service.load_allowed(get_settings(db).get("line_allowed", ""))
        kept = [u for u in users if u["id"] != uid]
        removed = [u for u in users if u["id"] == uid]
        db.merge(Setting(key="line_allowed", value=line_service.dump_allowed(kept)))
        db.commit()
    if removed:
        flash(request, f"ลบ {line_service.user_label(removed[0])} ออกจากผู้อนุมัติแล้ว")
    return back("/line")


@app.post("/line/test")
async def line_test(request: Request):
    """ส่งการ์ดตัวอย่างเข้า LINE ที่เชื่อมไว้ เพื่อดูหน้าตาจริง (ไม่เกี่ยวกับรายการจริง กดปุ่มในการ์ดไม่มีผลต่อข้อมูล)"""
    current_user(request, "admin")
    with SessionLocal() as db:
        target = get_settings(db).get("line_target", "")
    if not line_service.configured():
        flash(request, "ยังไม่ได้ตั้ง LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET", "error")
    elif not target:
        flash(request, "ยังไม่ได้เชื่อมปลายทาง LINE · ทัก OA หรือเพิ่มเป็นเพื่อนก่อน", "error")
    elif await line_service.push(target, line_service.sample_messages()):
        if not line_service.public_url():
            flash(request, "ส่งการ์ดทดสอบแล้ว แต่ยังไม่มีที่อยู่เว็บสาธารณะ จึงยังไม่แสดงรูปและปุ่ม \"เปิดดูในเว็บ\" "
                           "· Generate Domain ใน Railway หรือตั้ง PUBLIC_URL", "error")
        else:
            flash(request, "ส่งข้อความทดสอบเข้า LINE แล้ว · เปิดดูที่แชท LINE OA (ถ้ารูปไม่ขึ้น ดูสาเหตุที่หน้า Log ระบบ)")
    else:
        flash(request, "ส่งเข้า LINE ไม่สำเร็จ · ดูสาเหตุที่หน้า Log ระบบ", "error")
    return back("/line")


async def _rewrite_draft(reply_id: int, instruction: str, text: str = "") -> str | None:
    """ให้ AI เขียนร่างใหม่ตามคำสั่งแล้วบันทึกลงร่าง · คืนข้อความใหม่ · None = ร่างนี้ถูกดำเนินการไปแล้ว · AIError = AI ทำไม่ได้"""
    reply = _load_pending(reply_id)
    if not reply:
        return None
    notes_text = ""
    with SessionLocal() as db:
        settings = get_settings(db)
        chat = db.get(Chat, reply.chat_id)
        history = list(db.scalars(
            select(Message).where(Message.chat_id == reply.chat_id).order_by(Message.date.desc(), Message.tg_message_id.desc()).limit(12)
        ))[::-1]
        if settings.get("learn_use_notes") == "1":
            try:
                notes_text, _ = learning.notes_for_prompt(db, reply.chat_id, instruction)
            except Exception:  # noqa: BLE001 - โหลดโน้ตไม่ได้ ก็เขียนใหม่ต่อได้
                log.exception("load AI notes failed")
    started = time.perf_counter()
    new_text = await ai_service.rewrite_reply(
        settings, chat.title if chat else "", history, text or reply.final_text, instruction, notes=notes_text
    )
    log.info("AI เขียนร่าง #%s ใหม่ใน %.1f วินาที (รุ่น %s)", reply_id, time.perf_counter() - started, ai_service.rewrite_model(settings))
    with SessionLocal() as db:
        row = db.get(Reply, reply_id)
        row.final_text = new_text
        try:  # จำคำสั่งของแอดมินไว้ เป็นสัญญาณให้ AI เรียนรู้ว่าควรร่างให้ตรงใจตั้งแต่แรก
            history_log = [str(x) for x in json.loads(row.edit_log or "[]")]
        except ValueError:
            history_log = []
        row.edit_log = json.dumps((history_log + [instruction.strip()[:200]])[-10:], ensure_ascii=False)
        db.commit()
    return new_text


REANALYZE_MAX_MESSAGES = 20


def _reopen_for_reanalysis(chat_id: int) -> int:
    """เปิดข้อความลูกค้าล่าสุดของแชทให้ AI อ่านใหม่: ข้อความลูกค้าที่ใหม่กว่าข้อความทีมงานล่าสุด (ไม่เกิน 20)
    ถ้าทีมงานตอบไปแล้ว ใช้ข้อความลูกค้าล่าสุด 1 ข้อความ · คืนค่าจำนวนข้อความที่เปิด"""
    with SessionLocal() as db:
        last_out = db.scalar(select(func.max(Message.id)).where(Message.chat_id == chat_id, Message.is_outgoing.is_(True)))
        query = select(Message).where(Message.chat_id == chat_id, Message.is_outgoing.is_(False))
        if last_out:
            query = query.where(Message.id > last_out)
        msgs = list(db.scalars(query.order_by(Message.id.desc()).limit(REANALYZE_MAX_MESSAGES)))
        if not msgs:
            msgs = list(db.scalars(select(Message).where(Message.chat_id == chat_id, Message.is_outgoing.is_(False))
                                   .order_by(Message.id.desc()).limit(1)))
        for m in msgs:
            m.analyzed = False
        db.commit()
        return len(msgs)


async def _run_reanalysis(user, chat_id: int, focus_ticket_id: int = 0) -> dict:
    """ให้ AI วิเคราะห์แชทนี้ใหม่ (ทั้งร่างคำตอบลูกค้า และร่างข้อความปัญหาของ ticket ที่ยังไม่ได้ส่งเข้ากลุ่ม)
    คืนค่า dict: error | summary, seconds, new_reply_id, new_status, redrafted (id ticket ที่ร่างข้อความปัญหาใหม่)"""
    with SessionLocal() as db:
        before = db.scalar(select(func.max(Reply.id)).where(Reply.chat_id == chat_id)) or 0
    n = _reopen_for_reanalysis(chat_id)
    started_at = utcnow()
    started = time.perf_counter()
    summary = await analyzer.analyze(chat_id, reanalysis=True, focus_ticket_id=focus_ticket_id)
    seconds = round(time.perf_counter() - started, 1)
    if summary.startswith("ผิดพลาด"):
        return {"error": summary}
    with SessionLocal() as db:
        new = db.scalar(select(Reply).where(Reply.chat_id == chat_id, Reply.id > before).order_by(Reply.id.desc()).limit(1))
        new_id, new_status = (new.id, new.status) if new else (None, "")
        redrafted = sorted({e.ticket_id for e in db.scalars(
            select(TicketEvent).join(Ticket, Ticket.id == TicketEvent.ticket_id)
            .where(Ticket.chat_id == chat_id, TicketEvent.kind == "ai_summary", TicketEvent.created_at >= started_at,
                   TicketEvent.body.like(analyzer.REANALYSIS_EVENT + "%" + analyzer.REANALYSIS_REDRAFTED)))})
    log.info("%s สั่งวิเคราะห์แชท %s ใหม่ (%s ข้อความ): %s", user.username, chat_id, n, summary)
    return {"summary": summary, "seconds": seconds, "new_reply_id": new_id, "new_status": new_status, "redrafted": redrafted}


def _redraft_text(redrafted: list[int]) -> str:
    return (" · ร่างข้อความปัญหา " + ", ".join(f"ticket #{i}" for i in redrafted) + " ใหม่แล้ว (ดูที่แท็บ ส่งเข้ากลุ่ม)") if redrafted else ""


@app.post("/api/replies/{reply_id}/reanalyze")
async def api_reply_reanalyze(request: Request, reply_id: int):
    """วิเคราะห์แชทนี้ใหม่ (กรณี AI เข้าใจผิด): ให้ AI อ่านข้อความล่าสุดของลูกค้าอีกรอบ แล้วร่างคำตอบใหม่แทนร่างเดิม
    และร่างข้อความปัญหาของ ticket ที่ยังไม่ได้ส่งเข้ากลุ่มโปรแกรมเมอร์ใหม่ด้วย (นับโควตา AI 1 ครั้ง)
    AI ผิดพลาด/เห็นว่าไม่ต้องตอบ = ร่างเดิมยังอยู่"""
    user = current_user(request, "agent")
    reply = _load_pending(reply_id)
    if not reply or reply.kind != "ai":
        return JSONResponse({"ok": False, "error": "ร่างนี้ถูกดำเนินการไปแล้ว หรือไม่ใช่ร่างตอบลูกค้า"}, status_code=409)
    out = await _run_reanalysis(user, reply.chat_id, focus_ticket_id=reply.ticket_id or 0)
    if "error" in out:
        return JSONResponse({"ok": False, "error": f"AI วิเคราะห์ไม่สำเร็จ ร่างเดิมยังอยู่ · {out['error']}"}, status_code=502)
    extra = _redraft_text(out["redrafted"])
    if out["new_reply_id"] is None:
        msg = f"AI วิเคราะห์ใหม่แล้ว ({out['summary']}) · ไม่มีร่างคำตอบใหม่ ร่างเดิมยังอยู่" + extra
        if out["redrafted"]:
            flash(request, msg)
        return JSONResponse({"ok": True, "new_reply_id": None, "redrafted": out["redrafted"], "seconds": out["seconds"],
                             "message": msg, "reload": bool(out["redrafted"])})
    msg = ("AI วิเคราะห์ใหม่แล้ว และส่งคำตอบถึงลูกค้าอัตโนมัติแล้ว (โหมดตอบทันที)" if out["new_status"] == "sent"
           else "AI วิเคราะห์ใหม่แล้ว ได้ร่างคำตอบใหม่ ตรวจก่อนกดอนุมัติ") + extra
    flash(request, msg)
    return JSONResponse({"ok": True, "new_reply_id": out["new_reply_id"], "status": out["new_status"], "redrafted": out["redrafted"],
                         "seconds": out["seconds"], "message": msg, "reload": True})


@app.post("/api/tickets/{ticket_id}/reanalyze")
async def api_ticket_reanalyze(request: Request, ticket_id: int):
    """จากแท็บ "ส่งเข้ากลุ่ม": วิเคราะห์แชทของ ticket นี้ใหม่ แล้วร่างข้อความปัญหาที่จะโพสต์เข้ากลุ่มใหม่ (ticket ต้องยังรออนุมัติ)"""
    user = current_user(request, "agent")
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        chat_id = ticket.chat_id if ticket and ticket.dev_status == "pending" else None
    if chat_id is None:
        return JSONResponse({"ok": False, "error": "ticket นี้ไม่ได้รออนุมัติส่งเข้ากลุ่มแล้ว"}, status_code=409)
    out = await _run_reanalysis(user, chat_id, focus_ticket_id=ticket_id)
    if "error" in out:
        return JSONResponse({"ok": False, "error": f"AI วิเคราะห์ไม่สำเร็จ ข้อความเดิมยังอยู่ · {out['error']}"}, status_code=502)
    done = ticket_id in out["redrafted"]
    msg = (f"AI วิเคราะห์ใหม่แล้ว ร่างข้อความปัญหา ticket #{ticket_id} ใหม่แล้ว ตรวจก่อนกดอนุมัติส่งเข้ากลุ่ม" if done
           else f"AI วิเคราะห์ใหม่แล้ว แต่ไม่ได้ร่างข้อความ ticket #{ticket_id} ใหม่ ({out['summary']}) · ข้อความเดิมยังอยู่")
    if out["new_reply_id"] and out["new_status"] != "sent":
        msg += " · มีร่างคำตอบลูกค้าใหม่ที่แท็บ ตอบลูกค้า"
    if done:
        flash(request, msg)
    return JSONResponse({"ok": True, "redrafted": done, "ticket_id": ticket_id, "seconds": out["seconds"], "message": msg})


@app.post("/replies/{reply_id}/regenerate")
async def replies_regenerate(request: Request, reply_id: int, instruction: str = Form(...),
                             text: str = Form("")):
    current_user(request, "agent")
    home = _replies_home(reply_id)
    try:
        new_text = await _rewrite_draft(reply_id, instruction, text)
    except ai_service.AIError as e:
        flash(request, str(e), "error")
        return back(home)
    if new_text is None:
        return back(home)
    flash(request, "AI เขียนคำตอบใหม่แล้ว ตรวจสอบก่อนกดอนุมัติ")
    return back(home)


class RewriteIn(BaseModel):
    instruction: str
    text: str = ""


@app.post("/api/replies/{reply_id}/rewrite")
async def api_reply_rewrite(request: Request, reply_id: int, body: RewriteIn):
    """ให้ AI เขียนร่างใหม่โดยไม่ต้องโหลดหน้าใหม่ (หน้ารออนุมัติแสดงสถานะ "กำลังเขียน" แล้วใส่ข้อความที่ได้ในช่องเดิม)"""
    current_user(request, "agent")
    instruction = body.instruction.strip()
    if not instruction:
        return JSONResponse({"ok": False, "error": "ยังไม่ได้ใส่คำสั่ง"}, status_code=400)
    started = time.perf_counter()
    try:
        new_text = await _rewrite_draft(reply_id, instruction[:500], body.text)
    except ai_service.AIError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=502)
    if new_text is None:
        return JSONResponse({"ok": False, "error": "ข้อความนี้ถูกดำเนินการไปแล้ว"}, status_code=409)
    return JSONResponse({"ok": True, "text": new_text, "seconds": round(time.perf_counter() - started, 1)})


# ---------------------------------------------------------------- LINE Chat Bot (ลูกค้าทักผ่าน LINE OA ที่แยกจาก LINE แจ้งเตือนอนุมัติ)
@app.post("/linebot/webhook")
async def linebot_webhook(request: Request):
    """LINE ส่งข้อความของลูกค้ามาที่นี่ (ตรวจลายเซ็นด้วย Channel secret ของ OA บอท) · ตอบ 200 ทันทีแล้วประมวลผลต่อเบื้องหลัง"""
    body = await request.body()
    if len(body) > 1_000_000:
        return JSONResponse({"error": "too large"}, status_code=413)
    if not linebot.verify(body, request.headers.get("x-line-signature", "")):
        return JSONResponse({"error": "bad signature"}, status_code=403)
    try:
        events = json.loads(body).get("events", [])
    except (json.JSONDecodeError, AttributeError):
        events = []
    if isinstance(events, list) and events:
        linebot.spawn([e for e in events if isinstance(e, dict)])
    return JSONResponse({"ok": True})


def _bot_answer_is_safe(reply: Reply) -> bool:
    """โหมดตอบอัตโนมัติ: ส่งเองเฉพาะ (1) ตอบจากเนื้อหาของบอท (2) ตอบทักทายด้วยข้อความต้อนรับ
    (3) ข้อความ "ขอตรวจสอบ" มาตรฐานเมื่อไม่มีในเนื้อหา (ลูกค้าไม่เงียบ ทีมงานตอบต่อจากรายการคำถามที่ตอบไม่ได้)
    ที่เหลือ (แจ้งปัญหา / AI ตอบเองนอกเนื้อหา) ต้องผ่านการอนุมัติ"""
    note = reply.note or ""
    if analyzer.LINE_NOTE in note or analyzer.LINE_HELLO_NOTE in note:
        return True
    return analyzer.QUESTION_NOTE in note and (reply.final_text or "").strip() == analyzer.GUIDE_HOLD


async def _auto_send_bot(reply_id: int) -> None:
    """โหมดตอบอัตโนมัติ: ร่างตอบลูกค้าที่ทักมาทาง LINE Chat Bot ถูกส่งทันที (ไม่ผ่านการอนุมัติ)"""
    with SessionLocal() as db:
        settings = get_settings(db)
        reply = db.get(Reply, reply_id)
        chat = db.get(Chat, reply.chat_id) if reply else None
        if not (reply and chat and chat.channel == "line" and reply.kind == "ai" and reply.status == "pending"):
            return
        if settings.get("linebot_enabled") != "1" or settings.get("linebot_mode") != "auto":
            return
        text = reply.final_text
        if settings.get("linebot_auto_only_found") != "0" and not _bot_answer_is_safe(reply):
            log.info("LINE Chat Bot: ร่าง #%s ไม่ได้ตอบจากเนื้อหาของบอท ส่งเข้าหน้ารออนุมัติแทน", reply_id)
            return
    status, msg = await deliver_reply(reply_id, text, "AI อัตโนมัติ")
    log.info("LINE Chat Bot ตอบอัตโนมัติ (ร่าง #%s): %s · %s", reply_id, status, msg)


async def _load_bot_name() -> None:
    if not linebot.configured():
        return
    try:
        linebot.set_bot_name((await linebot.bot_info()).get("displayName", ""))
    except linebot.LineBotError:
        log.warning("โหลดชื่อ LINE OA ของบอทไม่สำเร็จ")


@app.get("/linebot")
async def linebot_page(request: Request):
    user = current_user(request, "admin")
    with SessionLocal() as db:
        settings = get_settings(db)
        n_chats = db.scalar(select(func.count(Chat.id)).where(Chat.channel == "line")) or 0
        n_today = db.scalar(
            select(func.count(Message.id)).join(Chat, Chat.id == Message.chat_id)
            .where(Chat.channel == "line", Message.is_outgoing.is_(False), Message.date >= local_day_start_utc())) or 0
        n_auto = db.scalar(select(func.count(Reply.id)).where(Reply.decided_by == "AI อัตโนมัติ", Reply.status == "sent")) or 0
        # ร่างตอบลูกค้า LINE ที่ยังรอทีมงานอนุมัติ (บอทยังไม่ส่ง) · บอกให้เห็นว่าทำไมลูกค้ายังไม่ได้คำตอบ
        n_pending = db.scalar(
            select(func.count(Reply.id)).join(Chat, Chat.id == Reply.chat_id)
            .where(Chat.channel == "line", Reply.status == "pending", Reply.kind == "ai")) or 0
        n_content = db.scalar(select(func.count(LineContent.id))) or 0
        n_used = db.scalar(select(func.coalesce(func.sum(LineContent.used_count), 0))) or 0
        n_questions = db.scalar(select(func.count(LineQuestion.id)).where(LineQuestion.status == "open")) or 0
    return render(request, "linebot.html", user, settings=settings, bot_has_token=bool(linebot.token()),
                  bot_has_secret=bool(linebot.secret()), bot_ready=linebot.configured(),
                  webhook=str(request.base_url).rstrip("/") + "/linebot/webhook", public_url=line_service.public_url(),
                  n_chats=n_chats, n_today=n_today, n_auto=n_auto, same_oa=bool(
                      linebot.token() and linebot.token() == line_service.token()),
                  n_content=n_content, n_used=n_used, n_questions=n_questions, n_pending=n_pending)


@app.post("/linebot")
async def linebot_save(request: Request):
    current_user(request, "admin")
    form = await request.form()
    values = {
        "linebot_enabled": "1" if form.get("linebot_enabled") else "0",
        "linebot_mode": "auto" if form.get("linebot_mode") == "auto" else "approve",
        "linebot_greeting": str(form.get("linebot_greeting", "")).strip()[:500],
        "linebot_auto_only_found": "1" if form.get("linebot_auto_only_found") else "0",
    }
    with SessionLocal() as db:
        for key, value in values.items():
            db.merge(Setting(key=key, value=value))
        db.commit()
    flash(request, "บันทึกการตั้งค่า LINE Chat Bot แล้ว")
    return back("/linebot")


@app.post("/linebot/mode")
async def linebot_set_mode(request: Request, mode: str = Form("")):
    """สลับโหมดจากหน้ารออนุมัติ LINE (ปุ่มเดียว): auto = ตอบลูกค้าทันที · approve = ให้ทีมงานอนุมัติก่อนส่ง"""
    current_user(request, "admin")
    mode = "auto" if mode == "auto" else "approve"
    with SessionLocal() as db:
        db.merge(Setting(key="linebot_mode", value=mode))
        db.commit()
    flash(request, "เปิดโหมดตอบลูกค้าทันทีแล้ว: บอทส่งเองเฉพาะเรื่องที่ตอบจากเนื้อหาของบอท คำทักทาย และข้อความขอตรวจสอบ เรื่องอื่นรอทีมงานในหน้านี้"
          if mode == "auto" else "กลับไปโหมดให้ทีมงานอนุมัติก่อนส่งแล้ว")
    return back("/linebot/replies")


@app.post("/linebot/check")
async def linebot_check(request: Request):
    """ตรวจว่า LINE_BOT_CHANNEL_ACCESS_TOKEN ใช้ได้จริง (ขอข้อมูลบอทจาก LINE)"""
    current_user(request, "admin")
    if not linebot.configured():
        flash(request, "ยังไม่ได้ตั้ง LINE_BOT_CHANNEL_ACCESS_TOKEN และ LINE_BOT_CHANNEL_SECRET ใน Variables ของ Railway", "error")
        return back("/linebot")
    try:
        info = await linebot.bot_info()
    except linebot.LineBotError as e:
        flash(request, f"เชื่อมต่อ LINE OA ของบอทไม่สำเร็จ: {e}", "error")
        return back("/linebot")
    linebot.set_bot_name(info.get("displayName", ""))
    flash(request, f"เชื่อมต่อ OA \"{info.get('displayName', '')}\" {info.get('basicId', '')} ได้แล้ว")
    return back("/linebot")


# ---------------------------------------------------------------- เนื้อหาบอท LINE (แยกจากคู่มือตอบคำถามของ Telegram โดยสิ้นเชิง)
def _line_content_page(request: Request, user: User, q: str = "", **extra):
    with SessionLocal() as db:
        query = select(LineContent).order_by(LineContent.updated_at.desc())
        if q.strip():
            like = f"%{q.strip()}%"
            query = query.where(LineContent.title.ilike(like) | LineContent.keywords.ilike(like)
                                | LineContent.answer.ilike(like))
        items = list(db.scalars(query))
        questions = list(db.scalars(select(LineQuestion).where(LineQuestion.status == "open")
                                    .order_by(LineQuestion.created_at.desc()).limit(50)))
        total = db.scalar(select(func.count(LineContent.id))) or 0
        used = db.scalar(select(func.coalesce(func.sum(LineContent.used_count), 0))) or 0
        chat_titles = {c.id: c.title for c in db.scalars(select(Chat).where(Chat.channel == "line"))}
        data = {g.id: {"title": g.title, "keywords": g.keywords, "answer": g.answer, "images": guide_images(g)}
                for g in db.scalars(select(LineContent))}
    return render(request, "linebot_content.html", user, items=items, questions=questions, q=q, total=total, used=used,
                  chat_titles=chat_titles, content_data=data, max_images=GUIDE_MAX_IMAGES, **extra)


@app.get("/linebot/content")
async def linebot_content_page(request: Request, q: str = ""):
    return _line_content_page(request, current_user(request, "admin"), q, test=None, test_q="")


@app.get("/linebot/content/new")
async def linebot_content_new(request: Request, question_id: int = 0):
    current_user(request, "admin")
    return back("/linebot/content?new=1" + (f"&question_id={question_id}" if question_id else ""))


@app.post("/linebot/content/new")
async def linebot_content_create(request: Request, title: str = Form(""), keywords: str = Form(""),
                                 answer: str = Form(""), question_id: int = Form(0),
                                 images: list[UploadFile] = File([])):
    user = current_user(request, "admin")
    try:
        title, keywords, answer = _clean_guide(title, keywords, answer)
    except ValueError as e:
        flash(request, str(e), "error")
        return back("/linebot/content?new=1" + (f"&question_id={question_id}" if question_id else ""))
    saved, errors = await _save_guide_images(images, GUIDE_MAX_IMAGES)
    with SessionLocal() as db:
        item = LineContent(title=title, keywords=keywords, answer=answer, updated_by=user.username,
                           images=json.dumps(saved) if saved else "")
        db.add(item)
        db.flush()
        if question_id and (question := db.get(LineQuestion, question_id)):
            question.status, question.content_id = "added", item.id
        db.commit()
    for err in errors:
        flash(request, err, "error")
    flash(request, f"เพิ่มเนื้อหา \"{title}\" แล้ว" + (f" พร้อมรูป {len(saved)} รูป" if saved else "")
          + " บอท LINE จะใช้ตอบลูกค้าตั้งแต่ข้อความถัดไป")
    return back("/linebot/content")


@app.post("/linebot/content/ask")
async def linebot_content_ask(request: Request):
    """ลองถามบอท: ให้ AI ตอบจากเนื้อหาของบอทจริง (ใช้โควตา AI 1 ครั้ง) แต่ไม่ส่งถึงลูกค้า ไม่บันทึกแชท ไม่นับว่าเนื้อหาถูกใช้"""
    user = current_user(request, "admin")
    form = await request.form()
    q = str(form.get("test_q", "")).strip()[:500]
    if not q:
        flash(request, "พิมพ์คำถามที่อยากลองถามบอทก่อน", "error")
        return back("/linebot/content")
    with SessionLocal() as db:
        settings = get_settings(db)
        contents = list(db.scalars(select(LineContent).order_by(LineContent.id)))
    msg = Message(id=-1, chat_id=0, tg_message_id=1, sender_name="ลูกค้า (ทดลอง)", text=q, date=utcnow(), is_outgoing=False)
    started = time.perf_counter()
    try:
        result = await ai_service.analyze_chat(settings, "ทดลองถามบอท LINE", [msg], [msg], [], guides=contents, channel="line")
    except ai_service.AIError as e:
        flash(request, f"ลองถามบอทไม่สำเร็จ: {e}", "error")
        return back("/linebot/content")
    hit = next((g for g in contents if g.id == result.guide_id), None) if result.answered_from_guide else None
    answer = result.reply_text.strip() if result.needs_reply else ""
    if hit and not answer:
        answer = hit.answer
    if not hit and result.question.strip() and not answer:
        answer = analyzer.GUIDE_HOLD
    test = {
        "found": bool(hit), "content_id": hit.id if hit else 0, "content_title": hit.title if hit else "",
        "pics": len(guide_images(hit)) if hit else 0, "answer": answer, "question": result.question.strip(),
        "is_issue": result.is_issue, "seconds": round(time.perf_counter() - started, 1), "n_content": len(contents),
    }
    return _line_content_page(request, user, test=test, test_q=q)


@app.post("/linebot/content/questions/{question_id}/dismiss")
async def linebot_question_dismiss(request: Request, question_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        if question := db.get(LineQuestion, question_id):
            question.status = "dismissed"
            db.commit()
    return back("/linebot/content")


@app.get("/linebot/content/{content_id}")
async def linebot_content_edit(request: Request, content_id: int):
    current_user(request, "admin")
    return back(f"/linebot/content?edit={content_id}")


@app.post("/linebot/content/{content_id}")
async def linebot_content_update(request: Request, content_id: int, title: str = Form(""), keywords: str = Form(""),
                                 answer: str = Form(""), remove_images: list[str] = Form([]),
                                 images: list[UploadFile] = File([])):
    user = current_user(request, "admin")
    try:
        title, keywords, answer = _clean_guide(title, keywords, answer)
    except ValueError as e:
        flash(request, str(e), "error")
        return back(f"/linebot/content?edit={content_id}")
    with SessionLocal() as db:
        item = db.get(LineContent, content_id)
        if not item:
            return back("/linebot/content")
        current = guide_images(item)
        removed = [p for p in current if p in remove_images]
        kept = [p for p in current if p not in removed]
        saved, errors = await _save_guide_images(images, GUIDE_MAX_IMAGES - len(kept))
        item.title, item.keywords, item.answer = title, keywords, answer
        item.images = json.dumps(kept + saved) if kept or saved else ""
        item.updated_by, item.updated_at = user.username, utcnow()
        db.commit()
        _drop_guide_files(db, removed)
    for err in errors:
        flash(request, err, "error")
    flash(request, "บันทึกเนื้อหาแล้ว")
    return back("/linebot/content")


@app.post("/linebot/content/{content_id}/delete")
async def linebot_content_delete(request: Request, content_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        if item := db.get(LineContent, content_id):
            files = guide_images(item)
            title = item.title
            db.delete(item)
            db.commit()
            _drop_guide_files(db, files)
            flash(request, f"ลบเนื้อหา \"{title}\" แล้ว")
    return back("/linebot/content")

# ---------------------------------------------------------------- สมอง AI (โน้ตที่ AI เรียนรู้จากการทำงานจริง)
BRAIN_FILTERS = {"all": "ทั้งหมด", "global": "ทั่วไป", "chat": "เฉพาะแชท", "auto": "AI จดเอง", "manual": "แอดมินเขียน",
                 "disabled": "ปิดอยู่"}


def _brain_conditions(f: str) -> list:
    return {
        "global": [AiNote.scope == "global", AiNote.status == "active"],
        "chat": [AiNote.scope == "chat", AiNote.status == "active"],
        "auto": [AiNote.source == "auto"],
        "manual": [AiNote.source == "manual"],
        "disabled": [AiNote.status == "disabled"],
    }.get(f, [])


@app.get("/brain")
async def brain_page(request: Request, f: str = "all", q: str = ""):
    user = current_user(request, "admin")
    f = f if f in BRAIN_FILTERS else "all"
    with SessionLocal() as db:
        settings = get_settings(db)
        query = select(AiNote).where(*_brain_conditions(f))
        if q.strip():
            like = f"%{q.strip()}%"
            query = query.where(AiNote.title.ilike(like) | AiNote.body.ilike(like))
        notes = list(db.scalars(query.order_by(AiNote.pinned.desc(), AiNote.status, AiNote.updated_at.desc()).limit(300)))
        counts = {k: db.scalar(select(func.count(AiNote.id)).where(*_brain_conditions(k))) or 0 for k in BRAIN_FILTERS}
        chat_titles = {c.id: c.title for c in db.scalars(select(Chat))}
        chats = list(db.scalars(select(Chat).order_by(Chat.title)))
        runs = list(db.scalars(select(LearnRun).order_by(LearnRun.id.desc()).limit(12)))
        since = utcnow() - timedelta(hours=24)
        new_24h = db.scalar(select(func.count(AiNote.id)).where(AiNote.source == "auto", AiNote.created_at >= since)) or 0
        uses = db.scalar(select(func.coalesce(func.sum(AiNote.uses), 0))) or 0
        cursor = learning.load_cursor(settings.get("learn_cursor", ""))
        pending = learning.collect_signals(db, cursor)["count"]
    model = learning.valid_model(settings)
    q_row = next((r for r in quota.snapshot(settings)["rows"] if r["model"] == model), None)
    last_ok = next((r for r in runs if r.status == "ok"), None)
    note_data = {n.id: {"title": n.title, "body": n.body, "kind": n.kind, "scope": n.scope,
                        "chat_id": n.chat_id or "", "confidence": n.confidence} for n in notes}
    return render(request, "brain.html", user, notes=notes, f=f, q=q, filters=BRAIN_FILTERS, counts=counts,
                  chat_titles=chat_titles, chats=chats, runs=runs, new_24h=new_24h, uses=uses, pending=pending,
                  next_after=learning._parse_iso(cursor.get("next_after")), last_ok=last_ok, learn_model=model,
                  learn_quota=q_row, kinds=learning.KINDS, note_data=note_data, learn_settings=settings,
                  learning_busy=learning._lock.locked())


@app.post("/brain/settings")
async def brain_settings(request: Request):
    current_user(request, "admin")
    form = await request.form()
    values = {key: "1" if form.get(key) else "0" for key in ("auto_learn", "learn_use_notes", "learn_ticket_notes")}
    model = str(form.get("learn_model", ""))
    with SessionLocal() as db:
        known_models = ai_service.all_models(get_settings(db))
    if model in known_models:
        values["learn_model"] = model
    for key, low, high in (("learn_interval_hours", 1, 72), ("learn_min_signals", 1, 50), ("learn_max_notes", 20, 500)):
        raw = str(form.get(key, "")).strip()
        if raw.isdigit():
            values[key] = str(max(low, min(high, int(raw))))
    with SessionLocal() as db:
        for key, value in values.items():
            db.merge(Setting(key=key, value=value))
        db.commit()
    flash(request, "บันทึกการตั้งค่าสมอง AI แล้ว")
    return back("/brain")


@app.post("/brain/learn")
async def brain_learn(request: Request):
    """แอดมินกดให้ AI เรียนรู้ตอนนี้ (ไม่รอรอบอัตโนมัติ)"""
    current_user(request, "admin")
    result = await learning.run_learning("manual")
    status = result.get("status")
    flash(request, ("AI เรียนรู้เสร็จแล้ว · " if status == "ok" else "") + str(result.get("message", "")),
          "ok" if status == "ok" else "error")
    return back("/brain")


def _note_fields(form) -> dict | str:
    """อ่านและตรวจฟอร์มโน้ต -> dict หรือข้อความข้อผิดพลาด"""
    title, body = str(form.get("title", "")).strip()[:learning.TITLE_MAX], str(form.get("body", "")).strip()[:1000]
    kind = str(form.get("kind", "lesson"))
    scope = str(form.get("scope", "global"))
    if not title or not body:
        return "ต้องมีหัวข้อและเนื้อหาโน้ต"
    if kind not in learning.KINDS or scope not in learning.SCOPES:
        return "ประเภทหรือขอบเขตของโน้ตไม่ถูกต้อง"
    chat_id = None
    if scope == "chat":
        raw = str(form.get("chat_id", "")).strip().lstrip("-")
        if not raw.isdigit():
            return "เลือกแชทสำหรับโน้ตเฉพาะแชท"
        chat_id = int(str(form.get("chat_id")).strip())
        with SessionLocal() as db:
            if db.get(Chat, chat_id) is None:
                return "ไม่พบแชทที่เลือก"
    try:
        confidence = max(1, min(5, int(form.get("confidence", 5))))
    except (TypeError, ValueError):
        confidence = 5
    return {"title": title, "body": body, "kind": kind, "scope": scope, "chat_id": chat_id, "confidence": confidence}


@app.post("/brain/notes")
async def brain_note_create(request: Request):
    current_user(request, "admin")
    fields = _note_fields(await request.form())
    if isinstance(fields, str):
        flash(request, fields, "error")
        return back("/brain")
    with SessionLocal() as db:
        db.add(AiNote(source="manual", evidence="แอดมินเขียนเอง", **fields))
        db.commit()
    flash(request, "เพิ่มโน้ตแล้ว AI จะใช้ตั้งแต่การวิเคราะห์ครั้งถัดไป")
    return back("/brain")


@app.post("/brain/notes/{note_id}")
async def brain_note_update(request: Request, note_id: int):
    current_user(request, "admin")
    fields = _note_fields(await request.form())
    if isinstance(fields, str):
        flash(request, fields, "error")
        return back("/brain")
    with SessionLocal() as db:
        note = db.get(AiNote, note_id)
        if note:
            for key, value in fields.items():
                setattr(note, key, value)
            note.source = "manual"  # แอดมินแก้แล้ว = ล็อก AI จะไม่แก้หรือปิดโน้ตนี้เอง
            db.commit()
            flash(request, "บันทึกโน้ตแล้ว (AI จะไม่แก้โน้ตนี้เองอีก)")
    return back("/brain")


@app.post("/brain/notes/{note_id}/toggle")
async def brain_note_toggle(request: Request, note_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        if note := db.get(AiNote, note_id):
            note.status = "disabled" if note.status == "active" else "active"
            db.commit()
    return back("/brain")


@app.post("/brain/notes/{note_id}/pin")
async def brain_note_pin(request: Request, note_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        if note := db.get(AiNote, note_id):
            note.pinned = not note.pinned
            if note.pinned:
                note.status = "active"
            db.commit()
    return back("/brain")


@app.post("/brain/notes/{note_id}/delete")
async def brain_note_delete(request: Request, note_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        if note := db.get(AiNote, note_id):
            db.delete(note)
            db.commit()
            flash(request, f"ลบโน้ต \"{note.title}\" แล้ว")
    return back("/brain")


# ---------------------------------------------------------------- tickets
@app.get("/tickets")
async def tickets_page(request: Request, status: str = "active", category: str = "", q: str = ""):
    user = current_user(request, "programmer", "agent")
    q = q.strip()
    with SessionLocal() as db:
        query = select(Ticket).order_by(Ticket.created_at.desc()).limit(300)
        if status == "active":
            query = query.where(Ticket.status.in_(analyzer.OPEN_STATUSES))
        elif status != "all":
            query = query.where(Ticket.status == status)
        if category:
            query = query.where(Ticket.category == category)
        if q:
            like = f"%{q}%"
            query = query.where(
                Ticket.title.ilike(like) | Ticket.customer_name.ilike(like) | Ticket.summary.ilike(like)
                | Ticket.website_url.ilike(like) | (Ticket.id == (int(q.lstrip("#")) if q.lstrip("#").isdigit() else -1))
            )
        tickets = list(db.scalars(query))
        status_counts = dict(db.execute(select(Ticket.status, func.count(Ticket.id)).group_by(Ticket.status)).all())
        chat_titles = {c.id: c.title for c in db.scalars(select(Chat))}
        users = {u.id: u.username for u in db.scalars(select(User))}
    status_counts["active"] = sum(status_counts.get(k, 0) for k in analyzer.OPEN_STATUSES)
    status_counts["all"] = sum(v for k, v in status_counts.items() if k in TICKET_STATUSES)
    return render(request, "tickets.html", user, tickets=tickets, status=status, category=category, q=q,
                  chat_titles=chat_titles, users=users, status_counts=status_counts)


@app.get("/tickets/{ticket_id}")
async def ticket_detail(request: Request, ticket_id: int):
    user = current_user(request, "programmer", "agent")
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        if not ticket:
            return back("/tickets")
        events, attachments = list(ticket.events), list(ticket.attachments)
        chat = db.get(Chat, ticket.chat_id)
        users = list(db.scalars(select(User).order_by(User.username)))
        replies = list(db.scalars(select(Reply).where(Reply.ticket_id == ticket_id).order_by(Reply.created_at)))
        dev_posted = db.scalar(select(func.count(TicketLink.id)).where(TicketLink.ticket_id == ticket_id))
    return render(request, "ticket_detail.html", user, ticket=ticket, timeline=build_timeline(events, attachments),
                  attachments=attachments, chat=chat, users=users, replies=replies, dev_posted=dev_posted,
                  dev_group_set=bool(telegram.dev_group_id))


def build_timeline(events, attachments) -> list[dict]:
    """จัดประวัติ ticket: ข้อความลูกค้าที่ส่งติดกัน (คนเดียวกัน ห่างกันไม่เกิน 10 นาที) รวมเป็นก้อนเดียว พร้อมรูป"""
    # ประวัติเก่าที่ยังไม่มี media_path: จับคู่รูปแนบตามลำดับ (คำบรรยายรูป = ข้อความเดียวกัน)
    unused = list(attachments)
    groups: list[dict] = []
    for e in events:
        media = e.media_path
        if e.kind == "customer_message" and not media:
            caption = "" if e.body == "(รูปภาพ)" else e.body
            # รูปเปล่า: คำบรรยายว่าง / รูปพร้อมข้อความ: คำบรรยายตรงกับข้อความ
            match = next((a for a in unused if (a.caption or "") == caption), None) if (caption or e.body) else None
            if match:
                unused.remove(match)
                media = match.media_path
        elif media:
            unused = [a for a in unused if a.media_path != media]
        body = "" if e.body == "(รูปภาพ)" and media else e.body
        last = groups[-1] if groups else None
        if (last and e.kind == "customer_message" and last["kind"] == e.kind and last["author"] == e.author
                and (e.created_at - last["last_at"]).total_seconds() <= 600):
            last["items"].append({"body": body, "media": media})
            last["last_at"] = e.created_at
            continue
        groups.append({"kind": e.kind, "author": e.author, "created_at": e.created_at, "last_at": e.created_at,
                       "items": [{"body": body, "media": media}]})
    return groups


def _safe_next(value: str, default: str) -> str:
    return value if value.startswith("/") and not value.startswith("//") else default


@app.post("/tickets/{ticket_id}/post-dev")
async def ticket_post_dev(request: Request, ticket_id: int, text: str = Form(""), next: str = Form(""),
                          photo_edit: str = Form(""), media: list[str] = Form([]),
                          images: list[UploadFile] = File([])):
    """อนุมัติและส่ง ticket เข้ากลุ่มโปรแกรมเมอร์ (text = ข้อความที่แอดมินแก้)
    · photo_edit=1: ส่งเฉพาะรูปเดิมที่ติ๊กไว้ (media) + รูปที่เพิ่มใหม่ (images) · ไม่ส่ง photo_edit = ส่งรูปของ ticket ทั้งหมดเหมือนเดิม"""
    user = current_user(request, "agent")
    photos: list[str] | None = None
    notes: list[str] = []
    if photo_edit == "1":
        with SessionLocal() as db:
            ticket = db.get(Ticket, ticket_id)
            own = [a.media_path for a in ticket.attachments] if ticket else []
        chosen = [m for m in dict.fromkeys(media) if m in own][:dev_bridge.MAX_PHOTOS]
        saved, notes = await _save_guide_images(images, max(dev_bridge.MAX_PHOTOS - len(chosen), 0),
                                                prefix="devphoto", max_total=dev_bridge.MAX_PHOTOS)
        if saved:
            with SessionLocal() as db:
                for name in saved:
                    db.add(TicketAttachment(ticket_id=ticket_id, media_path=name, caption=f"เพิ่มโดย {user.username}"))
                db.commit()
        photos = chosen + saved
    try:
        result = await dev_bridge.post_ticket(ticket_id, force=True, text=text, approver=user.username, photos=photos)
    except Exception as e:  # noqa: BLE001
        log.exception("post ticket to dev group failed")
        result = f"ส่งไม่สำเร็จ: {e}"
    shown = result or "ส่งแล้ว"
    if notes:
        shown += " · " + "; ".join(notes)
    flash(request, shown, "ok" if result.startswith("ส่งเข้ากลุ่ม") else "error")
    return back(_safe_next(next, f"/tickets/{ticket_id}"))


@app.post("/tickets/{ticket_id}/skip-dev")
async def ticket_skip_dev(request: Request, ticket_id: int, next: str = Form("")):
    user = current_user(request, "agent")
    if dev_bridge.skip_ticket(ticket_id, user.username):
        flash(request, f"ไม่ส่ง ticket #{ticket_id} เข้ากลุ่มโปรแกรมเมอร์")
    return back(_safe_next(next, f"/tickets/{ticket_id}"))


@app.post("/tickets/{ticket_id}/update")
async def ticket_update(request: Request, ticket_id: int, status: str = Form(...), severity: str = Form(...),
                        assignee_id: str = Form(""), website_url: str = Form(""), title: str = Form(...)):
    user = current_user(request, "programmer", "agent")
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        if ticket:
            changes = []
            old_status = ticket.status
            if status in TICKET_STATUSES and status != ticket.status:
                changes.append(f"สถานะ: {TICKET_STATUSES[ticket.status]} → {TICKET_STATUSES[status]}")
                ticket.status = status
            if severity in SEVERITY_LABELS and severity != ticket.severity:
                changes.append(f"ความรุนแรง: {SEVERITY_LABELS[ticket.severity]} → {SEVERITY_LABELS[severity]}")
                ticket.severity = severity
            assignee = db.get(User, int(assignee_id)) if assignee_id.isdigit() else None
            new_assignee = assignee.id if assignee else None
            if new_assignee != ticket.assignee_id:
                changes.append(f"ผู้รับผิดชอบ: {assignee.username if assignee else '-'}")
                ticket.assignee_id = new_assignee
            ticket.website_url, ticket.title = website_url.strip(), title.strip() or ticket.title
            if changes:
                db.add(TicketEvent(ticket_id=ticket_id, kind="status", author=user.username, body="\n".join(changes)))
            db.commit()
            flash(request, "บันทึก ticket แล้ว")
            notice = sync_resolved_notice(db, ticket, user.username) if ticket.status != old_status else ""
            if notice == "created":
                flash(request, "ร่างข้อความแจ้งลูกค้าว่าแก้ไขเรียบร้อยแล้ว รออนุมัติที่หน้า \"รออนุมัติ\"")
            elif notice == "cancelled":
                flash(request, "ยกเลิกข้อความแจ้งลูกค้าที่ยังไม่ได้ส่ง เพราะ ticket ยังไม่ได้แก้ไขเสร็จ")
    return back(f"/tickets/{ticket_id}")




def delete_tickets(db, ticket_ids: list[int]) -> int:
    """ลบ ticket พร้อมประวัติ รูปแนบ และลิงก์ในกลุ่มโปรแกรมเมอร์
    ข้อความถึงลูกค้าที่ผูกกับ ticket: ร่างแจ้งความคืบหน้าที่ยังไม่ส่งจะถูกยกเลิก ส่วนข้อความอื่นยังเก็บไว้เป็นประวัติ
    (ไฟล์รูปไม่ลบ เพราะยังใช้แสดงในประวัติแชท)"""
    deleted = 0
    for ticket_id in ticket_ids:
        ticket = db.get(Ticket, ticket_id)
        if not ticket:
            continue
        for r in db.scalars(select(Reply).where(Reply.ticket_id == ticket_id)):
            if r.kind != "ai" and r.status in ("pending", "failed"):
                r.status = "superseded"
            r.ticket_id = None
        for link in db.scalars(select(TicketLink).where(TicketLink.ticket_id == ticket_id)):
            db.delete(link)
        db.delete(ticket)
        deleted += 1
    db.commit()
    return deleted


@app.post("/tickets/{ticket_id}/delete")
async def ticket_delete(request: Request, ticket_id: int):
    current_user(request, "admin")
    with SessionLocal() as db:
        n = delete_tickets(db, [ticket_id])
    flash(request, f"ลบ ticket #{ticket_id} แล้ว" if n else "ไม่พบ ticket นี้", "ok" if n else "error")
    return back("/tickets")


@app.post("/tickets/delete")
async def tickets_bulk_delete(request: Request):
    current_user(request, "admin")
    form = await request.form()
    ids = [int(v) for v in form.getlist("ids") if str(v).isdigit()]
    if not ids:
        flash(request, "ยังไม่ได้เลือก ticket", "error")
        return back("/tickets")
    with SessionLocal() as db:
        n = delete_tickets(db, ids)
    flash(request, f"ลบ ticket แล้ว {n} รายการ")
    return back("/tickets")


@app.post("/tickets/{ticket_id}/note")
async def ticket_note(request: Request, ticket_id: int, body: str = Form(...)):
    user = current_user(request, "programmer", "agent")
    if body.strip():
        with SessionLocal() as db:
            if db.get(Ticket, ticket_id):
                db.add(TicketEvent(ticket_id=ticket_id, kind="note", author=user.username, body=body.strip()))
                db.get(Ticket, ticket_id).updated_at = utcnow()
                db.commit()
    return back(f"/tickets/{ticket_id}")


def clean_website(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if " " in value or "." not in value or value.lower().startswith(("javascript:", "data:")):
        raise ValueError("ลิงก์ไม่ถูกต้อง")
    return value if value.lower().startswith(("http://", "https://")) else "https://" + value


@app.post("/tickets/{ticket_id}/website")
async def ticket_website(request: Request, ticket_id: int, website_url: str = Form(""), for_chat: str = Form("")):
    """แอดมินแก้ลิงก์เว็บไซต์ของ ticket (และตั้งเป็นเว็บประจำแชทได้) แล้วตรวจเว็บใหม่"""
    user = current_user(request, "programmer", "agent")
    try:
        url = clean_website(website_url)
    except ValueError as e:
        flash(request, str(e), "error")
        return back(f"/tickets/{ticket_id}")
    with SessionLocal() as db:
        ticket = db.get(Ticket, ticket_id)
        if not ticket:
            return back("/tickets")
        old = ticket.website_url
        ticket.website_url = url
        if not url:
            ticket.site_check = ""
        if for_chat:
            chat = db.get(Chat, ticket.chat_id)
            if chat:
                chat.website_url = url
        db.add(TicketEvent(ticket_id=ticket_id, kind="status", author=user.username,
                           body=f"แก้ลิงก์เว็บไซต์: {old or '-'} → {url or '-'}"
                                + (" (ตั้งเป็นเว็บประจำแชทนี้)" if for_chat else "")))
        db.commit()
    if url:
        result = await analyzer.run_site_check(ticket_id)
        if result and not result["ok"]:
            flash(request, f"บันทึกลิงก์แล้ว แต่เว็บไซต์เข้าไม่ได้: {result['error']}", "error")
        elif result and result.get("other_host"):
            flash(request, f"บันทึกลิงก์แล้ว แต่เว็บถูกพาไปโดเมนอื่น ({result.get('final_host')})", "error")
        else:
            flash(request, "บันทึกลิงก์เว็บไซต์แล้ว เว็บเข้าได้ปกติ")
    else:
        flash(request, "ลบลิงก์เว็บไซต์ของ ticket แล้ว")
    return back(f"/tickets/{ticket_id}")


@app.post("/chats/{chat_id}/website")
async def chat_website(request: Request, chat_id: int, website_url: str = Form("")):
    current_user(request, "agent")
    try:
        url = clean_website(website_url)
    except ValueError as e:
        flash(request, str(e), "error")
        return back(f"/chats/{chat_id}")
    with SessionLocal() as db:
        chat = db.get(Chat, chat_id)
        if chat:
            chat.website_url = url
            db.commit()
    flash(request, "บันทึกเว็บไซต์ประจำแชทแล้ว ticket ใหม่ของแชทนี้จะใช้ลิงก์นี้" if url else "ลบเว็บไซต์ประจำแชทแล้ว")
    return back(f"/chats/{chat_id}")


@app.post("/tickets/{ticket_id}/check-site")
async def ticket_check_site(request: Request, ticket_id: int):
    current_user(request, "programmer", "agent")
    result = await analyzer.run_site_check(ticket_id)
    if result is None:
        flash(request, "ticket นี้ไม่มีลิงก์เว็บไซต์", "error")
    else:
        if not result["ok"]:
            flash(request, f"เว็บไซต์เข้าไม่ได้: {result['error']}", "error")
        elif result.get("other_host"):
            flash(request, f"เว็บไซต์ถูกพาไปโดเมนอื่น ({result.get('final_host')}) โดเมนอาจหมดอายุ", "error")
        else:
            flash(request, "เว็บไซต์เข้าได้ปกติ")
    return back(f"/tickets/{ticket_id}")


# ---------------------------------------------------------------- คู่มือตอบคำถาม
@app.get("/guides")
async def guides_page(request: Request, q: str = ""):
    user = current_user(request, "agent")
    with SessionLocal() as db:
        query = select(Guide).order_by(Guide.updated_at.desc())
        if q.strip():
            like = f"%{q.strip()}%"
            query = query.where(Guide.title.ilike(like) | Guide.keywords.ilike(like) | Guide.answer.ilike(like))
        guides = list(db.scalars(query))
        questions = list(db.scalars(select(GuideQuestion).where(GuideQuestion.status == "open")
                                    .order_by(GuideQuestion.created_at.desc()).limit(50)))
        total = db.scalar(select(func.count(Guide.id)))
        used = db.scalar(select(func.coalesce(func.sum(Guide.used_count), 0)))
        chat_titles = {c.id: c.title for c in db.scalars(select(Chat))}
        all_guides = {g.id: {"title": g.title, "keywords": g.keywords, "answer": g.answer, "images": guide_images(g)}
                      for g in db.scalars(select(Guide))}
    return render(request, "guides.html", user, guides=guides, questions=questions, q=q, total=total, used=used,
                  chat_titles=chat_titles, guide_data=all_guides, max_images=GUIDE_MAX_IMAGES)


@app.get("/guides/new")
async def guide_new(request: Request, question_id: int = 0):
    current_user(request, "agent")
    return back("/guides?new=1" + (f"&question_id={question_id}" if question_id else ""))


GUIDE_MAX_IMAGES = 6
GUIDE_MAX_BYTES = 8 * 1024 * 1024
IMAGE_SIGNATURES = {b"\xff\xd8\xff": ".jpg", b"\x89PNG": ".png", b"GIF8": ".gif"}


def _image_ext(data: bytes) -> str:
    for sig, ext in IMAGE_SIGNATURES.items():
        if data.startswith(sig):
            return ext
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ""


async def _save_guide_images(files: list[UploadFile], room: int, prefix: str = "guide",
                             max_total: int = GUIDE_MAX_IMAGES) -> tuple[list[str], list[str]]:
    """บันทึกรูปที่อัปโหลด คืนค่า (ชื่อไฟล์ที่บันทึก, ข้อผิดพลาด) · prefix = คำนำหน้าชื่อไฟล์ (guide = รูปคู่มือ, devphoto = รูปเพิ่มของ ticket)"""
    saved, errors = [], []
    for f in files:
        if not f or not f.filename:
            continue
        if len(saved) >= room:
            errors.append(f"แนบรูปได้สูงสุด {max_total} รูป ข้าม {f.filename}")
            continue
        data = await f.read(GUIDE_MAX_BYTES + 1)
        if len(data) > GUIDE_MAX_BYTES:
            errors.append(f"{f.filename} ใหญ่เกิน 8 MB")
            continue
        ext = _image_ext(data)
        if not ext:
            errors.append(f"{f.filename} ไม่ใช่ไฟล์รูป (รองรับ JPG PNG GIF WEBP)")
            continue
        name = f"{prefix}_{uuid.uuid4().hex[:16]}{ext}"
        (MEDIA_DIR / name).write_bytes(data)
        saved.append(name)
    return saved, errors


def _drop_guide_files(db, names: list[str]) -> None:
    """ลบไฟล์รูปคู่มือที่ไม่ใช้แล้ว (ยกเว้นรูปที่ร่างคำตอบที่รออนุมัติยังจะส่ง)"""
    pending = " ".join(db.scalars(select(Reply.media).where(Reply.status.in_(("pending", "failed")), Reply.media != "")))
    for name in names:
        if name.startswith("guide_") and name not in pending:
            (MEDIA_DIR / name).unlink(missing_ok=True)


def _clean_guide(title: str, keywords: str, answer: str) -> tuple[str, str, str]:
    title, answer = title.strip()[:255], answer.strip()
    keywords = ", ".join(k.strip() for k in keywords.replace("\n", ",").split(",") if k.strip())
    if not title or not answer:
        raise ValueError("ต้องมีหัวข้อและคำตอบ")
    return title, keywords, answer


@app.post("/guides/new")
async def guide_create(request: Request, title: str = Form(""), keywords: str = Form(""), answer: str = Form(""),
                       question_id: int = Form(0), images: list[UploadFile] = File([])):
    user = current_user(request, "agent")
    try:
        title, keywords, answer = _clean_guide(title, keywords, answer)
    except ValueError as e:
        flash(request, str(e), "error")
        return back("/guides?new=1" + (f"&question_id={question_id}" if question_id else ""))
    saved, errors = await _save_guide_images(images, GUIDE_MAX_IMAGES)
    with SessionLocal() as db:
        guide = Guide(title=title, keywords=keywords, answer=answer, updated_by=user.username,
                      images=json.dumps(saved) if saved else "")
        db.add(guide)
        db.flush()
        if question_id and (question := db.get(GuideQuestion, question_id)):
            question.status, question.guide_id = "added", guide.id
        db.commit()
    for err in errors:
        flash(request, err, "error")
    flash(request, f"เพิ่มคู่มือ \"{title}\" แล้ว" + (f" พร้อมรูป {len(saved)} รูป" if saved else "")
          + " AI จะใช้ตอบลูกค้าตั้งแต่ข้อความถัดไป")
    return back("/guides")


@app.get("/guides/{guide_id}")
async def guide_edit(request: Request, guide_id: int):
    current_user(request, "agent")
    return back(f"/guides?edit={guide_id}")


@app.post("/guides/{guide_id}")
async def guide_update(request: Request, guide_id: int, title: str = Form(""), keywords: str = Form(""),
                       answer: str = Form(""), remove_images: list[str] = Form([]),
                       images: list[UploadFile] = File([])):
    user = current_user(request, "agent")
    try:
        title, keywords, answer = _clean_guide(title, keywords, answer)
    except ValueError as e:
        flash(request, str(e), "error")
        return back(f"/guides?edit={guide_id}")
    with SessionLocal() as db:
        guide = db.get(Guide, guide_id)
        if not guide:
            return back("/guides")
        current = guide_images(guide)
        removed = [p for p in current if p in remove_images]
        kept = [p for p in current if p not in removed]
        saved, errors = await _save_guide_images(images, GUIDE_MAX_IMAGES - len(kept))
        guide.title, guide.keywords, guide.answer = title, keywords, answer
        guide.images = json.dumps(kept + saved) if kept or saved else ""
        guide.updated_by, guide.updated_at = user.username, utcnow()
        db.commit()
        _drop_guide_files(db, removed)
    for err in errors:
        flash(request, err, "error")
    flash(request, "บันทึกคู่มือแล้ว")
    return back("/guides")


@app.post("/guides/{guide_id}/delete")
async def guide_delete(request: Request, guide_id: int):
    current_user(request, "agent")
    with SessionLocal() as db:
        if guide := db.get(Guide, guide_id):
            files = guide_images(guide)
            db.delete(guide)
            db.commit()
            _drop_guide_files(db, files)
            flash(request, f"ลบคู่มือ \"{guide.title}\" แล้ว")
    return back("/guides")


@app.post("/guides/questions/{question_id}/dismiss")
async def guide_question_dismiss(request: Request, question_id: int):
    current_user(request, "agent")
    with SessionLocal() as db:
        if question := db.get(GuideQuestion, question_id):
            question.status = "dismissed"
            db.commit()
    return back("/guides")


@app.get("/media/{name}")
async def media(request: Request, name: str):
    current_user(request)
    path = (MEDIA_DIR / name).resolve()
    if path.parent != MEDIA_DIR.resolve() or not path.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(path)


@app.get("/pub/media/{token}/{name}")
def public_media(token: str, name: str):
    """รูปสำหรับการ์ด LINE: LINE ดึงรูปโดยไม่ล็อกอิน จึงเปิดได้ด้วยลิงก์ที่เซ็นลายเซ็นและมีวันหมดอายุเท่านั้น
    (เดาลิงก์ไม่ได้ และเปิดได้เฉพาะไฟล์ที่ลายเซ็นระบุ) · แปลงเป็น JPEG/PNG ด้านยาวไม่เกิน 1024px ตามที่ LINE กำหนด"""
    path = (MEDIA_DIR / name).resolve()
    if not line_service.check_media(token, name) or path.parent != MEDIA_DIR.resolve() or not path.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    prepared = line_service.prepare_image(path)
    if not prepared:
        return JSONResponse({"error": "unsupported image"}, status_code=415)
    data, mime = prepared
    return Response(content=data, media_type=mime,
                    headers={"Cache-Control": "private, max-age=86400", "X-Robots-Tag": "noindex"})


# ---------------------------------------------------------------- settings & users
@app.get("/settings")
async def settings_page(request: Request):
    user = current_user(request, "admin")
    with SessionLocal() as db:
        settings = get_settings(db)
        users = list(db.scalars(select(User).order_by(User.username)))
    with SessionLocal() as db:
        groups = list(db.scalars(select(Chat).where(Chat.kind != "user").order_by(Chat.title)))
    suggested = next((g for g in groups if "autopay support" in g.title.lower()), None)
    return render(request, "settings.html", user, settings=settings, users=users, groups=groups, suggested=suggested,
                  limits=quota.limits(settings),
                  has_gemini_key=bool(quota.gemini_keys()),
                  gemini_keys=[(quota.slot_label(i), quota.mask_key(k), k in ai_service._bad_keys)
                               for i, k in enumerate(quota.gemini_keys(), 1)],
                  has_claude_key=bool(os.getenv("ANTHROPIC_API_KEY")))


@app.post("/settings")
async def settings_save(request: Request):
    current_user(request, "admin")
    form = await request.form()
    with SessionLocal() as db:
        known_models = ai_service.all_models(get_settings(db))
        for key in DEFAULT_SETTINGS:
            if key in ("auto_draft", "auto_ticket", "site_check", "ask_link", "ack_info", "notify_resolved", "dev_forward",
                       "dev_require_approval", "dev_watch", "ignore_bots"):
                value = "1" if form.get(key) else "0"
            elif key == "gemini_limits":
                limits = {}
                for model in quota.DEFAULT_LIMITS:
                    entry = {}
                    for field in ("rpm", "rpd"):
                        raw = str(form.get(f"{field}__{model}", "")).strip()
                        if raw.isdigit():
                            entry[field] = int(raw)
                    if entry:
                        limits[model] = entry
                value = json.dumps(limits)
            elif key == "dev_group_id":
                value = str(form.get(key, "")).strip()
                if value and not value.lstrip("-").isdigit():
                    continue
            elif key == "ai_model":
                value = str(form.get(key, ""))
                if value not in known_models:
                    continue
            elif key.startswith(("learn_", "line_", "linebot_")):
                continue  # ตั้งที่หน้าของตัวเอง (สมอง AI / แจ้งเตือน LINE) ไม่รับผ่านฟอร์มนี้
            elif key in ("debounce_seconds", "context_messages"):
                low, high = (0, 600) if key == "debounce_seconds" else (5, 100)
                value = str(int_setting({key: str(form.get(key, ""))}, key, low, high))
            elif key in form:
                value = str(form[key]).strip()
            else:
                continue
            row = db.get(Setting, key) or Setting(key=key)
            row.value = value
            db.merge(row)
        db.commit()
    dev_bridge.configure()
    if telegram.dev_group_id and telegram.connected:
        dev_bridge.schedule_backlog()
    flash(request, "บันทึกการตั้งค่าแล้ว")
    return back("/settings")


@app.post("/users")
async def users_create(request: Request, username: str = Form(...), password: str = Form(...),
                       role: str = Form(...)):
    current_user(request, "admin")
    username = username.strip()
    if role not in ROLES or not username or len(password) < 8:
        flash(request, "ข้อมูลไม่ครบ (รหัสผ่านอย่างน้อย 8 ตัวอักษร)", "error")
        return back("/settings")
    with SessionLocal() as db:
        if db.scalar(select(User).where(User.username == username)):
            flash(request, "มีชื่อผู้ใช้นี้แล้ว", "error")
            return back("/settings")
        db.add(User(username=username, password_hash=hash_password(password), role=role))
        db.commit()
    flash(request, f"เพิ่มผู้ใช้ {username} แล้ว")
    return back("/settings")


@app.post("/users/{user_id}/delete")
async def users_delete(request: Request, user_id: int):
    me = current_user(request, "admin")
    if user_id == me.id:
        flash(request, "ลบบัญชีตัวเองไม่ได้", "error")
        return back("/settings")
    with SessionLocal() as db:
        user = db.get(User, user_id)
        if user:
            for t in db.scalars(select(Ticket).where(Ticket.assignee_id == user_id)):
                t.assignee_id = None
            db.delete(user)
            db.commit()
    return back("/settings")


@app.get("/account")
async def account_page(request: Request):
    return render(request, "account.html", current_user(request))


@app.post("/account/password")
async def change_password(request: Request, current: str = Form(...), new: str = Form(...)):
    me = current_user(request)
    if not verify_password(current, me.password_hash) or len(new) < 8:
        flash(request, "รหัสผ่านเดิมไม่ถูกต้อง หรือรหัสใหม่สั้นกว่า 8 ตัวอักษร", "error")
    else:
        with SessionLocal() as db:
            db.get(User, me.id).password_hash = hash_password(new)
            db.commit()
        flash(request, "เปลี่ยนรหัสผ่านแล้ว")
    return back("/account")
