"""เชื่อมกับ LINE Messaging API: ส่งการ์ดรออนุมัติเข้า LINE และรับปุ่มกดอนุมัติ/ไม่ส่งกลับมา

ต้องตั้ง env: LINE_CHANNEL_ACCESS_TOKEN (ส่งข้อความ), LINE_CHANNEL_SECRET (ตรวจลายเซ็น webhook)
ปลายทางที่จะส่ง (line_target) จับอัตโนมัติเมื่อมีคนทักหรือเพิ่ม OA เป็นเพื่อน แล้วเก็บไว้ในตั้งค่า

การ์ดมี 2 แบบ (Flex Message):
- ตอบลูกค้า: ข้อมูลครบ ทั้งบทสนทนาล่าสุด รายละเอียด ticket หมายเหตุจาก AI และข้อความร่าง
- ticket รอส่งเข้ากลุ่มโปรแกรมเมอร์: รายละเอียด ticket ทั้งหมด
"""

import base64
import hashlib
import hmac
import io
import json
import logging
import os
import re
import secrets
import time
from pathlib import Path
from urllib.parse import quote

import httpx

log = logging.getLogger("app.line")
API = "https://api.line.me/v2/bot"

# ประเภทข้อความที่รออนุมัติ -> (หัวข้อบนการ์ด, สีหัวการ์ด)
KINDS = {
    "ai": ("ร่างตอบลูกค้า", "#0B7D52"),
    "resolved": ("แจ้งลูกค้าว่าแก้ไขเสร็จ", "#0F766E"),
    "dev_update": ("อัปเดตจากโปรแกรมเมอร์", "#B45309"),
}
DEV_COLOR = "#1D4ED8"
# สีของการ์ด (ตรงกับธีมหลังบ้าน)
INK, MUTED, PAPER, LINE_GREY = "#1C1B19", "#7D7970", "#F5F3EE", "#E3DFD5"
GREEN, GREEN_TINT, AMBER, AMBER_TINT, RED, BLUE = "#0B7D52", "#E2F8EE", "#A15C07", "#FBF0DC", "#B42318", "#1D4ED8"
# ความรุนแรงของ ticket -> สีตัวอักษร
SEV_COLOR = {"low": MUTED, "medium": BLUE, "high": AMBER, "critical": RED}
# สีของการ์ดแจ้งผลสั้นๆ: ชื่อ -> (สีตัวอักษร/ไอคอน, สีพื้นวงกลม, สัญลักษณ์)
TONES = {
    "ok": (GREEN, "#E2F8EE", "✓"),
    "bad": (RED, "#FBEAE8", "✕"),
    "warn": (AMBER, AMBER_TINT, "!"),
    "info": (BLUE, "#E8EEFC", "i"),
}
MAX_DRAFT = 1500   # ข้อความในการ์ดยาวเกินนี้ให้ตัด แล้วส่งฉบับเต็มเป็นข้อความธรรมดาแยกอีกก้อน
MAX_SUMMARY = 600  # สรุปปัญหาของ ticket
MAX_MSG = 220      # ข้อความลูกค้าแต่ละข้อความในบทสนทนา
MAX_NOTE = 400
MAX_CONV_IMAGES = 4  # รูปในบทสนทนาที่แสดงเป็นรูปจริงในการ์ด (ล่าสุดก่อน) ที่เหลือแสดงเป็น "[รูปภาพ]"
MAX_REPLY_IMAGES = 3
MAX_TICKET_IMAGES = 6
IMG_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
LINE_IMG_MAX_PX = 1024  # LINE: Flex image ด้านยาวไม่เกิน 1024px · JPEG/PNG เท่านั้น · ไม่เกิน 10MB


def token() -> str:
    return (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()


def secret() -> str:
    return (os.getenv("LINE_CHANNEL_SECRET") or "").strip()


def configured() -> bool:
    return bool(token() and secret())


def public_url() -> str:
    """ที่อยู่เว็บหลังบ้าน (ใช้ทำปุ่ม "เปิดดูในเว็บ") ตั้งเองได้ที่ PUBLIC_URL ไม่งั้นใช้โดเมนของ Railway · ว่าง = ไม่ใส่ปุ่ม"""
    url = (os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
    if not url:
        domain = (os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip()
        url = f"https://{domain}" if domain else ""
    return url if url.startswith("https://") else ""


def verify(body: bytes, signature: str) -> bool:
    """ตรวจ X-Line-Signature ว่ามาจาก LINE จริง (กันคนอื่นยิง webhook มาสั่งอนุมัติ)"""
    s = secret()
    if not s or not signature:
        return False
    mac = hmac.new(s.encode("utf-8"), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(mac).decode(), signature)


# ---------------------------------------------------------------- ผู้ที่ได้รับอนุญาตให้กดอนุมัติ/ไม่ส่ง
# เก็บเป็น LINE userId (ไม่ใช่ชื่อ เพราะเปลี่ยนได้/ซ้ำได้) เพิ่มคนด้วยรหัสเชื่อม 6 หลักที่แอดมินสร้างบนเว็บ
PAIR_TTL = 600        # รหัสเชื่อมหมดอายุใน 10 นาที
PAIR_MAX_FAILS = 5    # ใส่รหัสผิดครบกี่ครั้งแล้วยกเลิกรหัส (กันการไล่เดา)


def load_allowed(raw: str) -> list[dict]:
    """อ่านรายชื่อผู้อนุมัติจากค่าที่เก็บไว้ (JSON) ข้อมูลเสีย/รูปแบบผิดจะถูกข้าม ไม่ล้ม"""
    try:
        data = json.loads(raw or "[]")
    except ValueError:
        return []
    users = []
    for u in data if isinstance(data, list) else []:
        if isinstance(u, dict) and isinstance(u.get("id"), str) and u["id"].strip():
            users.append({"id": u["id"].strip(), "name": str(u.get("name") or "").strip(), "added": str(u.get("added") or "")})
    return users


def dump_allowed(users: list[dict]) -> str:
    return json.dumps(users, ensure_ascii=False)


def find_allowed(users: list[dict], user_id: str) -> dict | None:
    """หาผู้อนุมัติจาก LINE userId (userId ว่าง = ไม่มีสิทธิ์เสมอ)"""
    if not user_id:
        return None
    return next((u for u in users if u["id"] == user_id), None)


def user_label(user: dict) -> str:
    """ชื่อที่แสดง/บันทึกว่าใครกดอนุมัติ (ไม่มีชื่อใช้ท้าย userId แทน)"""
    return user.get("name") or f"…{user['id'][-6:]}"


def new_pair_code(now: float | None = None) -> tuple[str, str]:
    """สร้างรหัสเชื่อมผู้อนุมัติ -> (รหัส 6 หลัก, ค่าที่เก็บ "รหัส|หมดอายุ|ผิดกี่ครั้ง")"""
    code = f"{secrets.randbelow(10 ** 6):06d}"
    return code, f"{code}|{int((now if now is not None else time.time()) + PAIR_TTL)}|0"


def _pair_parts(raw: str) -> tuple[str, int, int] | None:
    parts = (raw or "").split("|")
    if len(parts) < 2 or not re.fullmatch(r"[0-9]{6}", parts[0]) or not re.fullmatch(r"[0-9]+", parts[1]):
        return None
    fails = int(parts[2]) if len(parts) > 2 and re.fullmatch(r"[0-9]+", parts[2]) else 0
    return parts[0], int(parts[1]), fails


def pair_state(raw: str, now: float | None = None) -> tuple[str, int] | None:
    """รหัสเชื่อมที่ยังใช้ได้ -> (รหัส, วินาทีที่เหลือ) · ไม่มี/หมดอายุ -> None"""
    p = _pair_parts(raw)
    now = now if now is not None else time.time()
    if not p or p[1] <= now:
        return None
    return p[0], int(p[1] - now)


def check_pair(raw: str, text: str, now: float | None = None) -> tuple[str, str]:
    """ตรวจข้อความที่ส่งเข้า LINE ว่าเป็นรหัสเชื่อมไหม -> (ผล, ค่าใหม่ที่ต้องเก็บ)
    ผล: "none" = ไม่ใช่รหัส/ไม่มีรหัสที่รออยู่ (ข้อความธรรมดา) · "ok" = ตรงและใช้ได้ (รหัสถูกใช้ทิ้ง) ·
    "wrong" = เป็นเลข 6 หลักแต่ไม่ตรง (นับครั้งที่ผิด ครบ PAIR_MAX_FAILS ยกเลิกรหัส)"""
    state = pair_state(raw, now)
    if not state:
        return "none", ""  # ไม่มีรหัสที่รออยู่ หรือหมดอายุแล้ว (ล้างค่าเก่าทิ้งด้วย)
    code, exp, fails = _pair_parts(raw)
    t = (text or "").strip()
    if not re.fullmatch(r"[0-9]{6}", t):
        return "none", raw
    if hmac.compare_digest(t, code):
        return "ok", ""
    fails += 1
    return "wrong", "" if fails >= PAIR_MAX_FAILS else f"{code}|{exp}|{fails}"


async def display_name(src: dict) -> str:
    """ดึงชื่อที่แสดงใน LINE ของผู้ส่ง event (ใช้ตั้งชื่อในรายชื่อผู้อนุมัติ) · ไม่ได้ = ว่าง"""
    uid, t = src.get("userId"), token()
    if not uid or not t:
        return ""
    if src.get("groupId"):
        path = f"/group/{src['groupId']}/member/{uid}"
    elif src.get("roomId"):
        path = f"/room/{src['roomId']}/member/{uid}"
    else:
        path = f"/profile/{uid}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{API}{path}", headers={"Authorization": f"Bearer {t}"})
        return (r.json().get("displayName") or "").strip()[:60] if r.status_code < 300 else ""
    except (httpx.HTTPError, ValueError):
        return ""


# ---------------------------------------------------------------- รูปภาพสำหรับการ์ด LINE
# LINE ดึงรูปจากลิงก์ HTTPS โดยไม่มีการล็อกอิน แต่รูปในระบบเป็นของลูกค้า (เช่น สลิป) จึงไม่เปิดสาธารณะตรงๆ
# ใช้ลิงก์ที่เซ็นลายเซ็น (HMAC) + วันหมดอายุ เดาไม่ได้ และเปิดได้เฉพาะไฟล์นั้นๆ ผ่าน /pub/media/<token>/<ชื่อไฟล์>
def _media_days() -> int:
    try:
        return max(1, int(os.getenv("LINE_MEDIA_DAYS") or 30))
    except ValueError:
        return 30


def _media_key() -> bytes:
    try:
        from .config import SECRET_KEY
    except ImportError:  # รันแยกไฟล์ (ทดสอบ)
        SECRET_KEY = os.getenv("SECRET_KEY", "dev")
    return ("line-media:" + SECRET_KEY).encode()


def _media_sig(name: str, exp: int) -> str:
    mac = hmac.new(_media_key(), f"{name}\n{exp}".encode(), hashlib.sha256).digest()[:18]
    return base64.urlsafe_b64encode(mac).decode().rstrip("=")


def media_url(name: str) -> str:
    """ลิงก์รูปที่ LINE เปิดได้ (หมดอายุตาม LINE_MEDIA_DAYS วัน) · ว่าง = ยังไม่มีที่อยู่เว็บสาธารณะ หรือไม่ใช่ไฟล์รูป"""
    base = public_url()
    if not base or not name or os.path.splitext(name)[1].lower() not in IMG_EXTS:
        return ""
    exp = int(time.time()) + _media_days() * 86400
    return f"{base}/pub/media/{exp}.{_media_sig(name, exp)}/{quote(name)}"


def check_media(token_: str, name: str) -> bool:
    """ตรวจลิงก์รูป: ลายเซ็นถูกต้อง ตรงกับชื่อไฟล์ และยังไม่หมดอายุ"""
    exp_s, _, sig = (token_ or "").partition(".")
    if not exp_s.isdigit() or int(exp_s) < time.time() or not sig:
        return False
    return hmac.compare_digest(sig, _media_sig(name, int(exp_s)))


def _image_url(name: str) -> str:
    """ชื่อไฟล์ในโฟลเดอร์รูป -> ลิงก์ให้ LINE (ว่างถ้าไม่มีไฟล์จริง)"""
    if not name:
        return ""
    try:
        from .config import MEDIA_DIR
        if not (MEDIA_DIR / name).is_file():
            return ""
    except ImportError:  # รันแยกไฟล์ (ทดสอบ)
        pass
    return media_url(name)


def prepare_image(path: Path) -> tuple[bytes, str] | None:
    """อ่านไฟล์รูปแล้วคืน (ข้อมูล, mime) ที่ LINE รับได้ = JPEG/PNG ด้านยาวไม่เกิน 1024px (แปลง WebP/GIF ให้ด้วย)
    ไม่มี Pillow หรือแปลงไม่ได้ -> ส่งไฟล์เดิมถ้าเป็น JPEG/PNG ไม่เกิน 10MB ไม่งั้นคืน None"""
    raw_mime = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}.get(path.suffix.lower())

    def original():
        if raw_mime and path.stat().st_size <= 10 * 1024 * 1024:
            return path.read_bytes(), raw_mime
        return None

    try:
        from PIL import Image, ImageOps
    except ImportError:
        return original()
    try:
        with Image.open(path) as im:
            im = ImageOps.exif_transpose(im)  # แก้รูปที่ถ่ายแนวตั้งแล้วหมุนผิด
            im.thumbnail((LINE_IMG_MAX_PX, LINE_IMG_MAX_PX))
            buf = io.BytesIO()
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                im.convert("RGBA").save(buf, "PNG", optimize=True)
                return buf.getvalue(), "image/png"
            im.convert("RGB").save(buf, "JPEG", quality=85, optimize=True)
            return buf.getvalue(), "image/jpeg"
    except Exception:  # noqa: BLE001 - ไฟล์เสีย/รูปแบบแปลก ลองส่งไฟล์เดิมแทน
        log.exception("prepare image failed: %s", path.name)
        return original()


async def _post(path: str, payload: dict) -> bool:
    t = token()
    if not t:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(f"{API}{path}", json=payload,
                                  headers={"Authorization": f"Bearer {t}"})
        if r.status_code >= 300:
            log.warning("LINE %s ผิดพลาด %s: %s", path, r.status_code, r.text[:300])
            return False
        return True
    except httpx.HTTPError as e:
        log.warning("LINE %s เชื่อมต่อไม่ได้: %s", path, e)
        return False


async def push(to: str, messages: list[dict]) -> bool:
    if not to:
        return False
    return await _post("/message/push", {"to": to, "messages": messages})


async def reply(reply_token: str, messages: list[dict]) -> bool:
    return await _post("/message/reply", {"replyToken": reply_token, "messages": messages})


def source_id(src: dict) -> str:
    """ดึง id ของผู้ส่ง event (user/group/room) ไว้ใช้เป็นปลายทางส่งข้อความ"""
    return src.get("groupId") or src.get("roomId") or src.get("userId") or ""


# ---------------------------------------------------------------- ชิ้นส่วนการ์ด
def _clip(text: str, n: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"


def _text(text: str, **kw) -> dict:
    return {"type": "text", "text": text or "-", "wrap": True, **kw}


def _section(title: str) -> dict:
    return {"type": "text", "text": title, "size": "xs", "weight": "bold", "color": MUTED, "margin": "md"}


def _fact(label: str, value: str, color: str = INK, bold: bool = False) -> dict:
    """แถว "ชื่อช่อง  ค่า" (ชื่อช่องสีเทาอยู่ซ้าย)"""
    val = _text(value, size="sm", color=color, flex=5)
    if bold:
        val["weight"] = "bold"
    return {"type": "box", "layout": "baseline", "spacing": "sm", "contents": [
        {"type": "text", "text": label, "size": "xs", "color": MUTED, "flex": 2}, val]}


def _box(contents: list[dict], bg: str = PAPER, pad: str = "10px") -> dict:
    return {"type": "box", "layout": "vertical", "backgroundColor": bg, "cornerRadius": "md",
            "paddingAll": pad, "spacing": "xs", "contents": contents}


def _header(small: str, title: str, color: str) -> dict:
    return {"type": "box", "layout": "vertical", "backgroundColor": color, "paddingAll": "16px", "spacing": "xs",
            "contents": [
                {"type": "text", "text": small, "size": "xs", "weight": "bold", "color": "#FFFFFFB3", "wrap": True},
                {"type": "text", "text": title, "size": "lg", "weight": "bold", "color": "#FFFFFF", "wrap": True},
            ]}


def _postback(label: str, data: str) -> dict:
    return {"type": "postback", "label": label, "data": data, "displayText": label}


def _footer(no: dict, yes: dict, color: str, link: tuple[str, str] | None = None) -> dict:
    rows = [{"type": "box", "layout": "horizontal", "spacing": "sm", "contents": [
        {"type": "button", "style": "secondary", "height": "sm", "flex": 1, "action": no},
        {"type": "button", "style": "primary", "color": color, "height": "sm", "flex": 2, "action": yes},
    ]}]
    if link:
        rows.append({"type": "button", "style": "link", "height": "sm", "color": color,
                     "action": {"type": "uri", "label": link[0], "uri": link[1]}})
    return {"type": "box", "layout": "vertical", "spacing": "sm", "paddingAll": "12px", "contents": rows}


def _img(url: str, ratio: str = "4:3", **kw) -> dict:
    """รูปในการ์ด (แตะเพื่อดูรูปเต็ม)"""
    return {"type": "image", "url": url, "size": "full", "aspectRatio": ratio, "aspectMode": "cover",
            "cornerRadius": "md", "action": {"type": "uri", "label": "ดูรูป", "uri": url}, **kw}


def _img_grid(urls: list[str], per_row: int = 3) -> list[dict]:
    """แถวรูปย่อ เรียงละ per_row รูป · เติม filler ให้ทุกรูปกว้างเท่ากัน"""
    rows = []
    for i in range(0, len(urls), per_row):
        chunk = urls[i:i + per_row]
        cells = [_img(u, "1:1", flex=1) for u in chunk] + [{"type": "filler", "flex": 1} for _ in range(per_row - len(chunk))]
        rows.append({"type": "box", "layout": "horizontal", "spacing": "xs", "contents": cells})
    return rows


def _site_row(site: str, website: str) -> list[dict]:
    """แถวเว็บไซต์ + ผลตรวจ (site = "ok|ข้อความ" / "bad|ข้อความ" / "warn|ข้อความ" / "")"""
    if not website:
        return [_fact("เว็บไซต์", "ยังไม่มีลิงก์", MUTED)]
    rows = [_fact("เว็บไซต์", website, INK)]
    if site:
        tone, _, label = site.partition("|")
        rows.append(_fact("สถานะเว็บ", label, {"ok": GREEN, "bad": RED, "warn": AMBER}.get(tone, MUTED), bold=True))
    return rows


# ---------------------------------------------------------------- การ์ดตอบลูกค้า
def approval_messages(reply_id: int, chat_label: str, draft: str, kind: str = "ai", ctx: dict | None = None) -> list[dict]:
    """การ์ดรออนุมัติตอบลูกค้า (Flex Message ใบเดียว) ข้อมูลครบ ตัดสินใจจาก LINE ได้เลย

    ctx (ไม่บังคับ): when, customer, note, photos, ticket{...}, messages[{who,when,text,image,out,target}]
    postback data เป็น "approve:<id>" / "reject:<id>" จึงใช้ตัวรับใน webhook เดิมได้
    """
    ctx = ctx or {}
    head, color = KINDS.get(kind, KINDS["ai"])
    label = (chat_label or "").strip() or "ไม่ทราบชื่อแชท"
    draft = (draft or "").strip()
    truncated = len(draft) > MAX_DRAFT
    shown = (draft[:MAX_DRAFT].rstrip() + "…") if truncated else (draft or "(ไม่มีข้อความ)")
    ticket = ctx.get("ticket") or {}

    body: list[dict] = [_fact("แชท", label, bold=True)]
    if ctx.get("customer"):
        body.append(_fact("ลูกค้า", ctx["customer"]))
    if ctx.get("when"):
        body.append(_fact("เวลา", ctx["when"], MUTED))

    if ticket:
        body.append({"type": "separator", "margin": "md", "color": LINE_GREY})
        body.append(_section(f"TICKET #{ticket.get('id', '')}"))
        info = [_text(ticket.get("title") or "-", size="sm", weight="bold", color=INK)]
        meta = " · ".join(x for x in (ticket.get("category"), ticket.get("status")) if x)
        if meta:
            info.append(_text(meta, size="xs", color=MUTED))
        if ticket.get("severity"):
            info.append(_fact("ความรุนแรง", ticket["severity"], SEV_COLOR.get(ticket.get("severity_key"), INK), bold=True))
        if ticket.get("summary"):
            info.append(_text(_clip(ticket["summary"], MAX_SUMMARY), size="xs", color=INK))
        info += _site_row(ticket.get("site", ""), ticket.get("website", ""))
        body.append(_box(info))

    msgs = [m for m in (ctx.get("messages") or []) if m.get("text") or m.get("image") or m.get("image_url")][-6:]
    if msgs:
        body.append({"type": "separator", "margin": "md", "color": LINE_GREY})
        body.append(_section("บทสนทนาล่าสุด"))
        # แสดงรูปจริงเฉพาะ MAX_CONV_IMAGES รูปล่าสุด ที่เหลือแสดงเป็น "[รูปภาพ]" (การ์ดจะได้ไม่ยาวเกินไป)
        show_img, budget = set(), MAX_CONV_IMAGES
        for i in range(len(msgs) - 1, -1, -1):
            if msgs[i].get("image_url") and budget > 0:
                show_img.add(i)
                budget -= 1
        for i, m in enumerate(msgs):
            who = f"{m.get('who') or 'ลูกค้า'} · {m.get('when', '')}".strip(" ·")
            if m.get("target"):
                who += " · ข้อความที่ตอบ"
            text = _clip(m.get("text") or "", MAX_MSG)
            has_img = bool(m.get("image") or m.get("image_url"))
            if has_img and i not in show_img:
                text = (text + "  " if text else "") + "[รูปภาพ]"
            parts = [_text(who, size="xxs", color=GREEN if m.get("out") else MUTED, weight="bold" if m.get("target") else "regular")]
            if text:
                parts.append(_text(text, size="sm", color=INK))
            if i in show_img:
                parts.append(_img(m["image_url"], "4:3", margin="sm"))
            body.append(_box(parts, bg=GREEN_TINT if m.get("out") else PAPER, pad="8px"))

    if ctx.get("note"):
        body.append(_box([_text("หมายเหตุจากระบบ", size="xxs", weight="bold", color=AMBER),
                          _text(_clip(ctx["note"], MAX_NOTE), size="xs", color=INK)], bg=AMBER_TINT, pad="10px"))

    body.append({"type": "separator", "margin": "md", "color": LINE_GREY})
    body.append(_section("ข้อความที่จะส่งถึงลูกค้า"))
    body.append(_box([_text(shown, size="sm", color=INK, lineSpacing="4px")], pad="12px"))
    media = [u for u in (ctx.get("media_urls") or []) if u][:MAX_REPLY_IMAGES]
    if media:
        body.append(_text(f"รูปที่จะส่งพร้อมข้อความ ({ctx.get('photos') or len(media)} รูป)", size="xs", color=MUTED))
        body += _img_grid(media)
    elif ctx.get("photos"):
        body.append(_text(f"แนบรูปประกอบ {ctx['photos']} รูป (ส่งพร้อมข้อความ)", size="xs", color=MUTED))
    if truncated:
        body.append(_text("ข้อความยาว ดูฉบับเต็มในข้อความด้านบน", size="xxs", color=MUTED))
    body.append(_text(f"รหัสรายการ #{reply_id}", size="xxs", color="#A8A397"))

    url = public_url()
    small = "รออนุมัติ" + (f" · ticket #{ticket['id']}" if ticket.get("id") is not None else "")
    bubble = {
        "type": "bubble", "size": "giga",
        "header": _header(small, head, color),
        "body": {"type": "box", "layout": "vertical", "paddingAll": "16px", "spacing": "sm", "contents": body},
        "footer": _footer(_postback("ไม่ส่ง", f"reject:{reply_id}"), _postback("อนุมัติส่ง", f"approve:{reply_id}"), color,
                          ("เปิดดูในเว็บ", f"{url}/replies") if url else None),
        "styles": {"footer": {"separator": True, "separatorColor": LINE_GREY}},
    }
    preview = " ".join(draft.split())[:70]
    alt = f"รออนุมัติ · {head} · {_clip(label, 80)}" + (f" — {preview}" if preview else "")
    flex = {"type": "flex", "altText": alt[:400], "contents": bubble}
    # ข้อความยาว: ส่งฉบับเต็มเป็นข้อความธรรมดาก่อน (ก๊อปได้) แล้วค่อยตามด้วยการ์ดที่มีปุ่ม ปุ่มจึงอยู่ล่างสุดเสมอ
    return ([{"type": "text", "text": draft[:4900]}] if truncated else []) + [flex]


# ---------------------------------------------------------------- การ์ด ticket รอส่งเข้ากลุ่มโปรแกรมเมอร์
def dev_ticket_messages(ticket_id: int, info: dict) -> list[dict]:
    """การ์ดรออนุมัติส่ง ticket เข้ากลุ่มโปรแกรมเมอร์ · info: title, category, severity, severity_key, customer, chat,
    website, site, summary, photos, created · postback: "postdev:<id>" / "skipdev:<id>" """
    title = (info.get("title") or "").strip() or "-"
    color = DEV_COLOR
    body: list[dict] = [_text(title, size="md", weight="bold", color=INK)]
    facts = [
        _fact("หมวด", info.get("category") or "-"),
        _fact("ความรุนแรง", info.get("severity") or "-", SEV_COLOR.get(info.get("severity_key"), INK), bold=True),
        _fact("ลูกค้า", info.get("customer") or "-"),
        _fact("แชท", info.get("chat") or "-"),
    ]
    if info.get("created"):
        facts.append(_fact("เวลา", info["created"], MUTED))
    facts += _site_row(info.get("site", ""), info.get("website", ""))
    photo_urls = [u for u in (info.get("photo_urls") or []) if u][:MAX_TICKET_IMAGES]
    if info.get("photos") and not photo_urls:
        facts.append(_fact("รูปแนบ", f"{info['photos']} รูป (โพสต์พร้อม ticket)"))
    body += facts
    body.append({"type": "separator", "margin": "md", "color": LINE_GREY})
    body.append(_section("สรุปปัญหา"))
    body.append(_box([_text(_clip(info.get("summary") or "-", MAX_DRAFT), size="sm", color=INK, lineSpacing="4px")], pad="12px"))
    if photo_urls:
        body.append(_text(f"รูปจากลูกค้า ({info.get('photos') or len(photo_urls)} รูป · โพสต์พร้อม ticket)", size="xs", color=MUTED, margin="md"))
        body += _img_grid(photo_urls)
    body.append(_text("หลังส่งแล้ว โปรแกรมเมอร์ reply โพสต์นี้เพื่ออัปเดตสถานะได้", size="xxs", color=MUTED))
    body.append(_text(f"Ticket #{ticket_id}", size="xxs", color="#A8A397"))

    url = public_url()
    bubble = {
        "type": "bubble", "size": "giga",
        "header": _header(f"รออนุมัติ · ticket #{ticket_id}", "ส่งเข้ากลุ่มโปรแกรมเมอร์", color),
        "body": {"type": "box", "layout": "vertical", "paddingAll": "16px", "spacing": "sm", "contents": body},
        "footer": _footer(_postback("ไม่ส่ง", f"skipdev:{ticket_id}"), _postback("ส่งเข้ากลุ่ม", f"postdev:{ticket_id}"), color,
                          ("เปิดดูในเว็บ", f"{url}/replies?box=dev") if url else None),
        "styles": {"footer": {"separator": True, "separatorColor": LINE_GREY}},
    }
    alt = f"รออนุมัติส่งเข้ากลุ่มโปรแกรมเมอร์ · ticket #{ticket_id} · {_clip(title, 80)}"
    return [{"type": "flex", "altText": alt[:400], "contents": bubble}]


# ---------------------------------------------------------------- การ์ดแจ้งผลสั้นๆ & ข้อความตัวอย่าง
def notice_message(text: str, tone: str = "ok") -> list[dict]:
    """การ์ดแจ้งผลสั้นๆ (วงกลมสัญลักษณ์ + ข้อความ) tone: ok / bad / warn / info"""
    color, tint, mark = TONES.get(tone, TONES["info"])
    bubble = {
        "type": "bubble", "size": "kilo",
        "body": {"type": "box", "layout": "horizontal", "spacing": "md", "paddingAll": "14px", "alignItems": "center",
                 "contents": [
                     {"type": "box", "layout": "vertical", "width": "30px", "height": "30px", "cornerRadius": "15px",
                      "backgroundColor": tint, "justifyContent": "center", "alignItems": "center", "flex": 0,
                      "contents": [{"type": "text", "text": mark, "size": "sm", "weight": "bold", "color": color, "align": "center"}]},
                     {"type": "text", "text": text, "size": "sm", "color": INK, "wrap": True, "flex": 1, "gravity": "center"},
                 ]},
    }
    return [{"type": "flex", "altText": text[:400], "contents": bubble}]


def sample_image_name() -> str:
    """สร้างรูปตัวอย่างในโฟลเดอร์รูป (ต้องมี Pillow) ไว้ทดสอบว่า LINE ดึงรูปจากเว็บเราได้จริง · ว่าง = สร้างไม่ได้"""
    try:
        from PIL import Image, ImageDraw
        from .config import MEDIA_DIR
    except ImportError:
        return ""
    name = "line_sample.png"
    path = MEDIA_DIR / name
    try:
        if not path.exists():
            im = Image.new("RGB", (1280, 960), "#F5F3EE")  # ใหญ่กว่า 1024px เพื่อทดสอบการย่อรูปด้วย
            d = ImageDraw.Draw(im)
            d.rectangle((60, 60, 1220, 900), outline="#0B7D52", width=10)
            d.rectangle((60, 60, 1220, 220), fill="#0B7D52")
            d.text((100, 120), "LINE IMAGE TEST", fill="#FFFFFF")
            d.text((100, 300), "If you can see this card image, LINE can fetch pictures from this server.", fill="#1C1B19")
            im.save(path)
        return name
    except OSError:
        return ""


def sample_messages() -> list[dict]:
    """การ์ดตัวอย่างสำหรับปุ่ม "ส่งข้อความทดสอบ" (id 0 ไม่มีอยู่จริง กดปุ่มแล้วจะแจ้งว่าดำเนินการไปแล้ว ไม่กระทบข้อมูลจริง)
    ถ้ามีที่อยู่เว็บสาธารณะและ Pillow จะใส่รูปตัวอย่างด้วย เพื่อทดสอบระบบรูปครบวงจร"""
    img = media_url(sample_image_name())
    ctx = {
        "when": "05/10/2026 20:58", "customer": "คุณสมชาย (ตัวอย่าง)", "photos": 1, "media_urls": [img] if img else [],
        "note": "ลูกค้าแจ้งฝากเงินไม่เข้า · ระบบขอสลิปและเวลาที่ทำรายการเพิ่ม",
        "ticket": {"id": 0, "title": "ฝากเงินแล้วยอดไม่เข้า", "category": "ฝากเงินไม่ออโต้", "status": "เปิดใหม่",
                   "severity": "สูง", "severity_key": "high", "website": "https://example-casino.com",
                   "site": "ok|เข้าได้ ตอบสนองปกติ",
                   "summary": "ลูกค้าโอนเงินฝากผ่านธนาคารเมื่อ 20:40 น. แต่ยอดยังไม่เข้าเครดิต ส่งสลิปมาให้แล้ว 1 รูป"},
        "messages": [
            {"who": "คุณสมชาย", "when": "05/10 20:52", "text": "ฝากเงินไป 500 ยอดยังไม่เข้าเลยครับ", "image": True, "image_url": img},
            {"who": "ทีมงาน", "when": "05/10 20:54", "text": "รบกวนแจ้งเวลาที่โอนและส่งสลิปด้วยนะคะ", "out": True},
            {"who": "คุณสมชาย", "when": "05/10 20:58", "text": "โอนตอน 20:40 ครับ ส่งสลิปให้แล้ว", "target": True},
        ],
    }
    draft = ("สวัสดีค่ะ ทางเราได้รับสลิปแล้ว กำลังตรวจสอบรายการให้นะคะ "
             "หากตรวจพบแล้วจะแจ้งกลับโดยเร็วที่สุดค่ะ")
    dev = {"title": "ฝากเงินแล้วยอดไม่เข้า", "category": "ฝากเงินไม่ออโต้", "severity": "สูง", "severity_key": "high",
           "customer": "คุณสมชาย (ตัวอย่าง)", "chat": "ตัวอย่าง · กลุ่มลูกค้า (Support)", "created": "05/10/2026 20:58",
           "website": "https://example-casino.com", "site": "ok|เข้าได้", "photos": 1, "photo_urls": [img] if img else [],
           "summary": "ลูกค้าโอนเงินฝากผ่านธนาคารเมื่อ 20:40 น. แต่ยอดไม่เข้าเครดิต ส่งสลิปมาให้แล้ว ขอให้ตรวจสอบรายการฝากย้อนหลัง"}
    return approval_messages(0, "ตัวอย่าง · กลุ่มลูกค้า (Support)", draft, "ai", ctx) + dev_ticket_messages(0, dev)


def text_message(text: str) -> list[dict]:
    return [{"type": "text", "text": text[:4900]}]


# ---------------------------------------------------------------- ดึงข้อมูลจากฐานข้อมูลมาทำการ์ด
def _fmt_time(dt, short: bool = False) -> str:
    from datetime import timezone
    from .config import DISPLAY_TZ
    if not dt:
        return ""
    local = dt.replace(tzinfo=timezone.utc).astimezone(DISPLAY_TZ)
    return local.strftime("%d/%m %H:%M" if short else "%d/%m/%Y %H:%M")


def _site_label(site_check: str) -> str:
    """ผลตรวจเว็บไซต์ล่าสุดของ ticket -> "ok|ข้อความ" / "bad|ข้อความ" / "warn|ข้อความ" (ว่าง = ยังไม่ได้ตรวจ)"""
    try:
        sc = json.loads(site_check) if site_check else {}
    except ValueError:
        return ""
    if not sc:
        return ""
    if not sc.get("ok"):
        return f"bad|เข้าไม่ได้ ({sc.get('error') or sc.get('status_code') or 'ไม่ทราบสาเหตุ'})"
    if sc.get("other_host"):
        return f"warn|ถูกพาไป {sc.get('final_host')} (โดเมนอาจหมดอายุ)"
    return "ok|เข้าได้"


def _ticket_info(t, chat_title: str = "") -> dict:
    from . import ai_service, dev_bridge
    return {
        "id": t.id, "title": t.title, "summary": t.summary,
        "category": ai_service.CATEGORIES.get(t.category, t.category),
        "severity": dev_bridge.SEVERITY_TH.get(t.severity, t.severity), "severity_key": t.severity,
        "status": dev_bridge.STATUS_TH.get(t.status, t.status),
        "customer": t.customer_name, "chat": chat_title, "website": t.website_url,
        "site": _site_label(t.site_check), "created": _fmt_time(t.created_at),
        "photos": len(t.attachments or []),
        "photo_urls": [u for u in (_image_url(a.media_path) for a in (t.attachments or [])[:MAX_TICKET_IMAGES]) if u],
    }


def _reply_context(db, reply, chat) -> dict:
    from sqlalchemy import select
    from .database import Message, Ticket
    try:
        media_names = [str(n) for n in json.loads(reply.media or "[]")]
    except ValueError:
        media_names = []
    photos = len(media_names)
    rows = list(db.scalars(select(Message).where(Message.chat_id == reply.chat_id)
                           .order_by(Message.date.desc(), Message.tg_message_id.desc()).limit(6)))[::-1]
    ctx = {
        "when": _fmt_time(reply.created_at), "note": (reply.note or "").strip(), "photos": photos,
        "media_urls": [u for u in (_image_url(n) for n in media_names[:MAX_REPLY_IMAGES]) if u],
        "customer": next((m.sender_name for m in reversed(rows) if not m.is_outgoing and m.sender_name), ""),
        "messages": [{"who": "ทีมงาน" if m.is_outgoing else (m.sender_name or "ลูกค้า"), "when": _fmt_time(m.date, short=True),
                      "text": (m.text or "").strip(), "image": bool(m.media_path), "image_url": _image_url(m.media_path),
                      "out": bool(m.is_outgoing), "target": m.tg_message_id == reply.reply_to_tg_id} for m in rows],
    }
    ticket = db.get(Ticket, reply.ticket_id) if reply.ticket_id else None
    if ticket:
        ctx["ticket"] = _ticket_info(ticket, chat.title if chat else "")
    return ctx


async def notify_reply(reply_id: int) -> bool:
    """ส่งการ์ดรออนุมัติของร่างคำตอบนี้เข้า LINE (ถ้าเปิดใช้และตั้งปลายทางไว้)"""
    from .database import Chat, Reply, SessionLocal, get_settings
    if not token():
        return False
    with SessionLocal() as db:
        settings = get_settings(db)
        if settings.get("line_enabled") != "1":
            return False
        target = settings.get("line_target", "")
        reply = db.get(Reply, reply_id)
        if not target or not reply or reply.status != "pending":
            return False
        chat = db.get(Chat, reply.chat_id)
        label = chat.title if chat else str(reply.chat_id)
        kind, draft = reply.kind, reply.final_text
        try:
            ctx = _reply_context(db, reply, chat)
        except Exception:  # noqa: BLE001 - ดึงรายละเอียดไม่ได้ ก็ยังส่งการ์ดแบบพื้นฐานให้
            log.exception("build LINE context failed for reply %s", reply_id)
            ctx = None
    return await push(target, approval_messages(reply_id, label, draft, kind, ctx))


async def notify_dev_ticket(ticket_id: int) -> bool:
    """ส่งการ์ด "ticket รอส่งเข้ากลุ่มโปรแกรมเมอร์" เข้า LINE (ถ้าเปิดใช้และตั้งปลายทางไว้)"""
    from .database import Chat, SessionLocal, Ticket, get_settings
    if not token():
        return False
    with SessionLocal() as db:
        settings = get_settings(db)
        if settings.get("line_enabled") != "1":
            return False
        target = settings.get("line_target", "")
        ticket = db.get(Ticket, ticket_id)
        if not target or not ticket or ticket.dev_status != "pending":
            return False
        chat = db.get(Chat, ticket.chat_id)
        info = _ticket_info(ticket, chat.title if chat else str(ticket.chat_id))
    return await push(target, dev_ticket_messages(ticket_id, info))
