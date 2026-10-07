"""เชื่อมกับ LINE Messaging API: ส่งการ์ดรออนุมัติเข้า LINE และรับปุ่มกดอนุมัติ/ไม่ส่งกลับมา

ต้องตั้ง env: LINE_CHANNEL_ACCESS_TOKEN (ส่งข้อความ), LINE_CHANNEL_SECRET (ตรวจลายเซ็น webhook)
ปลายทางที่จะส่ง (line_target) จับอัตโนมัติเมื่อมีคนทักหรือเพิ่ม OA เป็นเพื่อน แล้วเก็บไว้ในตั้งค่า
"""

import base64
import hashlib
import hmac
import logging
import os

import httpx

log = logging.getLogger("app.line")
API = "https://api.line.me/v2/bot"
HEAD = {"ai": "ร่างตอบลูกค้า", "resolved": "แจ้งลูกค้าว่าแก้ไขเสร็จ", "dev_update": "อัปเดตจากโปรแกรมเมอร์"}


def token() -> str:
    return (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()


def secret() -> str:
    return (os.getenv("LINE_CHANNEL_SECRET") or "").strip()


def configured() -> bool:
    return bool(token() and secret())


def verify(body: bytes, signature: str) -> bool:
    """ตรวจ X-Line-Signature ว่ามาจาก LINE จริง (กันคนอื่นยิง webhook มาสั่งอนุมัติ)"""
    s = secret()
    if not s or not signature:
        return False
    mac = hmac.new(s.encode("utf-8"), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(mac).decode(), signature)


async def _post(path: str, payload: dict) -> bool:
    t = token()
    if not t:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(f"{API}{path}", json=payload,
                                  headers={"Authorization": f"Bearer {t}"})
        if r.status_code >= 300:
            log.warning("LINE %s ผิดพลาด %s: %s", path, r.status_code, r.text[:200])
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


def approval_messages(reply_id: int, chat_label: str, draft: str, kind: str = "ai") -> list[dict]:
    head = HEAD.get(kind, "ร่างตอบลูกค้า")
    body = f"[{head}] {chat_label}\n\n{(draft or '').strip()}"
    btn = (f"{head}ให้ “{chat_label}” ไหม?"[:160]) or "อนุมัติส่งข้อความ?"
    return [
        {"type": "text", "text": body[:4900]},
        {"type": "template", "altText": f"{head} รออนุมัติ: {chat_label}",
         "template": {"type": "buttons", "text": btn, "actions": [
             {"type": "postback", "label": "✅ อนุมัติส่ง", "data": f"approve:{reply_id}", "displayText": "อนุมัติส่ง"},
             {"type": "postback", "label": "❌ ไม่ส่ง", "data": f"reject:{reply_id}", "displayText": "ไม่ส่ง"},
         ]}},
    ]


def text_message(text: str) -> list[dict]:
    return [{"type": "text", "text": text[:4900]}]


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
    return await push(target, approval_messages(reply_id, label, draft, kind))
