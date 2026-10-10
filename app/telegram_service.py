"""เชื่อมต่อบัญชี Telegram ของผู้ใช้ (userbot) ด้วย Telethon"""

import logging
from datetime import timedelta

from sqlalchemy import select
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from . import chatbus
from .config import MEDIA_DIR
from .database import Chat, Message, SessionLocal, TelegramAccount, utcnow
from .security import decrypt, encrypt

log = logging.getLogger(__name__)

STICKER_TEXT = "(สติกเกอร์)"
IMAGE_MIME_EXT = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif"}


class TelegramLoginError(Exception):
    pass


class TelegramService:
    def __init__(self) -> None:
        self.client: TelegramClient | None = None
        self._pending_login: dict | None = None  # เก็บ client ระหว่างขั้นตอนขอรหัส -> ยืนยันรหัส
        self._monitored: set[int] = set()
        self.on_message = None  # callback(chat_id) ถูกตั้งจาก analyzer
        # กลุ่มโปรแกรมเมอร์: chat id, username ที่ต้องติดตาม (ตัวพิมพ์เล็ก ไม่มี @), callback(dict)
        self.dev_group_id: int | None = None
        self.dev_users: set[str] = set()
        # ทีมงานคนอื่นในกลุ่มลูกค้า (นอกจากบัญชีที่เชื่อมต่อ) ข้อความของคนเหล่านี้ไม่ต้องวิเคราะห์
        self.staff_users: set[str] = set()
        # บัญชีที่ไม่รับข้อความเลย (ไม่บันทึก ไม่วิเคราะห์) เช่น บอทแจ้งเตือน
        self.ignore_users: set[str] = set()
        self.ignore_bots = True
        self.on_dev_message = None
        self.on_connected = None  # async callback หลังเชื่อมต่อสำเร็จ
        # พนักงานที่กดส่ง (ชื่อผู้ใช้เว็บ/LINE) ของข้อความที่เพิ่งส่ง รอให้ข้อความถูกบันทึกจากอีเวนต์ Telegram แล้วค่อยใส่ชื่อ
        self._sent_by_hints: dict[tuple[int, int], str] = {}

    # ------------------------------------------------------------------ state
    @property
    def connected(self) -> bool:
        return self.client is not None and self.client.is_connected()

    def set_dev(self, group_id: str | int | None, usernames: str) -> None:
        try:
            self.dev_group_id = int(group_id) if group_id else None
        except (TypeError, ValueError):
            self.dev_group_id = None
        self.dev_users = {u.strip().lstrip("@").lower() for u in (usernames or "").replace("\n", ",").split(",") if u.strip()}

    def set_staff(self, usernames: str) -> None:
        self.staff_users = {u.strip().lstrip("@").lower() for u in (usernames or "").replace("\n", ",").split(",") if u.strip()}

    def set_ignore(self, usernames: str, ignore_bots: bool) -> None:
        self.ignore_users = {u.strip().lstrip("@").lower() for u in (usernames or "").replace("\n", ",").split(",") if u.strip()}
        self.ignore_bots = ignore_bots

    def is_ignored(self, msg, sender) -> bool:
        if msg is not None and msg.out:
            return False
        username = (getattr(sender, "username", None) or "").lower()
        return (bool(username) and username in self.ignore_users) or (self.ignore_bots and bool(getattr(sender, "bot", False)))

    def purge_ignored(self) -> int:
        """ลบข้อความเก่าจากบัญชีที่ไม่รับข้อความ (ชื่อผู้ส่งเก็บเป็น "ชื่อ (@username)")"""
        if not self.ignore_users:
            return 0
        deleted = 0
        with SessionLocal() as db:
            for username in self.ignore_users:
                deleted += db.query(Message).filter(Message.sender_name.ilike(f"%(@{username})")).delete(
                    synchronize_session=False)
            db.commit()
        return deleted

    def mark_staff_history(self) -> int:
        """ข้อความเก่าของทีมงานที่เพิ่งเพิ่มในรายชื่อ (ชื่อผู้ส่งเก็บเป็น "ชื่อ (@username)") -> นับเป็นข้อความทีมงาน
        ขึ้นฝั่งทีมงานในบทสนทนา และไม่ต้องให้ AI วิเคราะห์ · คืนค่าจำนวนข้อความที่แก้"""
        if not self.staff_users:
            return 0
        changed = 0
        with SessionLocal() as db:
            for username in self.staff_users:
                pattern = "%(@" + username.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + ")"
                changed += db.query(Message).filter(
                    Message.sender_name.ilike(pattern, escape="\\"),
                    (Message.is_outgoing.is_(False)) | (Message.analyzed.is_(False)),
                ).update({Message.is_outgoing: True, Message.analyzed: True}, synchronize_session=False)
            db.commit()
        return changed

    def is_staff(self, msg, sender) -> bool:
        username = (getattr(sender, "username", None) or "").lower()
        return bool(msg.out) or (bool(username) and username in self.staff_users)

    def reload_monitored(self) -> None:
        with SessionLocal() as db:
            self._monitored = set(db.scalars(select(Chat.id).where(Chat.monitored)))

    def _set_status(self, status: str, error: str = "", **fields) -> None:
        with SessionLocal() as db:
            acc = db.get(TelegramAccount, 1)
            acc.status = status
            acc.last_error = error
            for k, v in fields.items():
                setattr(acc, k, v)
            db.commit()

    # ------------------------------------------------------------------ startup
    async def start_from_db(self) -> None:
        """เชื่อมต่ออัตโนมัติเมื่อเปิดโปรแกรม ถ้าเคยล็อกอินไว้แล้ว"""
        with SessionLocal() as db:
            acc = db.get(TelegramAccount, 1)
            api_id, api_hash, session = acc.api_id, decrypt(acc.api_hash_enc), decrypt(acc.session_enc)
        if not (api_id and api_hash and session):
            return
        client = TelegramClient(StringSession(session), api_id, api_hash)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                self._set_status("disconnected", "เซสชันหมดอายุ กรุณาเข้าสู่ระบบ Telegram ใหม่", session_enc="")
                return
            await self._activate(client)
        except (OSError, RPCError) as e:
            log.exception("connect telegram failed")
            self._set_status("error", f"เชื่อมต่อ Telegram ไม่ได้: {e}")

    async def _activate(self, client: TelegramClient) -> None:
        self.client = client
        me = await client.get_me()
        name = " ".join(filter(None, [me.first_name, me.last_name])) or me.username or str(me.id)
        self._set_status("connected", me_id=me.id, me_name=name,
                         session_enc=encrypt(client.session.save()))
        chatbus.set_me(name)
        self.reload_monitored()
        client.add_event_handler(self._handle_new_message, events.NewMessage())
        # โหลดรายชื่อแชทไว้ในแคช เพื่อให้ส่งข้อความหา chat id ได้หลังรีสตาร์ท
        await client.get_dialogs()
        log.info("Telegram connected as %s", name)
        # ข้อความที่เข้ามาระหว่างระบบปิด (เช่นตอน deploy) -> ดึงมาวิเคราะห์ต่อ
        for chat_id in list(self._monitored - {self.dev_group_id}):
            try:
                if await self.backfill(chat_id) and self.on_message:
                    self.on_message(chat_id)
            except (RPCError, ValueError, OSError):
                log.exception("backfill chat %s failed", chat_id)
        if self.on_connected:
            await self.on_connected()

    async def stop(self) -> None:
        if self.client:
            await self.client.disconnect()
        if self._pending_login:
            await self._pending_login["client"].disconnect()

    # ------------------------------------------------------------------ login
    async def send_code(self, api_id: int, api_hash: str, phone: str) -> None:
        if self._pending_login:
            await self._pending_login["client"].disconnect()
            self._pending_login = None
        client = TelegramClient(StringSession(), api_id, api_hash)
        await client.connect()
        try:
            sent = await client.send_code_request(phone)
        except PhoneNumberInvalidError as e:
            await client.disconnect()
            raise TelegramLoginError("เบอร์โทรศัพท์ไม่ถูกต้อง (ใส่รูปแบบ +66xxxxxxxxx)") from e
        except FloodWaitError as e:
            await client.disconnect()
            raise TelegramLoginError(f"ขอรหัสบ่อยเกินไป กรุณารอ {e.seconds} วินาที") from e
        except RPCError as e:
            await client.disconnect()
            raise TelegramLoginError(f"ขอรหัสไม่สำเร็จ: {e}") from e
        self._pending_login = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
        self._set_status("code_sent", api_id=api_id, api_hash_enc=encrypt(api_hash), phone=phone)

    async def verify_code(self, code: str) -> str:
        """คืนค่า 'connected' หรือ 'password_needed' (บัญชีเปิดยืนยันสองขั้นตอน)"""
        pending = self._require_pending()
        try:
            await pending["client"].sign_in(pending["phone"], code.strip(), phone_code_hash=pending["hash"])
        except SessionPasswordNeededError:
            self._set_status("password_needed")
            return "password_needed"
        except PhoneCodeInvalidError as e:
            raise TelegramLoginError("รหัสยืนยันไม่ถูกต้อง") from e
        except PhoneCodeExpiredError as e:
            raise TelegramLoginError("รหัสยืนยันหมดอายุ กรุณาขอรหัสใหม่") from e
        await self._finish_login()
        return "connected"

    async def verify_password(self, password: str) -> None:
        pending = self._require_pending()
        try:
            await pending["client"].sign_in(password=password)
        except PasswordHashInvalidError as e:
            raise TelegramLoginError("รหัสผ่าน Telegram (2FA) ไม่ถูกต้อง") from e
        await self._finish_login()

    def _require_pending(self) -> dict:
        if not self._pending_login:
            raise TelegramLoginError("ไม่พบขั้นตอนการเข้าสู่ระบบ กรุณาขอรหัสใหม่")
        return self._pending_login

    async def _finish_login(self) -> None:
        client = self._pending_login["client"]
        self._pending_login = None
        if self.client:
            await self.client.disconnect()
        await self._activate(client)

    async def logout(self) -> None:
        if self.client:
            try:
                await self.client.log_out()
            except RPCError:
                await self.client.disconnect()
            self.client = None
        self._set_status("disconnected", session_enc="", me_id=None, me_name="")

    # ------------------------------------------------------------------ chats
    async def list_dialogs(self) -> list[dict]:
        if not self.connected:
            return []
        dialogs = []
        async for d in self.client.iter_dialogs(limit=300):
            kind = "group" if d.is_group else "channel" if d.is_channel else "user"
            dialogs.append({"id": d.id, "title": d.name or str(d.id), "kind": kind})
        return dialogs

    async def _entity(self, chat_id: int):
        if not self.connected:
            raise TelegramLoginError("ยังไม่ได้เชื่อมต่อ Telegram")
        try:
            return await self.client.get_input_entity(chat_id)
        except ValueError:
            await self.client.get_dialogs()
            return await self.client.get_input_entity(chat_id)

    async def send_text(self, chat_id: int, text: str, reply_to: int | None = None) -> int:
        """ส่งข้อความ (ใช้กับกลุ่มภายใน) คืนค่า message id"""
        entity = await self._entity(chat_id)
        msg = await self.client.send_message(entity, text, reply_to=reply_to or None, link_preview=False)
        return msg.id

    async def send_files(self, chat_id: int, paths: list[str], reply_to: int | None = None) -> list[int]:
        """ส่งรูปเป็นอัลบั้ม คืนค่า message id ของทุกรูป"""
        if not paths:
            return []
        entity = await self._entity(chat_id)
        sent = await self.client.send_file(entity, paths if len(paths) > 1 else paths[0], reply_to=reply_to or None)
        sent = sent if isinstance(sent, list) else [sent]
        return [m.id for m in sent]

    async def send_reply(self, chat_id: int, text: str, reply_to: int | None) -> int | None:
        entity = await self._entity(chat_id)
        try:
            msg = await self.client.send_message(entity, text, reply_to=reply_to or None)
        except RPCError as e:
            # ข้อความลูกค้าที่จะตอบกลับถูกลบไปแล้ว -> ส่งเป็นข้อความปกติแทน
            if not reply_to or "REPLY" not in str(e).upper():
                raise
            msg = await self.client.send_message(entity, text)
        return getattr(msg, "id", None)

    async def backfill(self, chat_id: int, limit: int = 20, unanswered_hours: int = 24) -> int:
        """ดึงข้อความล่าสุดของแชทที่ยังไม่มีในระบบ
        ข้อความลูกค้าที่ทีมงานยังไม่ได้ตอบ (หลังข้อความล่าสุดของทีมงาน และไม่เกิน 24 ชม.) จะถูกส่งให้ AI วิเคราะห์
        คืนค่าจำนวนข้อความที่รอวิเคราะห์"""
        if not self.connected:
            return 0
        msgs = [m async for m in self.client.iter_messages(chat_id, limit=limit) if not getattr(m, "action", None)]
        msgs.reverse()  # เก่า -> ใหม่
        with SessionLocal() as db:
            existing = set(db.scalars(select(Message.tg_message_id).where(Message.chat_id == chat_id)))
        senders = []
        for m in msgs:
            try:
                senders.append(await m.get_sender())
            except RPCError:
                senders.append(None)
        kept = [(m, s) for m, s in zip(msgs, senders) if not self.is_ignored(m, s)]
        msgs, senders = [m for m, _ in kept], [s for _, s in kept]
        staff = [self.is_staff(m, s) for m, s in zip(msgs, senders)]
        last_staff = max((i for i, is_staff in enumerate(staff) if is_staff), default=-1)
        cutoff = utcnow() - timedelta(hours=unanswered_hours)
        waiting = 0
        for i, (m, sender) in enumerate(zip(msgs, senders)):
            if m.id in existing:
                continue
            date = m.date.replace(tzinfo=None) if m.date else utcnow()
            needs_analysis = not staff[i] and i > last_staff and date >= cutoff
            media_path = await self._download_image(chat_id, m) if needs_analysis else ""
            self._store(chat_id, m, sender, media_path, analyzed=not needs_analysis, staff=staff[i])
            waiting += needs_analysis
        return waiting

    # ------------------------------------------------------------------ events
    @staticmethod
    def _sender_name(sender) -> str:
        if sender is None:
            return ""
        name = (
            " ".join(filter(None, [getattr(sender, "first_name", None), getattr(sender, "last_name", None)]))
            or getattr(sender, "title", None) or getattr(sender, "username", None) or ""
        )
        if getattr(sender, "username", None):
            name += f" (@{sender.username})"
        return name

    def _store(self, chat_id: int, msg, sender, media_path: str, analyzed: bool, staff: bool = False) -> dict | None:
        """บันทึกข้อความลงฐานข้อมูลแล้วกระจายให้หน้าเว็บที่เปิดอยู่ · คืนค่า dict ของข้อความ (None = มีอยู่แล้ว ไม่บันทึกซ้ำ)"""
        text = msg.message or ""
        if not text and getattr(msg, "sticker", None):
            text = STICKER_TEXT
        with SessionLocal() as db:
            if db.scalar(select(Message.id).where(Message.chat_id == chat_id, Message.tg_message_id == msg.id)):
                return None  # ถูกบันทึกไปแล้ว (เช่น ส่งจากหน้าเว็บ แล้วอีเวนต์ NewMessage ตามมา)
            row = Message(
                chat_id=chat_id,
                tg_message_id=msg.id,
                sender_id=getattr(msg, "sender_id", None),
                sender_name=self._sender_name(sender),
                is_outgoing=bool(msg.out) or staff,  # ข้อความทีมงาน (บัญชีที่เชื่อมต่อ หรือรายชื่อทีมงาน)
                sent_by=self._sent_by_hints.pop((chat_id, msg.id), ""),
                text=text,
                media_path=media_path,
                date=msg.date.replace(tzinfo=None) if msg.date else utcnow(),
                analyzed=analyzed,
            )
            db.add(row)
            db.commit()
            data = chatbus.to_dict(row)
        chatbus.publish({"type": "message", "chat": chat_id, "message": data})
        return data

    def tag_sent_by(self, chat_id: int, tg_ids, by: str) -> None:
        """บันทึกว่าพนักงานคนไหนกดส่งข้อความ (ชื่อผู้ใช้เว็บ หรือ "LINE:ชื่อ") · ข้อความถูกบันทึกแล้ว = ใส่ชื่อทันที
        และแจ้งหน้าเว็บที่เปิดอยู่ · ยังไม่ถูกบันทึก = จำไว้ แล้วใส่ตอนอีเวนต์ Telegram ตามมา"""
        by = (by or "").strip()[:64]
        if not by:
            return
        for tg_id in tg_ids or []:
            if not tg_id:
                continue
            with SessionLocal() as db:
                row = db.scalar(select(Message).where(Message.chat_id == chat_id, Message.tg_message_id == tg_id))
                data = None
                if row is not None:
                    row.sent_by = by
                    db.commit()
                    data = chatbus.to_dict(row)
            if data:
                chatbus.publish({"type": "sender", "chat": chat_id, "message": data})
            else:
                if len(self._sent_by_hints) > 500:
                    self._sent_by_hints.clear()
                self._sent_by_hints[(chat_id, int(tg_id))] = by

    async def send_manual(self, chat_id: int, text: str, reply_to: int | None = None, by: str = "") -> dict:
        """ทีมงานพิมพ์ตอบในหน้าเว็บ -> ส่งเข้าแชทลูกค้าทันที แล้วบันทึก/กระจายให้ทุกหน้าที่เปิดอยู่ · คืนค่า dict ของข้อความ"""
        entity = await self._entity(chat_id)
        try:
            msg = await self.client.send_message(entity, text, reply_to=reply_to or None, link_preview=False)
        except RPCError as e:
            # ข้อความที่จะตอบกลับถูกลบไปแล้ว -> ส่งเป็นข้อความปกติแทน
            if not reply_to or "REPLY" not in str(e).upper():
                raise
            msg = await self.client.send_message(entity, text, link_preview=False)
        try:
            sender = await msg.get_sender()
        except (RPCError, ValueError, AttributeError):
            sender = None
        if by:
            self._sent_by_hints[(chat_id, msg.id)] = by.strip()[:64]
        data = self._store(chat_id, msg, sender, "", analyzed=True, staff=True)
        if data is None and by:  # อีเวนต์ NewMessage บันทึกไปก่อน -> ใส่ชื่อผู้ส่งให้แถวที่มีอยู่
            self._sent_by_hints.pop((chat_id, msg.id), None)
            self.tag_sent_by(chat_id, [msg.id], by)
        if data is None:  # อีเวนต์ NewMessage บันทึกไปก่อนแล้ว -> อ่านกลับมา
            with SessionLocal() as db:
                row = db.scalar(select(Message).where(Message.chat_id == chat_id, Message.tg_message_id == msg.id))
                data = chatbus.to_dict(row) if row else {
                    "id": 0, "tg": msg.id, "chat": chat_id, "name": "", "out": True, "text": text, "media": "", "hm": "", "at": ""}
        return data

    async def _handle_dev_message(self, event: events.NewMessage.Event) -> None:
        msg = event.message
        if msg.out or not self.on_dev_message:
            return
        try:
            sender = await event.get_sender()
        except RPCError:
            return
        username = (getattr(sender, "username", None) or "").lower()
        if username not in self.dev_users:
            return
        reply_to = getattr(msg, "reply_to_msg_id", None)
        self.on_dev_message({
            "chat_id": event.chat_id,
            "message_id": msg.id,
            "reply_to": reply_to,
            "text": msg.message or ("(รูปภาพ)" if msg.photo else ""),
            "username": username,
            "sender_name": self._sender_name(sender),
        })

    async def _handle_new_message(self, event: events.NewMessage.Event) -> None:
        if self.dev_group_id and event.chat_id == self.dev_group_id:
            await self._handle_dev_message(event)  # กลุ่มโปรแกรมเมอร์ ไม่ใช่แชทลูกค้า
            return
        if event.chat_id not in self._monitored:
            return
        msg = event.message
        with SessionLocal() as db:
            exists = db.scalar(select(Message.id).where(
                Message.chat_id == event.chat_id, Message.tg_message_id == msg.id))
        if exists:  # ถูกดึงมาแล้วจากการดึงย้อนหลัง
            return
        try:
            sender = await event.get_sender()
        except RPCError:
            sender = None
        if self.is_ignored(msg, sender):
            return  # ไม่รับข้อความจากบัญชีนี้ (เช่น บอทแจ้งเตือน)
        staff = self.is_staff(msg, sender)
        media_path = "" if staff else await self._download_image(event.chat_id, msg)
        # ข้อความของทีมงานไม่ต้องวิเคราะห์
        self._store(event.chat_id, msg, sender, media_path, analyzed=staff, staff=staff)
        if not staff and self.on_message:
            self.on_message(event.chat_id)

    async def _download_image(self, chat_id: int, msg) -> str:
        if getattr(msg, "sticker", None):
            return ""  # สติกเกอร์ไม่ใช่รูปปัญหา ไม่ต้องดาวน์โหลด
        ext = None
        if msg.photo:
            ext = ".jpg"
        elif msg.document and msg.file and msg.file.mime_type in IMAGE_MIME_EXT:
            ext = IMAGE_MIME_EXT[msg.file.mime_type]
        if not ext:
            return ""
        name = f"{abs(chat_id)}_{msg.id}{ext}"
        try:
            await msg.download_media(file=str(MEDIA_DIR / name))
        except (OSError, RPCError):
            log.exception("download media failed")
            return ""
        return name if (MEDIA_DIR / name).exists() else ""


telegram = TelegramService()

