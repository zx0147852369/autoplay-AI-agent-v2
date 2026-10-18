"""โทรตามทีมงานอนุมัติ: ร่างรออนุมัติค้างนานเกินกำหนดแล้วไม่มีใครอนุมัติ -> ระบบโทร + ส่งข้อความหาแอดมินซัพพอร์ตผ่าน Telegram

นับ "รออนุมัติ" = ร่างตอบลูกค้า (Telegram + LINE) ที่ยังไม่ถูกอนุมัติ/ปฏิเสธ + ticket ที่รอส่งเข้ากลุ่มโปรแกรมเมอร์
  - เริ่มโทรเมื่อมีรายการค้างนานกว่า chase_after_min นาที และยังไม่มีใครกดอนุมัติ/ปฏิเสธอะไรเลยในช่วงนั้น
  - ไม่มีใครรับ -> โทรซ้ำทุก chase_repeat_min นาที สูงสุด chase_max รอบ (แต่ละรอบเรียกคนถัดไปในรายชื่อ)
  - มีคนเริ่มอนุมัติ หรือคิวว่าง -> เริ่มนับรอบใหม่ทั้งหมด
เมื่อแอดมินกดรับสาย ระบบเล่นไฟล์เสียงที่เลือกไว้ให้ฟัง (chase_audio) แล้ววางสาย · ไม่รับสายจะดังจนครบเวลาแล้ววางเอง
ส่งข้อความสรุปแนบไปด้วยเมื่อเปิดไว้ · เล่นเสียงไม่ได้ (ไลบรารีไม่พร้อม) ก็ถอยไปโทรให้ดังอย่างเดียว
"""
import asyncio
import json
import logging
import re
from datetime import datetime, timedelta

from sqlalchemy import delete, func, select

from . import chase_audio, line_service
from .database import ChaseLog, ChaseSound, Reply, SessionLocal, Setting, Ticket, get_settings, int_setting, utcnow
from .telegram_service import telegram

log = logging.getLogger(__name__)

MAX_TARGETS = 5
RING_SECONDS = 30      # ให้โทรศัพท์ปลายทางดังกี่วินาทีก่อนวางสาย
WATCH_INTERVAL = 30    # ตรวจคิวทุกกี่วินาที
KEEP_LOGS = 200
TEST_TEXT = "ทดสอบระบบโทรตามอนุมัติ: ถ้าเห็นข้อความนี้และโทรศัพท์ดัง แปลว่าระบบแจ้งเตือนทำงานปกติ (ไม่ต้องทำอะไร)"
_USERNAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{3,31}")


def parse_targets(raw: str) -> tuple[list[str], list[str]]:
    """แยก username/ไอดี Telegram จากข้อความที่พิมพ์ (คั่นด้วยจุลภาค/ช่องว่าง/บรรทัดใหม่ · วางลิงก์ t.me ได้) -> (ใช้ได้, รูปแบบไม่ถูกต้อง)"""
    good, bad = [], []
    for part in re.split(r"[,;\s]+", raw or ""):
        p = re.sub(r"^(https?://)?(t\.me|telegram\.me)/", "", part.strip(), flags=re.I).lstrip("@").rstrip("/")
        if not p:
            continue
        if not (p.lstrip("-").isdigit() or _USERNAME.fullmatch(p)):
            bad.append(part.strip())
        elif p.lower() not in {g.lower() for g in good}:
            good.append(p)
    return good[:MAX_TARGETS], bad


def config(settings: dict[str, str]) -> dict:
    return {
        "enabled": settings.get("chase_enabled") == "1",
        "targets": parse_targets(settings.get("chase_targets", ""))[0],
        "after": int_setting(settings, "chase_after_min", 2, 240),
        "repeat": int_setting(settings, "chase_repeat_min", 2, 240),
        "max": int_setting(settings, "chase_max", 1, 10),
        "call": settings.get("chase_call") == "1",
        "message": settings.get("chase_message") == "1",
        "add_contact": settings.get("chase_add_contact") == "1",
        "audio": settings.get("chase_play_audio") == "1",
        "audio_repeat": int_setting(settings, "chase_audio_repeat", 1, 5),
        "sound_id": (settings.get("chase_sound_id") or "").strip(),
    }


def active_sound(db, sound_id: str) -> tuple[bytes, str]:
    """เสียงที่จะเล่นตอนรับสาย: ไฟล์ที่เลือกไว้ (หายไป/เสียหาย -> เสียงเตือนมาตรฐาน) · คืน (PCM, ชื่อที่แสดง)"""
    if sound_id.isdigit():
        row = db.get(ChaseSound, int(sound_id))
        if row is not None:
            try:
                return chase_audio.load_pcm(chase_audio.AUDIO_DIR / row.file), row.name
            except Exception:  # noqa: BLE001 - ไฟล์หาย/เสียหาย ใช้เสียงมาตรฐานแทน ไม่ให้โทรตามพัง
                log.warning("ไฟล์เสียงโทรตามอนุมัติ #%s เปิดไม่ได้ ใช้เสียงมาตรฐานแทน", sound_id)
    return chase_audio.load_pcm(chase_audio.builtin_wav()), "เสียงเตือนมาตรฐาน"


def waiting(db, now: datetime, after: int) -> dict:
    """รายการที่ค้างรออนุมัติ: ค้างนานกว่ากำหนด (หมวดร่างตอบลูกค้า / ส่งเข้ากลุ่มโปรแกรมเมอร์) + มีคนอนุมัติ/ปฏิเสธเมื่อไม่นานมานี้ไหม"""
    cut = now - timedelta(minutes=after)
    c_n, c_old = db.execute(select(func.count(Reply.id), func.min(Reply.created_at)).where(
        Reply.status == "pending", Reply.created_at <= cut)).one()
    d_n, d_old = db.execute(select(func.count(Ticket.id), func.min(Ticket.updated_at)).where(
        Ticket.dev_status == "pending", Ticket.updated_at <= cut)).one()
    olds = [x for x in (c_old, d_old) if x is not None]
    last = db.scalar(select(func.max(Reply.decided_at)))
    pending_all = (db.scalar(select(func.count(Reply.id)).where(Reply.status == "pending")) or 0) + \
                  (db.scalar(select(func.count(Ticket.id)).where(Ticket.dev_status == "pending")) or 0)
    return {"customer": c_n or 0, "dev": d_n or 0, "total": (c_n or 0) + (d_n or 0), "pending_all": pending_all,
            "oldest_min": int((now - min(olds)).total_seconds() // 60) if olds else 0,
            "active": last is not None and last >= cut}


def _state(db) -> dict:
    row = db.get(Setting, "chase_state")
    try:
        raw = json.loads(row.value) if row is not None and row.value else {}
        return {"round": int(raw.get("round", 0)), "last": datetime.fromisoformat(raw["last"]) if raw.get("last") else None}
    except (ValueError, TypeError, KeyError):
        return {"round": 0, "last": None}


def _set_state(db, rnd: int, last: datetime | None) -> None:
    value = json.dumps({"round": rnd, "last": last.isoformat() if last else ""}) if rnd else ""
    row = db.get(Setting, "chase_state")
    if row is None:
        db.add(Setting(key="chase_state", value=value))
    else:
        row.value = value
    db.commit()


def message_text(info: dict, rnd: int, rounds: int) -> str:
    lines = [f"⏰ ระบบเตือนอัตโนมัติ: มีรายการรออนุมัติค้าง {info['total']} รายการ นานสุด {info['oldest_min']} นาที ยังไม่มีใครอนุมัติ"]
    if info["customer"]:
        lines.append(f"• ร่างตอบลูกค้า {info['customer']} รายการ")
    if info["dev"]:
        lines.append(f"• รอส่งเข้ากลุ่มโปรแกรมเมอร์ {info['dev']} รายการ")
    base = line_service.public_url()
    if base:
        lines.append(f"เปิดหน้ารออนุมัติ: {base}/replies")
    lines.append(f"(ครั้งที่ {rnd}/{rounds} · ถ้ากำลังเปิดดูอยู่แล้ว ไม่ต้องสนใจข้อความนี้)")
    return "\n".join(lines)


def _record(db, kind: str, rnd: int, target: str, res: dict, info: dict) -> dict:
    row = ChaseLog(kind=kind, round=rnd, target=target[:64], target_name=res["name"][:128], waiting=info["total"], oldest_min=info["oldest_min"],
                   call_status=res["call"], call_error=res["call_error"][:500], msg_status=res["msg"], msg_error=res["msg_error"][:500], note=res.get("note", "")[:500])
    db.add(row)
    db.flush()
    keep = db.scalar(select(ChaseLog.id).order_by(ChaseLog.id.desc()).offset(KEEP_LOGS).limit(1))
    if keep is not None:
        db.execute(delete(ChaseLog).where(ChaseLog.id <= keep))
    db.commit()
    return {"id": row.id, "round": rnd, "target": target, **res}


async def tick(now: datetime | None = None) -> dict | None:
    """ตรวจคิวหนึ่งรอบ: ถึงเวลาโทรตามก็โทร/ส่งข้อความหาแอดมินคนถัดไป · คืนผลของรอบนี้ (None = ไม่ต้องทำอะไร)"""
    now = now or utcnow()
    with SessionLocal() as db:
        cfg = config(get_settings(db))
        if not cfg["enabled"] or not cfg["targets"] or not (cfg["call"] or cfg["message"]):
            return None
        info = waiting(db, now, cfg["after"])
        state = _state(db)
        if not info["total"] or info["active"]:
            if state["round"]:
                _set_state(db, 0, None)  # คิวว่าง/มีคนเริ่มอนุมัติแล้ว -> เหตุการณ์จบ เริ่มนับรอบใหม่
            return None
        if state["round"] >= cfg["max"] or (state["last"] and now - state["last"] < timedelta(minutes=cfg["repeat"])):
            return None
    if not telegram.connected:
        return None
    with SessionLocal() as db:  # โหลดไฟล์เสียงเฉพาะตอนจะโทรจริง
        audio = active_sound(db, cfg["sound_id"])[0] if cfg["audio"] and cfg["call"] else None
    rnd = state["round"] + 1
    target = cfg["targets"][(rnd - 1) % len(cfg["targets"])]
    res = await telegram.alert_user(target, message_text(info, rnd, cfg["max"]), cfg["call"], cfg["message"], RING_SECONDS, audio, cfg["audio_repeat"], cfg["add_contact"])
    log.warning("โทรตามอนุมัติรอบ %s/%s -> %s: โทร=%s ข้อความ=%s (ค้าง %s รายการ นานสุด %s นาที)",
                rnd, cfg["max"], target, res["call"], res["msg"], info["total"], info["oldest_min"])
    with SessionLocal() as db:
        out = _record(db, "auto", rnd, target, res, info)
        _set_state(db, rnd, now)
    return out


async def test_all() -> list[dict]:
    """แอดมินกดทดสอบ: โทร/ส่งข้อความทดสอบหาทุกคนในรายชื่อทันที (ไม่ขึ้นกับว่าเปิดระบบหรือยัง · ไม่นับรอบ)"""
    now = utcnow()
    with SessionLocal() as db:
        cfg = config(get_settings(db))
        info = waiting(db, now, cfg["after"])
        audio = active_sound(db, cfg["sound_id"])[0] if cfg["audio"] and cfg["call"] else None
    out = []
    for target in cfg["targets"]:
        res = await telegram.alert_user(target, TEST_TEXT, cfg["call"], cfg["message"] or not cfg["call"], RING_SECONDS, audio, cfg["audio_repeat"], cfg["add_contact"])
        with SessionLocal() as db:
            out.append(_record(db, "test", 0, target, res, info))
    return out


_test_task: asyncio.Task | None = None


def testing() -> bool:
    return _test_task is not None and not _test_task.done()


def start_test() -> bool:
    """เริ่มโทรทดสอบเบื้องหลัง (สายที่เล่นเสียงใช้เวลานาน ไม่รอในคำขอหน้าเว็บ) · มีรอบที่กำลังทดสอบอยู่แล้วคืน False"""
    global _test_task
    if testing():
        return False
    _test_task = asyncio.get_running_loop().create_task(test_all())
    return True


def status(db, now: datetime | None = None) -> dict:
    """สถานะสำหรับหน้าตั้งค่า: ตอนนี้ระบบอยู่ในสภาพไหน + รอบถัดไปคาดว่าเมื่อไร"""
    now = now or utcnow()
    cfg = config(get_settings(db))
    info = waiting(db, now, cfg["after"])
    st = _state(db)
    phase, next_at = "idle", None
    if not cfg["enabled"]:
        phase = "off"
    elif not cfg["targets"]:
        phase = "no_targets"
    elif not telegram.connected:
        phase = "disconnected"
    elif info["total"] and info["active"]:
        phase = "active"
    elif info["total"] and st["round"] >= cfg["max"]:
        phase = "maxed"
    elif info["total"]:
        phase, next_at = "due", (max(now, st["last"] + timedelta(minutes=cfg["repeat"])) if st["last"] else now)
    return {"cfg": cfg, "info": info, "round": st["round"], "last": st["last"], "phase": phase, "next_at": next_at, "testing": testing()}


async def watcher() -> None:
    while True:
        await asyncio.sleep(WATCH_INTERVAL)
        try:
            await tick()
        except Exception:  # noqa: BLE001
            log.exception("chase tick failed")
