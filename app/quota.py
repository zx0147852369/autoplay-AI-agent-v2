"""นับการเรียก AI และคำนวณโควตาที่เหลือ

Gemini API ไม่บอกโควตาที่เหลือมากับคำตอบ ระบบจึงนับเองจากทุกครั้งที่เรียก แล้วเทียบกับเพดานที่ตั้งไว้
(ตัวเลขจริงดูได้ที่ https://aistudio.google.com/rate-limit) · โควตารายวันของ Google รีเซ็ตเที่ยงคืนเวลาแปซิฟิก
"""

import json
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from .database import AiUsage, SessionLocal, get_settings, utcnow

PACIFIC = ZoneInfo("America/Los_Angeles")

# ค่าเริ่มต้นจากข้อมูลโควตาฟรี (ก.ย. 2026) แก้ได้ในหน้าตั้งค่าให้ตรงกับ AI Studio · 0 = ไม่จำกัด
DEFAULT_LIMITS = {
    "gemini-3.8-flash": {"rpm": 5, "rpd": 20},
    "gemini-3.5-flash-lite": {"rpm": 15, "rpd": 500},
    "gemini-2.5-flash": {"rpm": 10, "rpd": 250},
}


def gemini_keys() -> list[str]:
    """คีย์ Gemini ทั้งหมดตามลำดับ: GEMINI_API_KEY (หลัก), GEMINI_API_KEY_2, _3, ... (สำรอง)

    แต่ละคีย์ควรมาจากคนละบัญชี Google / คนละโปรเจกต์ เพราะโควตาฟรีนับต่อโปรเจกต์"""
    keys = [os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""]
    keys += [os.getenv(f"GEMINI_API_KEY_{i}") or "" for i in range(2, 10)]
    keys += (os.getenv("GEMINI_API_KEYS") or "").replace("\n", ",").split(",")
    result = []
    for key in (k.strip() for k in keys):
        if key and key not in result:
            result.append(key)
    return result


# คีย์ที่ Google แจ้งปัญหาระดับบัญชี (เช่น เครดิตเติมเงินหมด 402) -> พักทั้งคีย์ทุกรุ่นชั่วคราว
_blocked: dict[str, tuple[datetime, str]] = {}
KEY_BLOCK_MINUTES = 60


def block_key(key: str, reason: str, minutes: int = KEY_BLOCK_MINUTES) -> None:
    _blocked[key] = (utcnow() + timedelta(minutes=minutes), reason)


def key_block(key: str) -> str:
    """เหตุผลที่คีย์นี้ถูกพัก (ค่าว่าง = ใช้ได้)"""
    entry = _blocked.get(key)
    if entry and entry[0] <= utcnow():
        _blocked.pop(key, None)
        return ""
    return entry[1] if entry else ""


def mask_key(key: str) -> str:
    return f"{key[:4]}…{key[-4:]}" if len(key) > 10 else "…"


def slot_label(slot: int) -> str:
    return "คีย์หลัก" if slot == 1 else f"คีย์สำรอง {slot - 1}"


def limits(settings: dict[str, str]) -> dict[str, dict[str, int]]:
    result = {m: dict(v) for m, v in DEFAULT_LIMITS.items()}
    try:
        saved = json.loads(settings.get("gemini_limits") or "{}")
    except json.JSONDecodeError:
        saved = {}
    for model, values in saved.items():
        if model in result and isinstance(values, dict):
            for key in ("rpm", "rpd"):
                try:
                    result[model][key] = max(0, int(values.get(key, result[model][key])))
                except (TypeError, ValueError):
                    pass
    return result


def day_start_utc(now: datetime | None = None) -> datetime:
    """เที่ยงคืนเวลาแปซิฟิกของวันนี้ (เวลาที่ Google รีเซ็ตโควตารายวัน) เป็น UTC แบบ naive"""
    now = (now or utcnow()).replace(tzinfo=timezone.utc).astimezone(PACIFIC)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc).replace(tzinfo=None)


def next_reset_utc(now: datetime | None = None) -> datetime:
    now = (now or utcnow()).replace(tzinfo=timezone.utc).astimezone(PACIFIC)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow.astimezone(timezone.utc).replace(tzinfo=None)


def record(model: str, ok: bool, code: str = "ok", input_tokens: int = 0, output_tokens: int = 0,
           slot: int = 1) -> None:
    with SessionLocal() as db:
        row = AiUsage(model=model, ok=ok, code=code, input_tokens=input_tokens or 0, output_tokens=output_tokens or 0,
                      key_slot=slot)
        db.add(row)
        db.flush()
        if row.id % 500 == 0:  # ล้างประวัติเก่ากว่า 30 วันเป็นครั้งคราว
            db.query(AiUsage).filter(AiUsage.at < utcnow() - timedelta(days=30)).delete()
        db.commit()


def _counts(db, model: str, since: datetime, ok_only: bool, slot: int | None = None) -> int:
    query = select(func.count(AiUsage.id)).where(AiUsage.model == model, AiUsage.at >= since)
    if slot is not None:
        query = query.where(AiUsage.key_slot == slot)
    if ok_only:
        query = query.where(AiUsage.ok.is_(True))
    return db.scalar(query) or 0


def _last_429(db, model: str, slot: int, since: datetime | None = None):
    query = select(func.max(AiUsage.at)).where(AiUsage.model == model, AiUsage.code == "429", AiUsage.key_slot == slot)
    if since is not None:
        query = query.where(AiUsage.at >= since)
    return db.scalar(query)


# Google ตอบ 429 ซ้ำๆ ทั้งที่ยังนับไม่ครบ = โควตารายวันของคีย์นี้หมดจริง (นับรวมการใช้จากที่อื่นด้วย)
DAILY_429_STREAK = 3


def available(model: str, slot: int = 1) -> tuple[bool, str]:
    """ยังเรียกรุ่นนี้ด้วยคีย์นี้ได้ไหม (ตามที่ระบบนับเอง) คืนค่า (ได้/ไม่ได้, เหตุผล)"""
    with SessionLocal() as db:
        lim = limits(get_settings(db)).get(model)
        if not lim:
            return True, ""
        now = utcnow()
        if lim["rpd"] and _counts(db, model, day_start_utc(now), ok_only=True, slot=slot) >= lim["rpd"]:
            return False, f"ใช้ครบ {lim['rpd']} ครั้งของวันนี้แล้ว"
        if lim["rpm"] and _counts(db, model, now - timedelta(seconds=60), ok_only=False, slot=slot) >= lim["rpm"]:
            return False, f"ครบ {lim['rpm']} ครั้งต่อนาทีแล้ว"
        last_429 = _last_429(db, model, slot)
        if last_429 and now - last_429 < timedelta(seconds=60):
            return False, "Google แจ้งโควตาเต็มเมื่อไม่ถึง 1 นาทีที่แล้ว"
        # 429 ติดกันหลายครั้งในวันนี้ (ไม่มีครั้งไหนสำเร็จคั่น) -> พักคีย์นี้ 15 นาที ไม่ต้องลองทุกข้อความ
        recent = list(db.scalars(select(AiUsage.code).where(
            AiUsage.model == model, AiUsage.key_slot == slot, AiUsage.at >= day_start_utc(now))
            .order_by(AiUsage.at.desc()).limit(DAILY_429_STREAK)))
        if len(recent) == DAILY_429_STREAK and all(c == "429" for c in recent) \
                and last_429 and now - last_429 < timedelta(minutes=15):
            return False, "Google แจ้งโควตาเต็มติดกันหลายครั้ง พักคีย์นี้ 15 นาที"
    return True, ""


def snapshot(settings: dict[str, str]) -> dict:
    """ข้อมูลสำหรับหลอดโควตาในหน้าเว็บ"""
    now = utcnow()
    start = day_start_utc(now)
    reset = next_reset_utc(now)
    lims = limits(settings)
    selected = settings.get("ai_model", "")
    keys = gemini_keys()
    slots = list(range(1, max(1, len(keys)) + 1))
    rows = []

    def level(used: int, limit: int, pct: int) -> str:
        return "full" if limit and used >= limit else "high" if pct >= 80 else "mid" if pct >= 50 else "ok"

    with SessionLocal() as db:
        for model, lim in lims.items():
            per_key = []
            for slot in slots:
                used = _counts(db, model, start, ok_only=True, slot=slot)
                failed = db.scalar(select(func.count(AiUsage.id)).where(
                    AiUsage.model == model, AiUsage.key_slot == slot, AiUsage.at >= start, AiUsage.ok.is_(False))) or 0
                pct = min(100, round(used / lim["rpd"] * 100)) if lim["rpd"] else 0
                ok, why = available(model, slot)
                blocked = key_block(keys[slot - 1]) if slot <= len(keys) else ""
                if blocked:
                    ok, why = False, blocked
                per_key.append({
                    "slot": slot, "label": slot_label(slot),
                    "mask": mask_key(keys[slot - 1]) if slot <= len(keys) else "",
                    "used": used, "failed": failed, "limit": lim["rpd"], "pct": pct,
                    "left": max(0, lim["rpd"] - used) if lim["rpd"] else None,
                    "level": level(used, lim["rpd"], pct),
                    "per_min": _counts(db, model, now - timedelta(seconds=60), ok_only=False, slot=slot),
                    "last_429": _last_429(db, model, slot, start), "ready": ok, "why": why, "blocked": blocked,
                })
            used = sum(k["used"] for k in per_key)
            limit = lim["rpd"] * len(slots)
            pct = min(100, round(used / limit * 100)) if limit else 0
            rows.append({
                "model": model, "used": used, "failed": sum(k["failed"] for k in per_key), "limit": limit,
                "left": sum(k["left"] for k in per_key) if lim["rpd"] else None, "pct": pct,
                "level": level(used, limit, pct),
                "per_min": sum(k["per_min"] for k in per_key), "rpm": lim["rpm"] * len(slots),
                "last_429": max((k["last_429"] for k in per_key if k["last_429"]), default=None),
                "selected": model == selected, "key_rows": per_key,
                "ready_keys": sum(1 for k in per_key if k["ready"]),
            })
        tokens = db.execute(select(func.coalesce(func.sum(AiUsage.input_tokens), 0),
                                   func.coalesce(func.sum(AiUsage.output_tokens), 0)).where(AiUsage.at >= start)).one()
    left = (reset - now).total_seconds()
    current = next((r for r in rows if r["selected"]), None)
    total_limit = sum(r["limit"] for r in rows if r["limit"])
    total_used = sum(r["used"] for r in rows)
    return {
        "rows": rows, "current": current, "is_gemini": selected.startswith("gemini"), "key_count": len(keys),
        "total_used": total_used, "total_limit": total_limit,
        "total_left": max(0, total_limit - total_used) if total_limit else None,
        "total_pct": min(100, round(total_used / total_limit * 100)) if total_limit else 0,
        "input_tokens": tokens[0], "output_tokens": tokens[1],
        "reset_at": reset, "reset_in": f"{int(left // 3600)} ชม. {int(left % 3600 // 60)} นาที",
    }
