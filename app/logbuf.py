"""เก็บ log ของบอท ทั้งแบบดูสดในหน้าเว็บ และแบบไฟล์รายวันบน Volume เพื่อเช็คย้อนหลัง

- ในหน่วยความจำ (ring buffer ~800 บรรทัด) สำหรับหน้า "วันนี้ (สด)" · หายเมื่อดีพลอยใหม่
- ไฟล์รายวันใน DATA_DIR/logs/YYYY-MM-DD.log (ตามเวลาไทย) เก็บ 30 วันล่าสุด · อยู่บน Volume ถาวร
"""

import logging
import re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from .config import DATA_DIR, DISPLAY_TZ

MAX = 800
KEEP_DAYS = 30
LOG_DIR = Path(DATA_DIR) / "logs"
_RECORDS: deque = deque(maxlen=MAX)
_SEQ = 0
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_fmt = logging.Formatter()
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_LINE_RE = re.compile(r"^(\d{2}:\d{2}:\d{2}) (DEBUG|INFO|WARNING|ERROR|CRITICAL) (\S+): (.*)$")


class BufferHandler(logging.Handler):
    """เก็บ log ล่าสุดในหน่วยความจำ (ดูสดในหน้าเว็บ)"""

    def emit(self, record: logging.LogRecord) -> None:
        global _SEQ
        try:
            msg = record.getMessage()
            if record.exc_info:
                msg += "\n" + _fmt.formatException(record.exc_info)
        except Exception:  # noqa: BLE001 - handler ห้าม throw
            return
        _SEQ += 1
        _RECORDS.append({
            "id": _SEQ,
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": msg,
        })


class DailyFileHandler(logging.Handler):
    """เขียน log ลงไฟล์รายวัน (ตามเวลาไทย) เก็บ KEEP_DAYS วันล่าสุด"""

    def __init__(self, directory: Path, keep: int = KEEP_DAYS) -> None:
        super().__init__()
        self.dir = Path(directory)
        self.keep = keep
        self._date = ""
        self._fh = None
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            local = datetime.fromtimestamp(record.created, DISPLAY_TZ)
            day = local.strftime("%Y-%m-%d")
            if day != self._date or self._fh is None:
                if self._fh:
                    self._fh.close()
                self._fh = open(self.dir / f"{day}.log", "a", encoding="utf-8")
                self._date = day
                self._prune()
            line = f"{local.strftime('%H:%M:%S')} {record.levelname} {record.name}: {record.getMessage()}"
            self._fh.write(line + "\n")
            if record.exc_info:
                self._fh.write(_fmt.formatException(record.exc_info) + "\n")
            self._fh.flush()
        except Exception:  # noqa: BLE001 - handler ห้าม throw
            pass

    def _prune(self) -> None:
        files = sorted(self.dir.glob("*.log"))
        for f in files[:-self.keep]:
            try:
                f.unlink()
            except OSError:
                pass


def install(level: int = logging.INFO) -> None:
    mem = BufferHandler()
    mem.setLevel(level)
    daily = DailyFileHandler(LOG_DIR)
    daily.setLevel(level)
    root = logging.getLogger()
    root.addHandler(mem)
    root.addHandler(daily)
    # uvicorn ส่ง log ขึ้น root อยู่แล้ว -> เปิด propagate ให้ชัวร์ ไม่ต้องติด handler ซ้ำ (กัน log ซ้ำ 2 บรรทัด)
    # ไม่ยุ่งกับ uvicorn.access เพราะจะทำให้ log ทุก request รก
    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).propagate = True


def records(after: int = 0, level: str = "", limit: int = 500) -> dict:
    """log สดจากหน่วยความจำ (สำหรับหน้า 'วันนี้')"""
    thr = _LEVELS.get(level, 0)
    items = [r for r in _RECORDS if r["id"] > after and (not thr or _LEVELS.get(r["level"], 0) >= thr)]
    return {"records": items[-limit:], "last": _SEQ, "total": len(_RECORDS)}


def available_days() -> list[str]:
    """รายการวันที่มีไฟล์ log (ใหม่ -> เก่า)"""
    try:
        return sorted((f.stem for f in LOG_DIR.glob("*.log") if _DAY_RE.match(f.stem)), reverse=True)
    except OSError:
        return []


def day_path(day: str) -> Path | None:
    """path ไฟล์ log ของวันนั้น (ตรวจรูปแบบวันกัน path traversal)"""
    if not _DAY_RE.match(day or ""):
        return None
    p = (LOG_DIR / f"{day}.log").resolve()
    if p.parent != LOG_DIR.resolve() or not p.is_file():
        return None
    return p


def read_day(day: str, level: str = "", limit: int = 5000) -> list[dict]:
    """อ่านไฟล์ log ของวันนั้น แปลงเป็นรายการเรคคอร์ด (สำหรับแสดงในหน้าเว็บ)"""
    path = day_path(day)
    if not path:
        return []
    thr = _LEVELS.get(level, 0)
    out: list[dict] = []
    i = 0
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    for line in text.splitlines():
        m = _LINE_RE.match(line)
        if m:
            i += 1
            out.append({"id": i, "ts": f"{day}T{m.group(1)}", "level": m.group(2),
                        "name": m.group(3), "msg": m.group(4)})
        elif out:  # บรรทัดต่อ (เช่น stack trace) ต่อท้ายข้อความก่อนหน้า
            out[-1]["msg"] += "\n" + line
    if thr:
        out = [r for r in out if _LEVELS.get(r["level"], 0) >= thr]
    return out[-limit:]
