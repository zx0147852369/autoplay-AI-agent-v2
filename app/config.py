import os
import secrets
from datetime import timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# บน Railway/คลาวด์ต้องรับการเชื่อมต่อจากภายนอก (0.0.0.0) และใช้ PORT ที่แพลตฟอร์มกำหนด
ON_CLOUD = bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID"))

# บน Railway ถ้าสร้าง Volume ไว้ จะเก็บข้อมูลใน Volume อัตโนมัติ (ไม่หายเมื่อ deploy ใหม่)
VOLUME_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "")
DATA_DIR = Path(os.getenv("DATA_DIR") or VOLUME_DIR or BASE_DIR / "data")
MEDIA_DIR = DATA_DIR / "media"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{(DATA_DIR / 'app.db').as_posix()}")

# รันบนคลาวด์แต่ข้อมูลไม่ได้อยู่ใน Volume -> ตั้งค่า ticket และการเชื่อมต่อ Telegram จะหายทุกครั้งที่ deploy
_on_volume = bool(VOLUME_DIR) and DATA_DIR.resolve().is_relative_to(Path(VOLUME_DIR).resolve())
EPHEMERAL_STORAGE = ON_CLOUD and not _on_volume

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

HOST = os.getenv("HOST") or ("0.0.0.0" if ON_CLOUD else "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))

# เวลาที่แสดงบนหน้าเว็บ (ค่าเริ่มต้น: เวลาประเทศไทย UTC+7)
DISPLAY_TZ = timezone(timedelta(hours=int(os.getenv("DISPLAY_TZ_OFFSET", "7"))))


def _load_secret_key() -> str:
    key = os.getenv("SECRET_KEY", "").strip()
    if key:
        return key
    # ไม่ได้ตั้ง SECRET_KEY ใน .env -> สร้างครั้งเดียวแล้วเก็บไว้ใน data/secret.key
    path = DATA_DIR / "secret.key"
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    key = secrets.token_urlsafe(48)
    path.write_text(key, encoding="utf-8")
    return key


SECRET_KEY = _load_secret_key()
