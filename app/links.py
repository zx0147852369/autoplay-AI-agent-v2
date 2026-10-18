"""ลิงก์เว็บไซต์: รวมลิงก์ที่ทีมใช้งานบ่อยไว้ในระบบ กดเปิดในแท็บใหม่ได้เลย

ตรวจลิงก์ให้ปลอดภัยก่อนเก็บ: รับเฉพาะ http/https (ไม่รับ javascript: data: mailto: ฯลฯ), ไม่มีช่องว่าง, ไม่ฝังชื่อผู้ใช้/รหัสผ่านในลิงก์
(ลิงก์ปลอมแนว https://bank.com@evil.com), โดเมนต้องเป็นรูปแบบที่ถูกต้อง
"""
import re
from urllib.parse import urlsplit

MAX_LINKS = 300
MAX_URL = 2000
_LABEL = r"[\w\u0E00-\u0E7F-]+"   # ส่วนหนึ่งของชื่อโดเมน (รับตัวอักษรต่างภาษา รวมภาษาไทยที่มีสระ/วรรณยุกต์)
_HOST = re.compile(rf"^(?:localhost|{_LABEL}(?:\.{_LABEL})+)$", re.UNICODE)
_ANY_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*):")
_HOST_PORT = re.compile(r"^[^/\s?#]+:\d+(?:[/?#]|$)")


def normalize_url(raw: str) -> str:
    """วางลิงก์อะไรมาก็ได้ -> ลิงก์ http/https ที่ใช้ได้ (ไม่ใส่ http มาให้เติม https://) · ไม่ถูกต้อง -> ValueError พร้อมเหตุผลภาษาไทย"""
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("ใส่ลิงก์เว็บไซต์ก่อน")
    if len(raw) > MAX_URL:
        raise ValueError(f"ลิงก์ยาวเกิน {MAX_URL} ตัวอักษร")
    if re.search(r"[\s\x00-\x1f\x7f]", raw):
        raise ValueError("ลิงก์มีช่องว่างหรืออักขระที่ใช้ไม่ได้")
    m = _ANY_SCHEME.match(raw)
    if m and m.group(1).lower() not in ("http", "https") and not _HOST_PORT.match(raw):
        raise ValueError("รองรับเฉพาะลิงก์เว็บ (http / https)")
    if not re.match(r"^https?://", raw, re.IGNORECASE):
        if "://" in raw:
            raise ValueError("รองรับเฉพาะลิงก์เว็บ (http / https)")
        raw = "https://" + raw.lstrip("/")
    try:
        parts = urlsplit(raw)
        port = parts.port
    except ValueError:
        raise ValueError("ลิงก์ไม่ถูกต้อง") from None
    host = (parts.hostname or "").lower()
    if "@" in parts.netloc:
        raise ValueError("ลิงก์ที่ฝังชื่อผู้ใช้/รหัสผ่านไว้ไม่ปลอดภัย จึงไม่รับ")
    if not host or not _HOST.match(host):
        raise ValueError("ชื่อเว็บไซต์ในลิงก์ไม่ถูกต้อง")
    netloc = f"[{host}]" if ":" in host else host
    if port:
        netloc += f":{port}"
    out = f"{parts.scheme.lower()}://{netloc}{parts.path}"
    if parts.query:
        out += f"?{parts.query}"
    if parts.fragment:
        out += f"#{parts.fragment}"
    return out


def safe_url(raw: str) -> str:
    """ลิงก์ที่เก็บไว้จากที่อื่น (เช่น เว็บประจำแชท) -> ลิงก์ที่เปิดได้อย่างปลอดภัย · ใช้ไม่ได้ = ว่าง"""
    try:
        return normalize_url(raw)
    except ValueError:
        return ""


def host_of(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def url_key(url: str) -> str:
    """ใช้เทียบว่าเป็นลิงก์เดียวกันไหม: ไม่สน http/https · www. · ตัวพิมพ์ของโดเมน · เครื่องหมาย / ท้ายลิงก์ · #ส่วนท้าย"""
    p = urlsplit(url)
    return f"{host_of(url)}{(':' + str(p.port)) if p.port else ''}{p.path.rstrip('/')}{('?' + p.query) if p.query else ''}"


def parse_bulk(text: str) -> list[tuple[str, str]]:
    """วางหลายลิงก์ทีเดียว: บรรทัดละ 1 ลิงก์ หรือ 'ชื่อ | ลิงก์' -> [(ชื่อ, ลิงก์ดิบ)] (ข้ามบรรทัดว่าง)"""
    out = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        title, sep, url = line.rpartition("|") if "|" in line else ("", "", line)
        out.append((title.strip(), url.strip()))
    return out


# ---------------------------------------------------------------- ล็อกอินอัตโนมัติ (ฟอร์มล็อกอินที่ส่งตรงไปยังเว็บนั้น)
_FIELD = re.compile(r"^[A-Za-z0-9_.\-\[\]:]{1,60}$")
MAX_EXTRA = 10


def field_name(raw: str, default: str) -> str:
    """ชื่อช่องในฟอร์มล็อกอิน (ค่า name ของ input) · ว่าง = ค่าเริ่มต้น · รูปแบบแปลก -> ValueError"""
    raw = (raw or "").strip()
    if not raw:
        return default
    if not _FIELD.match(raw):
        raise ValueError(f"ชื่อช่อง “{raw[:30]}” ไม่ถูกต้อง (ใช้ได้เฉพาะ a-z ตัวเลข _ - . [ ] ไม่เกิน 60 ตัว)")
    return raw


def parse_extra(text: str) -> dict[str, str]:
    """ช่องเพิ่มเติมที่ฟอร์มล็อกอินต้องส่งด้วย (เช่น remember=1) บรรทัดละ ชื่อ=ค่า · ไม่เกิน MAX_EXTRA ช่อง"""
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        key, sep, val = line.partition("=")
        if not sep:
            raise ValueError(f"ช่องเพิ่มเติมต้องเขียนเป็น ชื่อ=ค่า (บรรทัด: {line[:30]})")
        out[field_name(key, "")] = val.strip()[:200]
        if len(out) > MAX_EXTRA:
            raise ValueError(f"ช่องเพิ่มเติมใส่ได้ไม่เกิน {MAX_EXTRA} ช่อง")
    out.pop("", None)
    return out


def check_login_url(link_url: str, login_url: str) -> str:
    """URL ที่รับฟอร์มล็อกอิน: ต้องเป็นเว็บเดียวกับลิงก์ (กันส่งรหัสไปเว็บอื่น) · ว่าง = ใช้ลิงก์เอง"""
    login_url = (login_url or "").strip()
    if not login_url:
        return ""
    clean = normalize_url(login_url)
    from .site_checker import same_site
    if not same_site(host_of(clean), host_of(link_url)):
        raise ValueError("URL ที่รับฟอร์มล็อกอินต้องเป็นเว็บเดียวกับลิงก์ (กันส่งรหัสผ่านไปเว็บอื่น)")
    return clean