"""ปกปิดข้อมูลส่วนตัวก่อนส่งให้ AI ภายนอก แล้วคืนค่าเดิมในคำตอบ

ข้อความที่ส่งออกไปจะเหลือแค่ตัวแทน เช่น [PHONE_1] [EMAIL_1] [URL_2] [NAME_1] · AI ภายนอกเห็นเฉพาะตัวแทน ไม่เห็นข้อมูลจริง
พอ AI ตอบกลับมาพร้อมตัวแทนเดิม ระบบแทนค่าจริงกลับให้ก่อนนำไปใช้ (เช่น ร่างคำตอบลูกค้าจึงยังมีเบอร์/ลิงก์ที่ถูกต้อง)
ตารางตัวแทน <-> ค่าจริงอยู่ในหน่วยความจำของคำขอนั้นเท่านั้น ไม่เขียนลงดิสก์ ฐานข้อมูล หรือ log

ปกปิดได้: คีย์/โทเคน รหัสผ่าน OTP อีเมล ลิงก์และโดเมน IP เบอร์โทร เลขยาว (บัตร บัญชี อ้างอิง) @ชื่อผู้ใช้ ยูสเซอร์ที่มีเลข
          ชื่อที่ระบบรู้จัก (ผู้ส่งข้อความ ลูกค้าใน ticket ชื่อแชท) และคำที่แอดมินกำหนดเพิ่ม
ปกปิดไม่ได้: ชื่อ/ที่อยู่ที่ลูกค้าพิมพ์เองโดยระบบไม่รู้จัก และข้อมูลในรูปภาพ (สลิป ภาพหน้าจอ) -> ตั้งค่าเริ่มต้นเป็นไม่ส่งรูป

ระดับ "เข้มงวด" (strict · ค่าเริ่มต้น) เพิ่มจากมาตรฐาน:
  - ข้อมูลธุรกิจ ฐานความรู้ คู่มือ และบทเรียนของ AI ไม่ถูกส่งให้ AI ภายนอกเลย (ai_service.external_strict)
  - ข้อความที่ส่งปกปิดตัวเลข 4 หลักขึ้นไป/จำนวนเงิน และชื่อแบรนด์/ตัวย่อ (SCB, UFA168, KBank) ด้วย
"""
import json
import re
import time
from collections import Counter, deque

KIND_LABELS = {
    "SECRET": "คีย์/รหัสผ่าน/OTP", "EMAIL": "อีเมล", "URL": "ลิงก์/โดเมน", "IP": "IP", "PHONE": "เบอร์โทร",
    "ID": "เลขบัตร/บัญชี/อ้างอิง", "USER": "ยูสเซอร์/@ชื่อผู้ใช้", "NAME": "ชื่อ/คำที่กำหนด",
    "AMT": "จำนวนเงิน/ตัวเลข", "BRAND": "ชื่อแบรนด์/ตัวย่อ",
}
LEVELS = ("strict", "standard")
# คำเทคนิคทั่วไปที่ไม่ใช่ข้อมูลบริษัท (โหมดเข้มงวดไม่ปกปิด เพื่อไม่ให้คำสั่งในพรอมต์เพี้ยน)
_BRAND_SAFE = {"json", "schema", "url", "api", "ok", "ai", "id", "pdf", "png", "jpg", "jpeg", "gif", "qr", "atm", "sms", "otp", "vip",
               "ticket", "line", "telegram", "google", "android", "ios", "chrome", "http", "https", "html", "css", "ip", "app", "web",
               "login", "logout", "admin", "user", "wifi", "pin"}
MAX_TERMS = 60
TERM_MIN, TERM_MAX = 3, 80
# ชื่อที่ไม่ใช่ข้อมูลส่วนตัว (กันปกปิดคำทั่วไปจนข้อความอ่านไม่รู้เรื่อง)
_NAME_STOP = {"admin", "support", "team", "user", "customer", "staff", "unknown", "none", "null", "telegram", "line", "bot",
              "ลูกค้า", "ทีมงาน", "แอดมิน", "ซัพพอร์ต", "ไม่ระบุ"} | {k.lower() for k in KIND_LABELS}

_ASCII_L, _ASCII_R = r"(?<![A-Za-z0-9])", r"(?![A-Za-z0-9])"
_THAI = "฀-๿"

# ---------------------------------------------------------------- รูปแบบที่ตรวจจับ (ASCII ล้วน เพราะภาษาไทยเขียนติดกันไม่มีเว้นวรรค)
_SECRET_TOKEN = re.compile(
    r"(?<![A-Za-z0-9_-])(?:(?:sk|pk|rk|sess)-[A-Za-z0-9_-]{16,}|AIza[0-9A-Za-z_-]{30,}"
    r"|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|(?:ghp|gho|ghs|xox[abprs])[_-][A-Za-z0-9_-]{20,}"
    r"|\d{8,10}:[A-Za-z0-9_-]{30,})")
_LONG_TOKEN = re.compile(r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{32,}(?![A-Za-z0-9_-])")
_KEYWORD_VALUE = re.compile(r"((?<![A-Za-z])(?:api[_ -]?key|apikey|secret|token)(?![A-Za-z])\s*[:=]\s*)([^\s,;\"']{6,})", re.I)
_PW_WORD = r"(?:(?<![A-Za-z])(?:password|passwd|pass|pwd)(?![A-Za-z])|รหัสผ่าน|รหัส|พาสเวิร์ด|พาส)"
_PW_VALUE = r"[A-Za-z0-9!@#$%^&*._~+=/-]{4,}"
# รหัสผ่าน: มีตัวคั่น (: = คือ เป็น is) -> ค่าอะไรก็ได้ · ไม่มีตัวคั่น -> ค่าต้องมีตัวเลข/อักขระพิเศษ (กัน "password policy" ถูกปกปิด)
_PASSWORD = re.compile(r"(" + _PW_WORD + r"\s*(?:คือ|เป็น|is|[:=：])\s*)(" + _PW_VALUE + r")|(" + _PW_WORD + r"\s*)((?=\S*[\d!@#$%^&*._~+=/-])" + _PW_VALUE + r")", re.I)
_OTP = re.compile(r"((?<![A-Za-z])(?:otp|pin)(?![A-Za-z])\s*(?:คือ|เป็น|is|[:=：])?\s*)(\d{4,8})(?!\d)", re.I)
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
_TLDS = ("com|net|org|info|biz|co|th|io|me|app|dev|xyz|vip|bet|live|club|online|site|shop|top|win|cc|tv|us|uk|asia|pro|link|click|"
         "icu|cloud|store|tech|fun|one|ws|to|ai|ph|sg|my|vn|jp|cn|kr|hk|tw|la|in|gg|ly|bz|casino|game|games|page|work|world|life|"
         "today|network|digital|space|website|run|cx|pw|mobi|name|tk|ml|ga|cf|gq|bio|lol|wtf|best|fit|bar|cam|buzz|rest|vegas")
_URL = re.compile(
    r"https?://[^\s<>\"'\])}" + _THAI + r"]+|(?<![A-Za-z0-9@._-])www\.[^\s<>\"'\])}" + _THAI + r"]+"
    r"|(?<![A-Za-z0-9@._-])(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+(?:" + _TLDS + r")(?![A-Za-z0-9-])"
    r"(?::\d{2,5})?(?:/[^\s<>\"'\])}" + _THAI + r"]*)?", re.I)
_IP = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
_PHONE = re.compile(r"(?<!\d)(?:\+?66|0)[\s-]?\d{1,2}[\s-]?\d{3}[\s-]?\d{4}(?!\d)|(?<![\d+])\+\d{1,3}[\s-]?\d{6,12}(?!\d)")
# เลขยาว: ติดกัน 7 หลักขึ้นไป (ไม่นับเลขหลัง # เช่น #1234567 ซึ่งเป็นเลขข้อความที่ AI ต้องอ้างอิงกลับ) หรือรูปแบบบัตร/บัญชีที่มีขีด/เว้นวรรค
_LONGNUM = re.compile(
    r"(?<![A-Za-z0-9#])(?:\d{7,}|\d-\d{4}-\d{5}-\d{2}-\d|\d{3}-\d-\d{5}-\d|\d{4}[ -]\d{4}[ -]\d{4}(?:[ -]\d{1,4})?)(?!\d)")
_USERNAME = re.compile(
    r"((?:ยูสเซอร์เนม|ยูสเซอร์|ยูเซอร์|ยูส|ไอดี|(?<![A-Za-z])(?:user\s*name|user\s*id|username|userid|user|id)(?![A-Za-z]))"
    r"\s*(?:คือ|เป็น|is|[:=：])?\s*)(@?(?=[A-Za-z0-9_.-]*\d)[A-Za-z0-9][A-Za-z0-9_.-]{3,})", re.I)
_HANDLE = re.compile(r"(?<![A-Za-z0-9._@])@[A-Za-z0-9_]{3,}")
_TOKEN_RE = re.compile(r"\[\s*([A-Za-z]{2,8})\s*[_\s-]\s*(\d{1,4})\s*\]")
# ระดับเข้มงวด: จำนวนเงิน (1,000 / 25,500.50) และตัวเลข 4 หลักขึ้นไป (ไม่นับเลขหลัง # = เลขข้อความ/ticket ที่ AI ต้องอ้างอิงกลับ)
_AMT = re.compile(r"(?<![A-Za-z0-9#_\[.,])(?:\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{4,}(?:\.\d+)?)(?![A-Za-z0-9_\]])")
# คำละติน 3 ตัวขึ้นไปที่เป็นตัวย่อ/ชื่อแบรนด์ (มีตัวพิมพ์ใหญ่หลังตัวแรก หรือมีตัวเลขปน) · R12 / T7 (รหัสอ้างอิงในพรอมต์เรียนรู้) ไม่นับ
_BRAND = re.compile(r"(?<![A-Za-z0-9_\[])[A-Za-z][A-Za-z0-9]{2,}(?![A-Za-z0-9_\]])")


def clean_terms(raw, limit: int = MAX_TERMS) -> list[str]:
    """คำที่ต้องปกปิดเพิ่ม (บรรทัดละ 1 คำ หรือคั่นด้วยจุลภาค) -> รายการที่ใช้ได้ ตัดซ้ำ/สั้นเกิน/คำทั่วไป"""
    items = re.split(r"[\n\r,;]+", raw) if isinstance(raw, str) else list(raw or [])
    out, seen = [], set()
    for item in items:
        term = re.sub(r"\s+", " ", str(item or "")).strip()
        low = term.lower()
        if not (TERM_MIN <= len(term) <= TERM_MAX) or low in seen or low in _NAME_STOP or re.fullmatch(r"[\W\d_]+", term):
            continue
        seen.add(low)
        out.append(term)
        if len(out) >= limit:
            break
    return out


def _term_pattern(term: str) -> str:
    left = _ASCII_L if re.match(r"[A-Za-z0-9]", term[0]) else ""
    right = _ASCII_R if re.match(r"[A-Za-z0-9]", term[-1]) else ""
    return left + re.escape(term).replace(r"\ ", r"\s+") + right


class Shield:
    """ปกปิด/คืนค่าสำหรับ 1 คำขอ · names = ชื่อที่ระบบรู้จัก · terms = คำที่แอดมินกำหนดเพิ่ม"""

    def __init__(self, names=(), terms=(), level: str = "standard"):
        self.strict = level == "strict"
        self._by_value: dict[tuple[str, str], str] = {}
        self._by_token: dict[str, str] = {}
        self.counts: Counter = Counter()   # ชนิด -> จำนวนที่ปกปิด (นับทุกครั้งที่แทน)
        self._kinds: Counter = Counter()   # ชนิด -> เลขลำดับตัวแทนล่าสุด
        words: list[str] = []
        for name in names or ():
            name = re.sub(r"\s+", " ", str(name or "")).strip()
            words += [name, *name.split(" ")] if " " in name else [name]
        words += list(clean_terms(terms))
        words = clean_terms(words, limit=300)
        words.sort(key=len, reverse=True)  # คำยาวก่อน กันแทนทับซ้อน
        self._words = re.compile("|".join(_term_pattern(w) for w in words), re.I) if words else None

    # ---- ตัวแทน
    def _token(self, kind: str, value: str) -> str:
        key = (kind, value)
        if key not in self._by_value:
            self._kinds[kind] += 1
            token = f"{kind}_{self._kinds[kind]}"
            self._by_value[key] = f"[{token}]"
            self._by_token[token] = value
        self.counts[kind] += 1
        return self._by_value[key]

    def _brand(self, m: re.Match) -> str:
        word = m.group(0)
        if word.lower() in _BRAND_SAFE or re.fullmatch(r"[A-Z]\d{1,6}", word):
            return word
        if re.search(r"\d", word) or re.search(r"[A-Z]", word[1:]):
            return self._token("BRAND", word)
        return word

    def mask(self, text: str, extras: bool = False) -> str:
        """extras=True (ระดับเข้มงวด) = ปกปิดจำนวนเงิน/ตัวเลขยาว และชื่อแบรนด์/ตัวย่อเพิ่มด้วย · ใช้กับเนื้อหาข้อความ ไม่ใช้กับคำสั่งระบบ"""
        if not text:
            return text or ""
        t = self._mask_standard(text)
        if extras and self.strict:
            t = _AMT.sub(lambda m: self._token("AMT", m.group(0)), t)
            t = _BRAND.sub(self._brand, t)
        return t

    def _mask_standard(self, text: str) -> str:
        t = _SECRET_TOKEN.sub(lambda m: self._token("SECRET", m.group(0)), text)
        t = _KEYWORD_VALUE.sub(lambda m: m.group(1) + self._token("SECRET", m.group(2)), t)
        t = _PASSWORD.sub(lambda m: (m.group(1) or m.group(3)) + self._token("SECRET", m.group(2) or m.group(4)), t)
        t = _OTP.sub(lambda m: m.group(1) + self._token("SECRET", m.group(2)), t)
        t = _LONG_TOKEN.sub(lambda m: self._token("SECRET", m.group(0)) if re.search(r"[A-Za-z]", m.group(0)) and re.search(r"\d", m.group(0))
                            else m.group(0), t)
        t = _EMAIL.sub(lambda m: self._token("EMAIL", m.group(0)), t)
        t = _URL.sub(lambda m: self._token("URL", m.group(0)), t)
        t = _IP.sub(lambda m: self._token("IP", m.group(0)) if all(int(p) <= 255 for p in m.group(0).split(".")) else m.group(0), t)
        t = _PHONE.sub(lambda m: self._token("PHONE", m.group(0)), t)
        t = _LONGNUM.sub(lambda m: self._token("ID", m.group(0)), t)
        t = _USERNAME.sub(lambda m: m.group(1) + self._token("USER", m.group(2)), t)
        t = _HANDLE.sub(lambda m: self._token("USER", m.group(0)), t)
        if self._words:
            t = self._words.sub(lambda m: self._token("NAME", m.group(0)), t)
        return t

    def mask_parts(self, parts: list[tuple]) -> list[tuple]:
        """ปกปิดเฉพาะส่วนที่เป็นข้อความ (รูปภาพผ่านไปตามเดิม · การส่งรูปควบคุมที่ตั้งค่าแยกต่างหาก)"""
        return [("text", self.mask(p[1], extras=True)) if p[0] == "text" else p for p in parts]

    def unmask(self, text: str, as_json: bool = False) -> str:
        """แทนตัวแทนที่ AI ส่งกลับมาด้วยค่าจริง · ตัวแทนที่ไม่รู้จักปล่อยไว้ตามเดิม (แอดมินจะเห็นตอนตรวจร่าง)
        as_json = ข้อความเป็น JSON -> ใส่ค่าจริงแบบ escape ให้ยังเป็น JSON ที่ถูกต้อง"""
        def sub(m: re.Match) -> str:
            original = self._by_token.get(f"{m.group(1).upper()}_{int(m.group(2))}")
            if original is None:
                return m.group(0)
            return json.dumps(original, ensure_ascii=False)[1:-1] if as_json else original
        return _TOKEN_RE.sub(sub, text or "")

    @property
    def total(self) -> int:
        return sum(self.counts.values())


# ---------------------------------------------------------------- สถิติ (ในหน่วยความจำ รีเซ็ตเมื่อรีสตาร์ต · ไม่มีเนื้อหาข้อความ)
_stats = {"requests": 0, "items": 0, "by_kind": Counter(), "since": time.strftime("%Y-%m-%d %H:%M", time.gmtime())}


# ข้อความที่ส่งออกล่าสุด (เฉพาะฉบับที่ปกปิดแล้ว = สิ่งเดียวกับที่ AI ภายนอกได้รับ) เก็บในหน่วยความจำ 5 รายการ ให้แอดมินตรวจสอบได้
_recent: deque = deque(maxlen=5)
RECENT_CLIP = 6000


def record(shield: Shield, model: str = "", system: str = "", parts: list[tuple] | None = None) -> None:
    _stats["requests"] += 1
    _stats["items"] += shield.total
    _stats["by_kind"].update(shield.counts)
    if system or parts:
        body = "\n\n".join(p[1] if p[0] == "text" else f"({p[0]})" for p in parts or [])
        _recent.appendleft({"at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), "model": model, "items": shield.total,
                            "system": system[:RECENT_CLIP], "body": body[:RECENT_CLIP]})


def recent() -> list[dict]:
    return list(_recent)


def snapshot() -> dict:
    return {"requests": _stats["requests"], "items": _stats["items"], "since": _stats["since"],
            "by_kind": {KIND_LABELS.get(k, k): n for k, n in _stats["by_kind"].most_common()}}


def enabled(settings: dict) -> bool:
    return (settings.get("custom_ai_mask") or "on") != "off"


def level(settings: dict) -> str:
    """ระดับการปกปิด: strict (ค่าเริ่มต้น) / standard"""
    return "standard" if (settings.get("custom_ai_mask_level") or "strict") == "standard" else "strict"


def preview(text: str, terms="", names=(), lvl: str = "strict") -> dict:
    """ตัวอย่างว่าข้อความจะถูกปกปิดอย่างไร (ไม่นับสถิติ ไม่เก็บ)"""
    shield = Shield(names=names, terms=terms, level=lvl)
    masked = shield.mask(text, extras=True)
    return {"masked": masked, "counts": {KIND_LABELS.get(k, k): n for k, n in shield.counts.items()}, "total": shield.total}
