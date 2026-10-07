"""เชื่อมต่อ AI ผ่าน API ภายนอกที่เป็นรูปแบบ OpenAI (POST {base}/chat/completions) เช่น เกตเวย์ AI, OpenRouter, DeepSeek, Ollama

ตั้งค่า:
- CUSTOM_AI_API_KEY (Railway Variables) = API key · เก็บเป็นตัวแปรสภาพแวดล้อมเท่านั้น ไม่ลงฐานข้อมูล ไม่แสดงบนเว็บ ไม่อยู่ในข้อความ error
- Base URL / รายชื่อโมเดล / โหมด JSON ตั้งที่หน้า "เชื่อมต่อ AI" (หรือ CUSTOM_AI_BASE_URL เป็นค่าตั้งต้น)
- ชื่อโมเดลในระบบขึ้นต้นด้วย "oai:" เช่น oai:deepseek-v4.1-flash (ตัดคำนำหน้าออกก่อนส่งให้ API)

ความแตกต่างของแต่ละเจ้าที่จัดการให้:
- การบังคับให้ตอบเป็น JSON: ลอง json_schema -> json_object -> ขอใน prompt แล้วดึง JSON จากข้อความ (จำโหมดที่ใช้ได้ไว้)
- รูปภาพ: ส่งแบบ image_url (data URI) ถ้าโมเดลไม่รับ จะส่งซ้ำเฉพาะข้อความ
- โมเดลที่คิดก่อนตอบ: ตัดส่วน <think>...</think> ออก
- ล่มชั่วคราว/โควตา (429, 5xx, timeout): ลองซ้ำสูงสุด 3 ครั้ง
- ไม่ตาม redirect เพื่อกัน API key รั่วไปยังโดเมนอื่น
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

log = logging.getLogger("app.custom_ai")

PREFIX = "oai:"
JSON_MODES = ("schema", "object", "prompt")
MODEL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@\-]{0,99}$")
MAX_ATTEMPTS = 3
RETRY_DELAYS = (1.0, 3.0)
# รูปจากเทสต์ความสามารถ: PNG 2x2 สีแดง
TINY_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP4z8DwHwyBNBgAAEuJC/3Dj72bAAAAAElFTkSuQmCC")


class ProviderError(Exception):
    """ข้อผิดพลาดจากผู้ให้บริการ (ข้อความอ่านได้ ไม่มี API key)"""

    def __init__(self, message: str, status: int = 0, retryable: bool = False, quota: bool = False,
                 bad_key: bool = False, key_blocked: bool = False, format_rejected: bool = False,
                 image_rejected: bool = False):
        super().__init__(message)
        self.status = status
        self.retryable = retryable
        self.quota = quota
        self.bad_key = bad_key
        self.key_blocked = key_blocked
        self.format_rejected = format_rejected  # ผู้ให้บริการไม่รับ response_format แบบที่ส่ง -> ลองแบบอื่น
        self.image_rejected = image_rejected    # โมเดลไม่รับรูป -> ส่งซ้ำเฉพาะข้อความ


@dataclass
class Result:
    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    json_mode: str = ""      # โหมด JSON ที่ใช้ได้จริง (schema/object/prompt) ว่าง = ไม่ได้ขอ JSON
    vision: bool = False     # ส่งรูปให้โมเดลจริงหรือไม่
    ms: int = 0


# ความสามารถที่ค้นพบระหว่างใช้งาน (จำไว้ในหน่วยความจำ ไม่ต้องลองผิดซ้ำทุกครั้ง)
_json_cache: dict[tuple[str, str], str] = {}
_vision_cache: dict[tuple[str, str], bool] = {}


def clear_caches() -> None:
    _json_cache.clear()
    _vision_cache.clear()


# ---------------------------------------------------------------- ตั้งค่า
def api_key() -> str:
    return (os.getenv("CUSTOM_AI_API_KEY") or "").strip()


def mask_key(key: str) -> str:
    return f"…{key[-4:]}" if len(key) > 8 else "…"


def is_custom(model: str) -> bool:
    return (model or "").startswith(PREFIX)


def model_id(model: str) -> str:
    return (model or "")[len(PREFIX):] if is_custom(model) else (model or "")


def normalize_base_url(url: str) -> str:
    """ตัดช่องว่าง / ท้าย URL ที่คนมักวางเกินมา (/chat/completions, /models) ให้เหลือ base URL"""
    url = (url or "").strip()
    url = re.sub(r"/(chat/completions|completions|models)/?$", "", url, flags=re.I)
    return url.rstrip("/")


def check_base_url(url: str) -> str:
    """ตรวจ Base URL คืนข้อความผิดพลาด (ว่าง = ใช้ได้) · ต้องเป็น https ยกเว้นเครื่องตัวเอง/เครือข่ายภายใน Railway"""
    url = normalize_base_url(url)
    if not url:
        return "ยังไม่ได้ใส่ Base URL"
    if len(url) > 300:
        return "Base URL ยาวเกินไป"
    try:
        p = urlparse(url)
    except ValueError:
        return "Base URL รูปแบบไม่ถูกต้อง"
    host = (p.hostname or "").lower()
    if p.scheme not in ("http", "https") or not host:
        return "Base URL ต้องขึ้นต้นด้วย https:// เช่น https://api.example.com/v1"
    if p.username or p.password:
        return "ห้ามใส่ชื่อผู้ใช้/รหัสผ่านใน URL (ใส่ API key ที่ตัวแปร CUSTOM_AI_API_KEY)"
    local = host in ("localhost", "127.0.0.1", "::1") or host.endswith(".railway.internal")
    if p.scheme == "http" and not local:
        return "ต้องใช้ https:// เพื่อไม่ให้ API key และข้อมูลลูกค้าถูกส่งแบบไม่เข้ารหัส"
    return ""


def base_url(settings: dict) -> str:
    url = normalize_base_url(settings.get("custom_ai_base_url") or os.getenv("CUSTOM_AI_BASE_URL") or "")
    return "" if check_base_url(url) else url


def parse_models(text: str) -> dict[str, str]:
    """รายชื่อโมเดลจากข้อความ (บรรทัดละ 1 รุ่น หรือคั่นด้วยจุลภาค · "id | ชื่อที่แสดง") -> {"oai:id": ชื่อ}"""
    result = {}
    text = text or ""
    # บรรทัดใหม่คือตัวแยกหลัก (ชื่อที่แสดงมีจุลภาคได้) · ถ้าอยู่บรรทัดเดียวให้คั่นด้วยจุลภาค
    for raw in (text.splitlines() if "\n" in text else text.split(",")):
        mid, _, label = raw.partition("|")
        mid, label = mid.strip(), label.strip()
        if mid and MODEL_ID_RE.match(mid) and PREFIX + mid not in result:
            result[PREFIX + mid] = label or mid
    return result


def models(settings: dict) -> dict[str, str]:
    name = (settings.get("custom_ai_name") or "").strip() or "AI ภายนอก"
    return {m: f"{name} · {label}" for m, label in parse_models(settings.get("custom_ai_models", "")).items()}


def ready(settings: dict) -> bool:
    return bool(api_key() and base_url(settings))


def caps(settings: dict, model: str) -> dict:
    """ผลทดสอบความสามารถของโมเดลที่เคยบันทึกไว้ {"json": "schema|object|prompt", "vision": bool, "tested_at": ...}"""
    try:
        data = json.loads(settings.get("custom_ai_caps") or "{}")
    except ValueError:
        return {}
    value = data.get(model_id(model)) if isinstance(data, dict) else None
    return value if isinstance(value, dict) else {}


def _int_setting(settings: dict, key: str, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(high, int(settings.get(key) or default)))
    except (TypeError, ValueError):
        return default


def _mask(text: str) -> str:
    """ลบ API key ออกจากข้อความ (เผื่อผู้ให้บริการสะท้อนคีย์กลับมาในข้อความ error)"""
    key = api_key()
    text = str(text or "")
    if key:
        text = text.replace(key, "***")
    return re.sub(r"(sk-[A-Za-z0-9_\-]{6})[A-Za-z0-9_\-]{8,}", r"\1***", text)


# ---------------------------------------------------------------- สร้างคำขอ
def _schema_prompt(schema: dict) -> str:
    return ("\n\nตอบเป็น JSON object เพียงชิ้นเดียวตาม JSON Schema ด้านล่างนี้ ห้ามมีข้อความอื่นนอกจาก JSON "
            "และห้ามใส่เครื่องหมาย ``` ครอบ:\n" + json.dumps(schema, ensure_ascii=False))


def _build_messages(system: str, parts: list[tuple], with_images: bool, schema_hint: str) -> list[dict]:
    texts: list[str] = []
    content: list[dict] = []
    images = 0
    for p in parts:
        if p[0] == "text":
            texts.append(p[1])
            content.append({"type": "text", "text": p[1]})
        else:
            images += 1
            content.append({"type": "image_url", "image_url": {
                "url": f"data:{p[2]};base64,{base64.standard_b64encode(p[1]).decode()}"}})
    if with_images and images:
        user: object = content
    else:
        note = (f"\n\n(มีรูปภาพแนบมา {images} รูป แต่โมเดลนี้ไม่รับรูป จึงส่งให้เฉพาะข้อความ)" if images else "")
        user = "\n\n".join(texts) + note
    return [{"role": "system", "content": system + schema_hint}, {"role": "user", "content": user}]


def _response_format(mode: str, schema: dict) -> dict | None:
    if mode == "schema":
        return {"type": "json_schema", "json_schema": {"name": "result", "strict": True, "schema": schema}}
    if mode == "object":
        return {"type": "json_object"}
    return None


# ---------------------------------------------------------------- เรียก API
_FORMAT_HINT = re.compile(r"response_format|json_schema|json schema|json_object|structured|schema|format", re.I)
_IMAGE_HINT = re.compile(r"image|vision|multimodal|multi-modal|modalit|content", re.I)


def _error_text(resp: httpx.Response) -> str:
    try:
        data = resp.json()
    except ValueError:
        return _mask(resp.text[:300])
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            msg = err.get("message") or err.get("code") or err.get("type")
        else:
            msg = err or data.get("message") or data.get("detail")
        if msg:
            return _mask(str(msg)[:300])
    return _mask(resp.text[:300])


def _to_error(resp: httpx.Response) -> ProviderError:
    status, detail = resp.status_code, _error_text(resp)
    if 300 <= status < 400:
        return ProviderError(f"Base URL ถูกเปลี่ยนเส้นทาง ({status}) ระบบไม่ตามไปเพื่อความปลอดภัยของ API key · แก้ Base URL ให้ตรง ({detail})", status)
    if status in (401, 403):
        return ProviderError(f"API key ไม่ถูกต้องหรือไม่มีสิทธิ์ ({status}): {detail}", status, bad_key=True)
    if status == 402:
        return ProviderError(f"เครดิตของผู้ให้บริการหมด (402): {detail}", status, key_blocked=True)
    if status == 404:
        return ProviderError(f"ไม่พบ endpoint หรือโมเดล (404): {detail} · ตรวจ Base URL และชื่อโมเดล", status)
    if status == 429:
        return ProviderError(f"เรียกบ่อยเกินไปหรือโควตาหมด (429): {detail}", status, retryable=True, quota=True)
    if status in (408, 409) or status >= 500:
        return ProviderError(f"ผู้ให้บริการขัดข้องชั่วคราว ({status}): {detail}", status, retryable=True)
    return ProviderError(f"ผู้ให้บริการตอบกลับผิดพลาด ({status}): {detail}", status,
                         format_rejected=status in (400, 422) and bool(_FORMAT_HINT.search(detail)),
                         image_rejected=status in (400, 422) and bool(_IMAGE_HINT.search(detail)))


async def _post(url: str, payload: dict, key: str, timeout: float) -> dict:
    """POST พร้อมลองซ้ำเมื่อล่มชั่วคราว (ไม่ตาม redirect)"""
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    last: ProviderError | None = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=15), follow_redirects=False) as client:
                resp = await client.post(url, json=payload, headers=headers)
        except httpx.TimeoutException:
            last = ProviderError(f"ผู้ให้บริการตอบช้าเกิน {int(timeout)} วินาที", 0, retryable=True)
        except httpx.HTTPError as e:
            last = ProviderError(f"เชื่อมต่อผู้ให้บริการไม่ได้: {_mask(type(e).__name__ + ' ' + str(e))[:200]}", 0, retryable=True)
        else:
            if resp.status_code < 300:
                try:
                    data = resp.json()
                except ValueError as e:
                    raise ProviderError("ผู้ให้บริการตอบกลับไม่ใช่ JSON (ตรวจ Base URL ให้ลงท้ายด้วย /v1 ตามเอกสารของผู้ให้บริการ)") from e
                if not isinstance(data, dict):
                    raise ProviderError("ผู้ให้บริการตอบกลับรูปแบบที่ไม่รู้จัก")
                return data
            last = _to_error(resp)
            if not last.retryable:
                raise last
        if attempt < MAX_ATTEMPTS - 1:
            await asyncio.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
    assert last is not None
    raise last


_THINK = re.compile(r"<think(?:ing)?>.*?</think(?:ing)?>", re.DOTALL | re.IGNORECASE)


def _extract_text(data: dict) -> str:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ProviderError("ผู้ให้บริการตอบกลับไม่มีข้อความ (ไม่พบ choices)")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, list):
        content = "".join(str(p.get("text", "")) for p in content if isinstance(p, dict))
    text = _THINK.sub("", content or "")
    text = re.sub(r"^\s*</?think(?:ing)?>", "", text, flags=re.IGNORECASE).strip()
    if not text:
        if choices[0].get("finish_reason") == "length":
            raise ProviderError("AI ตอบไม่ครบ (ข้อความถูกตัดเพราะยาวเกินกำหนด ลองตั้ง max tokens ให้มากขึ้น)")
        raise ProviderError("AI ไม่ได้ตอบข้อความกลับมา")
    return text


def extract_json(text: str) -> str:
    """ดึง JSON object จากคำตอบ (รองรับที่ครอบด้วย ``` หรือมีข้อความนำหน้า/ตามหลัง) คืนค่าเป็นข้อความ JSON"""
    t = (text or "").strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.IGNORECASE).strip()
    candidates = [t]
    i, j = t.find("{"), t.rfind("}")
    if 0 <= i < j:
        candidates.append(t[i:j + 1])
    for c in candidates:
        try:
            if isinstance(json.loads(c), dict):
                return c
        except ValueError:
            continue
    raise ProviderError("AI ตอบกลับไม่ใช่ JSON ที่อ่านได้")


async def chat(settings: dict, model: str, system: str, parts: list[tuple], schema: dict | None = None, *,
               mode: str = "", vision: bool | None = None, timeout: float | None = None) -> Result:
    """เรียกโมเดลหนึ่งครั้ง · schema = ต้องการคำตอบเป็น JSON ตาม schema · mode/vision = บังคับ (ใช้ตอนทดสอบ)"""
    key, base = api_key(), base_url(settings)
    if not key:
        raise ProviderError("ยังไม่ได้ตั้ง CUSTOM_AI_API_KEY ใน Railway Variables", bad_key=True)
    if not base:
        raise ProviderError("ยังไม่ได้ตั้ง Base URL ที่ถูกต้อง (หน้า เชื่อมต่อ AI)")
    mid = model_id(model)
    if not mid:
        raise ProviderError("ยังไม่ได้เลือกโมเดล")
    timeout = timeout or _int_setting(settings, "custom_ai_timeout", 120, 10, 600)
    max_tokens = _int_setting(settings, "custom_ai_max_tokens", 0, 0, 200000)
    saved = caps(settings, model)
    cache_key = (base, mid)

    # ลำดับโหมด JSON ที่จะลอง: ที่บังคับ > ที่ตั้งไว้ > ที่เคยรู้ว่าใช้ได้ > ลองไล่ schema, object, prompt
    if not schema:
        chain = [""]
    elif mode:
        chain = [mode]
    else:
        configured = (settings.get("custom_ai_json_mode") or "auto")
        if configured in JSON_MODES:
            chain = [configured]
        else:
            known = _json_cache.get(cache_key) or saved.get("json")
            chain = [known] + [m for m in JSON_MODES if m != known] if known in JSON_MODES else list(JSON_MODES)

    images_present = any(p[0] == "image" for p in parts)
    vision_setting = (settings.get("custom_ai_vision") or "auto")
    if vision is not None:
        use_images = vision
    elif vision_setting == "no":
        use_images = False
    elif vision_setting == "yes":
        use_images = True
    else:
        use_images = _vision_cache.get(cache_key, saved.get("vision", True))
    started = time.monotonic()
    last_error: ProviderError | None = None
    for json_mode in chain:
        images_now = use_images and images_present
        while True:
            payload: dict = {"model": mid, "messages": _build_messages(
                system, parts, images_now, _schema_prompt(schema) if json_mode in ("object", "prompt") and schema else "")}
            rf = _response_format(json_mode, schema) if schema else None
            if rf:
                payload["response_format"] = rf
            if max_tokens:
                payload["max_tokens"] = max_tokens
            try:
                data = await _post(f"{base}/chat/completions", payload, key, float(timeout))
                text = _extract_text(data)
                if schema:
                    text = extract_json(text)
            except ProviderError as e:
                if images_now and e.image_rejected and not e.format_rejected:
                    _vision_cache[cache_key] = False  # โมเดลไม่รับรูป: ส่งซ้ำเฉพาะข้อความ แล้วจำไว้
                    images_now = use_images = False
                    continue
                if json_mode and schema and (e.format_rejected or e.status in (400, 422) or "ไม่ใช่ JSON" in str(e)):
                    last_error = e  # โหมดนี้ใช้ไม่ได้กับผู้ให้บริการนี้ -> ลองโหมดถัดไป
                    break
                raise
            if images_present and images_now:
                _vision_cache[cache_key] = True
            if json_mode:
                _json_cache[cache_key] = json_mode
            usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
            return Result(text=text, input_tokens=int(usage.get("prompt_tokens") or 0),
                          output_tokens=int(usage.get("completion_tokens") or 0), json_mode=json_mode,
                          vision=bool(images_present and images_now), ms=int((time.monotonic() - started) * 1000))
    raise last_error or ProviderError("เรียก AI ไม่สำเร็จ")


# ---------------------------------------------------------------- รายชื่อโมเดล / ทดสอบความสามารถ
async def list_models(settings: dict, base: str = "") -> list[str]:
    """ดึงรายชื่อโมเดลจาก GET {base}/models (รูปแบบ OpenAI)"""
    key = api_key()
    base = normalize_base_url(base) or base_url(settings)
    if not key:
        raise ProviderError("ยังไม่ได้ตั้ง CUSTOM_AI_API_KEY ใน Railway Variables", bad_key=True)
    if (err := check_base_url(base)):
        raise ProviderError(err)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30, connect=15), follow_redirects=False) as client:
            resp = await client.get(f"{base}/models", headers={"Authorization": f"Bearer {key}"})
    except httpx.HTTPError as e:
        raise ProviderError(f"เชื่อมต่อผู้ให้บริการไม่ได้: {_mask(type(e).__name__)}") from e
    if resp.status_code >= 300:
        raise _to_error(resp)
    try:
        data = resp.json()
    except ValueError as e:
        raise ProviderError("ผู้ให้บริการตอบกลับไม่ใช่ JSON (ตรวจ Base URL)") from e
    items = data.get("data") if isinstance(data, dict) else data
    ids = sorted({str(i.get("id")) for i in (items or []) if isinstance(i, dict) and i.get("id")})
    return [i for i in ids if MODEL_ID_RE.match(i)]


_PROBE_SCHEMA = {"type": "object", "properties": {"ok": {"type": "boolean"}, "word": {"type": "string"}},
                 "required": ["ok", "word"], "additionalProperties": False}


async def probe(settings: dict, model: str, base: str = "") -> dict:
    """ทดสอบโมเดลจริง: ข้อความ / JSON 3 โหมด / รูปภาพ -> {"checks": [...], "caps": {...}, "tokens": n}
    (ใช้โทเคนไม่กี่ร้อยตัว) caps ใช้บันทึกเป็นค่าแนะนำให้ระบบเลือกโหมดถูกตั้งแต่ครั้งแรก"""
    settings = dict(settings)
    if base:
        settings["custom_ai_base_url"] = base
    checks, tokens = [], 0

    async def run(name: str, **kw):
        nonlocal tokens
        started = time.monotonic()
        try:
            res = await chat(settings, model, "คุณคือผู้ช่วยทดสอบระบบ ตอบสั้นที่สุด", kw.pop("parts"), kw.pop("schema", None),
                             timeout=60, **kw)
            tokens += res.input_tokens + res.output_tokens
            checks.append({"name": name, "ok": True, "ms": res.ms, "detail": res.text[:80]})
            return res
        except ProviderError as e:
            checks.append({"name": name, "ok": False, "ms": int((time.monotonic() - started) * 1000), "detail": str(e)[:240]})
            return None

    text = await run("ข้อความทั่วไป", parts=[("text", "ตอบคำว่า OK คำเดียว")])
    found = {}
    if text:
        for m, label in (("schema", "JSON แบบ schema (แนะนำ)"), ("object", "JSON แบบ json_object"), ("prompt", "JSON จากการขอใน prompt")):
            res = await run(label, parts=[("text", 'ตอบเป็น JSON: ok เป็น true และ word เป็นคำว่า "hi"')],
                            schema=_PROBE_SCHEMA, mode=m)
            if res:
                try:
                    good = json.loads(res.text).get("ok") is True
                except ValueError:
                    good = False
                if good:
                    found.setdefault("json", m)
        vis = await run("อ่านรูปภาพ", parts=[("text", "รูปนี้สีอะไร ตอบสั้นๆ"), ("image", TINY_PNG, "image/png")], vision=True)
        found["vision"] = bool(vis and vis.vision)
        if vis and not vis.vision:  # chat() ถอยไปส่งข้อความล้วนเมื่อโมเดลปฏิเสธรูป -> ไม่นับว่าอ่านรูปได้
            checks[-1].update(ok=False, detail="โมเดลไม่รับรูปภาพ · ระบบจะส่งเฉพาะข้อความให้โมเดลนี้")
    return {"checks": checks, "caps": found, "tokens": tokens, "ok": bool(text),
            "tested_at": time.strftime("%Y-%m-%d %H:%M", time.gmtime())}
