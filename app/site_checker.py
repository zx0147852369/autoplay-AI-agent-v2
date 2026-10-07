"""ตรวจสอบว่าเว็บไซต์ที่ลูกค้าแจ้งเข้าได้หรือไม่ (สถานะ HTTP + เวลาตอบสนอง)"""

import asyncio
import ipaddress
import socket
import time
from urllib.parse import urlparse

import httpx

from .database import utcnow


def _normalize(url: str) -> str:
    url = url.strip()
    if url and "://" not in url:
        url = "https://" + url
    return url


async def _is_public_host(host: str) -> bool:
    """กันไม่ให้ระบบถูกใช้ยิงเข้าเครือข่ายภายใน (SSRF)"""
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None)
    except socket.gaierror:
        return True  # ให้ httpx รายงานว่า DNS หาไม่เจอ
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            return False
    return True


def _base_host(host: str) -> str:
    host = (host or "").lower().rstrip(".")
    return host[4:] if host.startswith("www.") else host


def same_site(a: str, b: str) -> bool:
    """โดเมนเดียวกัน (นับ www. และ subdomain เป็นเว็บเดียวกัน)"""
    a, b = _base_host(a), _base_host(b)
    return a == b or a.endswith("." + b) or b.endswith("." + a)


async def check_site(url: str) -> dict:
    url = _normalize(url)
    result = {"url": url, "checked_at": utcnow().isoformat(), "ok": False,
              "status_code": None, "elapsed_ms": None, "error": ""}
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=False,
                                     headers={"User-Agent": "Mozilla/5.0 (support-site-check)"}) as client:
            # ตาม redirect เองทีละขั้น เพื่อตรวจทุกปลายทางว่าไม่ใช่ที่อยู่ภายใน
            for _ in range(6):
                parsed = urlparse(url)
                if parsed.scheme not in ("http", "https") or not parsed.hostname:
                    result["error"] = "ลิงก์ไม่ถูกต้อง"
                    return result
                if not await _is_public_host(parsed.hostname):
                    result["error"] = "ไม่อนุญาตให้ตรวจที่อยู่ภายในเครือข่าย"
                    return result
                response = await client.get(url)
                if not response.is_redirect:
                    break
                url = str(response.url.join(response.headers["location"]))
        result["status_code"] = response.status_code
        result["final_url"] = url
        # ถูกพาไปโดเมนอื่น (เช่น หน้าขายโดเมนหมดอายุ) -> เข้าได้แต่ไม่ใช่เว็บของลูกค้า
        start_host, final_host = urlparse(result["url"]).hostname, urlparse(url).hostname
        result["other_host"] = bool(start_host and final_host and not same_site(start_host, final_host))
        result["final_host"] = final_host or ""
        result["ok"] = response.status_code < 400
        if not result["ok"]:
            result["error"] = f"HTTP {response.status_code}"
    except httpx.TimeoutException:
        result["error"] = "หมดเวลา (timeout 15 วินาที)"
    except httpx.ConnectError as e:
        result["error"] = f"เชื่อมต่อไม่ได้: {e}"
    except httpx.HTTPError as e:
        result["error"] = f"ผิดพลาด: {e}"
    except (httpx.InvalidURL, ValueError, UnicodeError):
        result["error"] = "ลิงก์ไม่ถูกต้อง"
    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
    return result
