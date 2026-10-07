"""อ่านทรัพยากรเครื่อง/คอนเทนเนอร์ที่ระบบรันอยู่ (CPU, RAM, ดิสก์, เวลาทำงาน)

บน Railway คอนเทนเนอร์ใช้ cgroup v2 จึงอ่านเพดาน CPU/RAM จาก /sys/fs/cgroup ก่อน
ถ้าไม่มี (เช่นรันบน Windows ตอนพัฒนา) จะ fallback ไปใช้ค่าของทั้งเครื่องแทน
"""

import os
import platform
import socket
import time
from pathlib import Path

try:
    import psutil
except Exception:  # noqa: BLE001 - ไม่มี psutil ก็ยังเปิดหน้าได้ (แสดงข้อมูลเท่าที่ได้)
    psutil = None

from .config import DATA_DIR, DATABASE_URL

_proc = psutil.Process() if psutil else None
_START = time.time()
# seed การวัด CPU% (ครั้งแรกจะได้ 0 แล้วค่อยมีค่าจริงในครั้งถัดไป)
if psutil:
    try:
        psutil.cpu_percent(interval=None)
        _proc.cpu_percent(interval=None)
    except Exception:  # noqa: BLE001
        pass


def _read(path: str) -> str:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return ""


def _cgroup_cpu_quota() -> float | None:
    """จำนวนคอร์ที่คอนเทนเนอร์ใช้ได้ (cgroup v2) · None = ไม่จำกัด/อ่านไม่ได้"""
    val = _read("/sys/fs/cgroup/cpu.max")  # "quota period" หรือ "max period"
    if val:
        parts = val.split()
        if len(parts) == 2 and parts[0] != "max":
            try:
                return int(parts[0]) / int(parts[1])
            except (ValueError, ZeroDivisionError):
                return None
    return None


def _cgroup_mem() -> tuple[int | None, int | None]:
    """(ram ที่ใช้, เพดาน ram) ของคอนเทนเนอร์ เป็น bytes · None = อ่านไม่ได้"""
    cur = _read("/sys/fs/cgroup/memory.current")
    mx = _read("/sys/fs/cgroup/memory.max")
    used = int(cur) if cur.isdigit() else None
    limit = int(mx) if mx.isdigit() else None
    return used, limit


def _fmt_bytes(n: float | None) -> str:
    if n is None:
        return "-"
    mb = n / (1024 * 1024)
    return f"{mb / 1024:.2f} GB" if mb >= 1024 else f"{mb:.0f} MB"


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins = rem // 60
    if days:
        return f"{days} วัน {hours} ชม."
    if hours:
        return f"{hours} ชม. {mins} นาที"
    return f"{mins} นาที"


def _cpu_model() -> str:
    for line in _read("/proc/cpuinfo").splitlines():
        if line.lower().startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "-"


def _level(pct: float) -> str:
    return "bad" if pct >= 90 else "warn" if pct >= 75 else "ok"


def snapshot() -> dict:
    cores = os.cpu_count() or 1
    quota = _cgroup_cpu_quota()

    # ---- CPU ----
    proc_cpu = host_cpu = 0.0
    load1 = None
    if psutil:
        try:
            proc_cpu = round(_proc.cpu_percent(interval=None), 1)  # % เทียบ 1 คอร์ (เกิน 100 ได้ถ้าใช้หลายคอร์)
            host_cpu = round(psutil.cpu_percent(interval=None), 1)
        except Exception:  # noqa: BLE001
            pass
    try:
        load1 = round(os.getloadavg()[0], 2)  # มีเฉพาะ Linux/Unix
    except (OSError, AttributeError):
        load1 = None

    # ---- RAM ----
    mem_used, mem_limit = _cgroup_mem()
    if mem_used is None and psutil:
        vm = psutil.virtual_memory()
        mem_used, mem_limit = vm.used, vm.total
    proc_rss = None
    if psutil:
        try:
            proc_rss = _proc.memory_info().rss
        except Exception:  # noqa: BLE001
            pass
    mem_pct = round(mem_used / mem_limit * 100) if mem_used and mem_limit else 0

    # ---- ดิสก์ (โฟลเดอร์เก็บข้อมูล / Volume) ----
    disk_used = disk_total = disk_free = None
    disk_pct = 0
    try:
        import shutil
        du = shutil.disk_usage(str(DATA_DIR))
        disk_total, disk_free = du.total, du.free
        disk_used = du.total - du.free
        disk_pct = round(disk_used / du.total * 100) if du.total else 0
    except OSError:
        pass

    # ---- เวลาทำงาน ----
    app_uptime = time.time() - _START
    machine_uptime = None
    if psutil:
        try:
            machine_uptime = time.time() - psutil.boot_time()
        except Exception:  # noqa: BLE001
            pass

    # ---- ขนาดฐานข้อมูล ----
    db_path = DATABASE_URL.replace("sqlite:///", "") if DATABASE_URL.startswith("sqlite") else ""
    db_size = os.path.getsize(db_path) if db_path and os.path.exists(db_path) else None

    return {
        "cpu": {
            "proc": proc_cpu, "host": host_cpu, "host_level": _level(host_cpu),
            "cores": cores, "quota": quota, "load1": load1,
            "proc_level": _level(min(proc_cpu, 100)),
        },
        "ram": {
            "used": mem_used, "limit": mem_limit, "pct": mem_pct, "level": _level(mem_pct),
            "used_str": _fmt_bytes(mem_used), "limit_str": _fmt_bytes(mem_limit),
            "proc_rss": proc_rss, "proc_rss_str": _fmt_bytes(proc_rss),
        },
        "disk": {
            "used": disk_used, "total": disk_total, "free": disk_free, "pct": disk_pct, "level": _level(disk_pct),
            "used_str": _fmt_bytes(disk_used), "total_str": _fmt_bytes(disk_total), "free_str": _fmt_bytes(disk_free),
        },
        "uptime": {
            "app": _fmt_duration(app_uptime),
            "machine": _fmt_duration(machine_uptime) if machine_uptime else None,
        },
        "machine": {
            "host": socket.gethostname(),
            "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
            "cpu_model": _cpu_model(),
            "quota_str": f"{quota:.2f} คอร์" if quota else f"{cores} คอร์",
            "ram_limit_str": _fmt_bytes(mem_limit),
            "python": platform.python_version(),
            "service": os.getenv("RAILWAY_SERVICE_NAME", ""),
            "env": os.getenv("RAILWAY_ENVIRONMENT_NAME", ""),
            "region": os.getenv("RAILWAY_REPLICA_REGION") or os.getenv("RAILWAY_REGION", ""),
            "commit": (os.getenv("RAILWAY_GIT_COMMIT_SHA", "") or "")[:7],
            "db_path": db_path or "-",
            "db_size_str": _fmt_bytes(db_size),
        },
    }
