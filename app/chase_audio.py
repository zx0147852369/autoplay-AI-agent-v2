"""ไฟล์เสียงที่เล่นให้แอดมินฟังเมื่อรับสายโทรตามอนุมัติ

ไฟล์ที่อัปโหลดถูกแปลงครั้งเดียวตอนอัปโหลดเป็น WAV โมโน 48 kHz 16 บิต (รูปแบบที่ส่งเข้าสายได้เลย) เก็บใน DATA_DIR/chase_audio
  - WAV ปกติ (8/16/24/32 บิต) อ่านได้เองโดยไม่ต้องพึ่งไลบรารีเสริม
  - MP3 / M4A / OGG / FLAC ฯลฯ อ่านด้วย PyAV (ถ้าติดตั้งไว้) ไม่มีก็แจ้งให้ใช้ WAV
ไม่มีไฟล์ที่เลือกไว้ -> ใช้เสียงเตือนมาตรฐานที่ระบบสร้างเอง (เสียงปี๊บ 3 ชุด)
"""
import importlib.util
import io
import math
import struct
import sys
import wave
from array import array
from pathlib import Path

from .config import DATA_DIR

AUDIO_DIR = DATA_DIR / "chase_audio"
AUDIO_DIR.mkdir(parents=True, exist_ok=True)

RATE = 48000
MAX_SECONDS = 60
MIN_SECONDS = 0.3
MAX_UPLOAD = 40 * 1024 * 1024
MAX_SOUNDS = 10


class AudioError(Exception):
    """ไฟล์เสียงใช้ไม่ได้ (ข้อความเป็นภาษาไทยพร้อมแสดงให้แอดมินอ่าน)"""


def decoder_ready() -> bool:
    """อ่านไฟล์นอกจาก WAV (MP3/M4A/OGG ...) ได้ไหม"""
    return importlib.util.find_spec("av") is not None


def engine_ready() -> bool:
    """มีเครื่องมือโทรออกพร้อมส่งเสียง (py-tgcalls + ntgcalls) ไหม"""
    return importlib.util.find_spec("pytgcalls") is not None and importlib.util.find_spec("ntgcalls") is not None


# ---------------------------------------------------------------- แปลงไฟล์เป็น PCM โมโน 48 kHz
def _wav_samples(data: bytes, trim: bool) -> tuple[array, int, int, bool]:
    """อ่าน WAV แบบ PCM -> (ตัวอย่างเสียง 16 บิตสลับช่อง, จำนวนช่อง, sample rate, ถูกตัดให้สั้นลงไหม) · รูปแบบที่ wave อ่านไม่ได้ -> wave.Error"""
    cut = False
    with wave.open(io.BytesIO(data)) as w:
        ch, width, rate, n = w.getnchannels(), w.getsampwidth(), w.getframerate(), w.getnframes()
        if rate <= 0 or ch <= 0:
            raise AudioError("ไฟล์ WAV เสียหาย")
        if n / rate > MAX_SECONDS + 1:
            if not trim:
                raise AudioError(f"เสียงยาวเกิน {MAX_SECONDS} วินาที ตัดให้สั้นลงก่อนอัปโหลด")
            n, cut = int(rate * (MAX_SECONDS + 1)), True  # อ่านเฉพาะส่วนต้นที่ใช้
        raw = w.readframes(n)
    if width == 1:
        pcm = array("h", [(b - 128) << 8 for b in raw])
    elif width == 2:
        pcm = array("h")
        pcm.frombytes(raw[: len(raw) // 2 * 2])
        if sys.byteorder == "big":
            pcm.byteswap()
    elif width == 3:
        pcm = array("h", [int.from_bytes(raw[i:i + 3], "little", signed=True) >> 8 for i in range(0, len(raw) - 2, 3)])
    elif width == 4:
        pcm = array("h", [v >> 16 for v in struct.unpack(f"<{len(raw) // 4}i", raw[: len(raw) // 4 * 4])])
    else:
        raise wave.Error(f"sample width {width}")
    return pcm, ch, rate, cut


def _to_mono_48k(pcm: array, ch: int, rate: int) -> bytes:
    if ch > 1:
        n = len(pcm) // ch
        cols = [pcm[i:i + n * ch:ch] for i in range(ch)]
        pcm = array("h", [sum(v) // ch for v in zip(*cols)])
    if rate != RATE and len(pcm):
        step, last = rate / RATE, len(pcm) - 1
        out = array("h")
        for i in range(int(len(pcm) * RATE / rate)):
            pos = i * step
            j = int(pos)
            a = pcm[j]
            b = pcm[j + 1] if j < last else a
            out.append(int(a + (b - a) * (pos - j)))
        pcm = out
    if sys.byteorder == "big":
        pcm.byteswap()
    return pcm.tobytes()


def _decode_with_av(data: bytes, trim: bool) -> tuple[bytes, bool]:
    try:
        import av
    except ImportError as e:
        raise AudioError("ไฟล์นี้ไม่ใช่ WAV และระบบยังไม่มีตัวอ่าน MP3/M4A/OGG · แปลงเป็นไฟล์ WAV ก่อนแล้วอัปโหลดใหม่") from e
    limit = (MAX_SECONDS + 1) * RATE * 2
    chunks, total, cut = [], 0, False
    try:
        with av.open(io.BytesIO(data)) as container:
            stream = next((s for s in container.streams if s.type == "audio"), None)
            if stream is None:
                raise AudioError("ไม่พบเสียงในไฟล์นี้")
            resampler = av.AudioResampler(format="s16", layout="mono", rate=RATE)
            for packet in container.demux(stream):
                for frame in packet.decode():
                    for out in resampler.resample(frame):
                        b = bytes(out.planes[0])[: out.samples * 2]
                        chunks.append(b)
                        total += len(b)
                if total > limit:
                    if not trim:
                        raise AudioError(f"เสียงยาวเกิน {MAX_SECONDS} วินาที ตัดให้สั้นลงก่อนอัปโหลด")
                    cut = True  # ยาวเกินกำหนด: หยุดอ่านแค่ส่วนต้น ไม่ถอดทั้งไฟล์
                    break
            if not cut:
                for out in resampler.resample(None):
                    chunks.append(bytes(out.planes[0])[: out.samples * 2])
    except AudioError:
        raise
    except Exception as e:  # noqa: BLE001 - ไฟล์เสียหาย/รูปแบบที่ไม่รู้จัก
        raise AudioError("อ่านไฟล์เสียงนี้ไม่ได้ (ไฟล์อาจเสียหายหรือเป็นรูปแบบที่ไม่รองรับ)") from e
    return b"".join(chunks), cut


def _decode(data: bytes, trim: bool) -> tuple[bytes, bool]:
    if not data:
        raise AudioError("ไฟล์ว่างเปล่า")
    pcm, cut = None, False
    if data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        try:
            samples, ch, rate, cut = _wav_samples(data, trim)
            pcm = _to_mono_48k(samples, ch, rate)
        except (wave.Error, EOFError, struct.error):
            pcm, cut = None, False  # WAV ชนิดที่ wave อ่านไม่ได้ (เช่น float) -> ลองตัวอ่านเสริม
    if pcm is None:
        pcm, cut = _decode_with_av(data, trim)
    limit = MAX_SECONDS * RATE * 2
    if len(pcm) > limit:
        if not trim:
            raise AudioError(f"เสียงยาวเกิน {MAX_SECONDS} วินาที ตัดให้สั้นลงก่อนอัปโหลด")
        pcm, cut = pcm[:limit], True
    if len(pcm) / 2 / RATE < MIN_SECONDS:
        raise AudioError("เสียงสั้นเกินไป (ต้องยาวอย่างน้อย 0.3 วินาที)")
    return pcm, cut


def decode_pcm(data: bytes) -> bytes:
    """ไฟล์เสียงอะไรก็ได้ -> PCM 16 บิตโมโน 48 kHz (ผิดรูปแบบ/ยาวเกิน/สั้นเกิน -> AudioError) · ฟังก์ชันหนัก เรียกผ่าน thread"""
    return _decode(data, False)[0]


def decode_upload(data: bytes) -> tuple[bytes, bool]:
    """แบบเดียวกับ decode_pcm แต่ไฟล์ที่ยาวเกินกำหนดไม่ถูกปฏิเสธ · ตัดเหลือส่วนต้น MAX_SECONDS วินาที · คืน (PCM, ถูกตัดไหม)"""
    return _decode(data, True)

def save_wav(pcm: bytes, path: Path) -> None:
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(RATE)
        w.writeframes(pcm)


def load_pcm(path: Path) -> bytes:
    with wave.open(str(path), "rb") as w:
        return w.readframes(w.getnframes())


# ---------------------------------------------------------------- เสียงเตือนมาตรฐาน
def builtin_pcm() -> bytes:
    """ปี๊บ-ปี๊บ (สูง-ต่ำ) 3 ชุด ยาวประมาณ 3 วินาที · ค่อยๆ ดังเข้า/ออกกันเสียงปะทุ"""
    out = array("h")
    for _ in range(3):
        for freq, dur in ((880, 0.25), (660, 0.25)):
            n = int(RATE * dur)
            for i in range(n):
                env = min(1.0, i / 480, (n - 1 - i) / 480)
                out.append(int(16000 * env * math.sin(2 * math.pi * freq * i / RATE)))
            out.extend(array("h", [0]) * int(RATE * 0.12))
        out.extend(array("h", [0]) * int(RATE * 0.25))
    if sys.byteorder == "big":
        out.byteswap()
    return out.tobytes()


def builtin_wav() -> Path:
    path = AUDIO_DIR / "builtin.wav"
    if not path.is_file():
        save_wav(builtin_pcm(), path)
    return path
