import uvicorn

from app.config import HOST, ON_CLOUD, PORT

if __name__ == "__main__":
    # บน Railway มี proxy อยู่ข้างหน้า ต้องอ่าน IP จริงจาก X-Forwarded-For (ใช้จำกัดการเดารหัสผ่าน)
    uvicorn.run("app.main:app", host=HOST, port=PORT, proxy_headers=True,
                forwarded_allow_ips="*" if ON_CLOUD else "127.0.0.1")
