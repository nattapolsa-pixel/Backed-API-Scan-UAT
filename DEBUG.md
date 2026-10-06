# ระบบเก็บ Debug สำหรับ UAT

เว็บ UAT จะเก็บข้อผิดพลาดฝั่งเบราว์เซอร์และเชื่อม API ที่มีปัญหากับ Render logs ได้ ระบบนี้เก็บข้อมูลเพื่อวิเคราะห์เท่านั้น ยังไม่แก้ source code หรือ deploy วิธีแก้อัตโนมัติ

## ระบบเก็บอะไรบ้าง

- JavaScript error ที่ไม่ถูกจัดการ และ Promise rejection ที่ไม่มี handler
- การโหลดไฟล์ JavaScript หรือ stylesheet ไม่สำเร็จ
- API ที่ตอบ HTTP 422, 429 หรือ 5xx รวมถึง network error จาก `fetchWithTimeout()`
- Exception ที่หลุดจาก API และคำตอบ 5xx ใน Render logs

ผลลัพธ์ที่คาดไว้ เช่น ค้นหาไม่พบ (404), ข้อมูลชนกัน (409), สถานะพักสแกน (423) และ health check จะไม่ถูกรายงานเป็น API error จากเบราว์เซอร์

## วิธีเปิดดู

- รายงานจากเบราว์เซอร์จะต่อแถวในแท็บ `Debug Events` ของ UAT database spreadsheet ที่กำหนดด้วย `UAT_DATABASE_SPREADSHEET_ID`
- ข้อผิดพลาดฝั่ง API จะอยู่ใน Render logs เป็น JSON event ชื่อ `api_unhandled_exception` หรือ `api_http_error`
- API ที่หน้าเว็บส่งผ่าน `fetchWithTimeout()` จะมี request ID สำหรับติดตาม แม้กรณีไม่มี response กลับมา รายงานมี `Request_ID` ให้ใช้ค้นหา ID เดียวกันใน Render logs ส่วน `Ingest_Request_ID` คือ ID ของคำขอที่บันทึกรายงาน

API จะสร้างแท็บ `Debug Events` และหัวคอลัมน์พร้อมกับแท็บ event อื่นเมื่อเริ่มทำงาน การบันทึกรายงานต้องใช้ Google Sheets service account ที่ตั้งไว้ใน API

## ข้อจำกัดและข้อมูลที่ส่ง

- เมื่อออฟไลน์ เว็บเก็บรายงานใน `localStorage` แล้วส่งใหม่เมื่อกลับมาออนไลน์ เก็บได้สูงสุด 40 รายการ ส่งครั้งละไม่เกิน 3 รายการ กัน error ซ้ำภายใน 1 นาที และรับสูงสุด 20 รายการต่อการเปิดหน้าเว็บหนึ่งครั้ง
- API จำกัดรายงานจาก client address เดียวกันไว้ 30 รายการต่อนาที และจำกัดความยาวของแต่ละฟิลด์
- รายงานเก็บ path โดยตัด query string, HTTP status, เวอร์ชันเว็บ/API, user-agent, ข้อความ error และ stack trace แบบจำกัดความยาว โดยไม่ส่ง request body ระบบกรอง bearer token, email และเลขอ้างอิงยาวทั่วไปออกแล้ว ควรตรวจข้อความก่อนแชร์ออกนอก UAT
- Request ID ถูกเพิ่มเป็น query parameter สำหรับติดตาม แต่ API จะไม่บันทึก query string ลง log หรือชีต
- หาก Google Sheets บันทึกไม่ได้ API จะตอบ error เพื่อให้เว็บเก็บรายงานไว้ส่งใหม่ และเขียน `debug_event_persist_failed` ลง Render logs

## ขอบเขต Auto-fix

งานชุดนี้ทำให้เห็นบริบทของปัญหาได้ครบขึ้น แต่ยังไม่ให้เว็บแก้ source code เอง การซ่อมโค้ดอัตโนมัติต้องมี agent runner แยก พร้อมสิทธิ์เข้าถึง repository ขั้นตรวจสอบ และนโยบาย deploy เฉพาะ UAT

เมื่อนำขึ้นใช้งาน ให้ deploy API ก่อนเว็บ หากเว็บส่งรายงานก่อนที่ endpoint ใหม่จะพร้อม รายงานจะค้างในคิวของเบราว์เซอร์และลองส่งใหม่ภายหลัง
