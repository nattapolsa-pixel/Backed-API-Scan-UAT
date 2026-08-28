# Pro Scanner UAT — Google Sheets

ระบบทดสอบนี้แยกจาก Production และไม่ใช้ BigQuery เป็นแหล่งข้อมูลหน้าเอกสาร

## แหล่งข้อมูล

- ยอดกล่อง, Wave, รหัส/ชื่อสาขา, BU: `Member Data` (`1MO3lu1GssPZZvaruwQ5trUB045dzh4HUHdH35mbyOtc`)
- Booking, Wave, Carrier, ชื่อคนส่ง, ทะเบียนรถ: `Booking & Wave` (`1jOnJnnwlWZ491FEAFXAMgc7BftssHZcZp8x17LOQj6k`)
- จังหวัด: `Sheet3` (`14kBtY2tdMXi3I9rbNleokmyJ_WWGRmKXPPU2VaVstZQ`)
- ข้อมูลที่ผู้ใช้แก้/ย้าย/แบ่งใน UAT: `1RJcsrbWnGO7gMiq9bhBR4bA9Twh1NjqP6816dXOW9DI`

## ข้อจำกัดรอบ UAT

- Hold การสแกน Tote/LPN, เปิด/ส่งพาเลท, แก้ LPN และปิดสาขา
- เปิดให้ทดสอบการค้นหา Wave/Booking, ตรวจยอด และหน้าเอกสาร
- ไม่เขียนกลับ `Member Data` หรือ `Delivery report` ตัวจริง
- Production branch และ Production Render service ไม่ถูกเปลี่ยน

## Render environment

- `APP_ENV=uat`
- `UAT_SHEETS_ONLY=true`
- `SCAN_FEATURE_ENABLED=false`
