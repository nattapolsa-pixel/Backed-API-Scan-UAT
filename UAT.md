# Pro Scanner UAT — Google Sheets only

ระบบนี้แยกจาก Production และ **ไม่มีโค้ด BigQuery เหลืออยู่แล้ว** ทุกอย่างอ่าน/เขียน Google Sheets

## แหล่งข้อมูล

- ยอดกล่อง, Wave, รหัส/ชื่อสาขา, BU: `Member Data` (`1MO3lu1GssPZZvaruwQ5trUB045dzh4HUHdH35mbyOtc`)
- Booking, Wave, Carrier, ชื่อคนส่ง, ทะเบียนรถ: `Booking & Wave` (`1jOnJnnwlWZ491FEAFXAMgc7BftssHZcZp8x17LOQj6k`)
- จังหวัด/ภาคของสาขา: `ข้อมูลสาขา` (`18-gD0iSI3ivMijKQi54Ds-7Gm2p-LFyovjEs1MelrKQ`)
- Planned Pick Date + สาขาที่คาดหวัง: `Wave_Monitoring` (`1TL-tj-BrvYM7i_wNHlA0x641_VOqfT9SLpmm2NZATOo`)
- ข้อมูลที่ผู้ใช้แก้/ย้าย/แบ่ง/ปิดจบ ใน UAT: `1RJcsrbWnGO7gMiq9bhBR4bA9Twh1NjqP6816dXOW9DI`
- สำเนารายงานสำหรับ UAT: `1Am1cC8ORHgRfbyA_kfBEWQpDQZlKm1Ii8-wsx39o4xQ`

เขียนกลับได้เฉพาะ **Member Data**, **UAT database** และ **สำเนารายงาน UAT**
`Delivery report` ตัวจริงเป็น lookup อ่านอย่างเดียว ไม่มี code path ไหนเขียนถึงอีกแล้ว
การเปิดเอกสารใบคุมส่งและการซ่อน/ย้าย/แบ่งสาขาเขียนเฉพาะสำเนา `Delivery report` สำหรับ UAT

## ข้อจำกัดรอบ UAT

- Hold การสแกน Tote/LPN, เปิด/ส่งพาเลท, แก้ LPN และปิดจบสาขา
  → endpoint เหล่านี้ตอบ **423 Locked** พร้อมข้อความอธิบาย ไม่ใช่ 500 อีกแล้ว
- เปิดให้ทดสอบการค้นหา Wave/Booking, ตรวจยอด, หน้าเอกสาร, ย้าย/แบ่ง Booking และ dashboard
- ไม่เขียนกลับ `Delivery report` ตัวจริง
- Production branch และ Production Render service ไม่ถูกเปลี่ยน

## Render environment

| ตัวแปร | ค่า | ผล |
|---|---|---|
| `APP_ENV` | `uat` | แสดงบน `/api/health` |
| `SCAN_FEATURE_ENABLED` | `false` | hold การสแกน → 423 (ต้องตรงกับ `SCAN_FEATURE_ENABLED` ใน `index.html`) |
| `SCAN_DEMO_ONLY` | `true` | ถ้าเปิดสแกน จะแก้เฉพาะ memory ไม่เขียน Sheet |
| `APP_VERSION` | `1.9.12-free` | เวอร์ชัน API; เวอร์ชันเว็บแยกกันและส่ง `app_version`/`api_version` คนละช่อง |
| `UAT_DATABASE_SPREADSHEET_ID` | `1RJcs…` | ที่เก็บ event log 7 แท็บ รวม Debug Events |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | (secret) | สิทธิ์เขียน Sheets |

`UAT_SHEETS_ONLY` ถูกถอดออกแล้ว — Sheets เป็นโหมดเดียวที่มี ตัวแปรนี้ไม่มีผลอะไรถ้ายังค้างอยู่ใน Render

## Release notes

- **Frontend 1.9.9-free (2026-10-15):** after verified opening of the shipping document, queue a Delivery report-only snapshot; the shortage/excess document remains read-only.
- **API 1.9.12-free (2026-10-15):** remove existing UAT Delivery report rows unless all three required dates parse, then rebuild only records with dates confirmed by the current `Wave_Monitoring` source.
- **API 1.9.11-free (2026-10-15):** sync verified shipping-document snapshots directly to the UAT Delivery report without writing on document open to Member Data; hiding or moving a branch removes its old Booking row, and moving queues the new Booking row. Preserve these removals through reconciliation when a split allocates the source total to zero.
- **API 1.9.10-free (2026-10-15):** write Delivery report rows only for Wave+branch entries freshly confirmed by `Wave_Monitoring`, and require a planned pick date before deriving and writing all three report dates. Reconciliation no longer backfills old Waves from the full `Member Data` history or falls back to `Booking & Wave`; existing complete historical rows keep their dates when quantities are corrected.
- **API 1.9.9-free (2026-10-14):** keep source and target Booking report snapshots separate, repair stale existing split rows from the UAT event log, and return from split requests without rebuilding two full Booking views. Avoid reformatting and sorting the entire Delivery Report on ordinary row updates; serialize split validation and writes so concurrent targets cannot over-allocate one Wave+branch.
- **API 1.9.8-free (2026-10-14):** keep the Booking move/split flow from forcing full Member Data refreshes. Revalidate only the selected Wave, rebuild both Booking views from the warmed cache and latest split event, and retain only the target view in memory. This reduces request time and peak RAM for branch splits on the Free API.
- **API 1.9.7-free (2026-10-12):** synchronize the Blueprint start command with the Free service configuration. Keep one Gunicorn worker, cap worker connections at 200, and recycle it every 400–500 requests to reduce accumulated memory use. The Render plan remains Free.

## ประสิทธิภาพบน Free plan (0.1 CPU / 512 MB / 1 worker)

สิ่งที่ทำไว้เพื่อให้เว็บไม่ค้าง:

1. **stale-while-revalidate บน Member Data** — cache หมดอายุแล้วจะ *คืนข้อมูลเดิมทันที* และรีเฟรชเบื้องหลัง
   ไม่มีผู้ใช้คนไหนต้องรอโหลด Member Data กว่า 100,000 แถว (เดิมรอได้ถึง 45 วินาที)
2. **gzip บนทุกการอ่าน gviz** — `fetch_gviz_text()` ส่ง `Accept-Encoding: gzip` ลดขนาดโหลดหลายเท่า
3. **index ตาม Wave** — เดิมค้นหา 1 Wave ต้องสแกนทั้งชีต, Booking ที่มี 5 Wave ทำ 5 รอบ
4. **memoise รายการต่อ Wave** — 1 request เคยสร้างรายการเดิมซ้ำหลายครั้ง ตอนนี้สร้างครั้งเดียว
5. **snapshot บนดิสก์** (`/tmp`, gzip) — worker restart แล้วอ่านจากดิสก์ ไม่ต้องโหลดใหม่
6. **เลิก deepcopy บนแถวที่เป็น string ล้วน** — ใช้ `dict()` ต่อแถว เร็วกว่าหลายเท่า
7. **Booking fan-out เป็น sequential** — ตอนนี้เป็นงาน CPU ล้วน (อ่านจาก memory) thread จึงแย่งกันเปล่า ๆ
8. **health check ราคาถูก** — ไม่แตะเครือข่าย ใช้ `?deep=1` เมื่อต้องตรวจ Google credentials
9. **warm cache ตอนบูต** — โหลด Member Data → pending waves → booking meta ตามลำดับ ไม่ block startup
10. **keep-alive cron** — GitHub Actions ping ทุก 10 นาที 06:00–20:00 จ.–ส. กัน cold start
    (~364 ชม./เดือน อยู่ในโควตา 750 ชม. ของ Render Free)

### สิ่งที่ต้องระวัง

- **`-w 1` เท่านั้น** — cache และ single-flight lock ทั้งหมดอยู่ใน memory ของ process เดียว
  เพิ่ม worker = cache แตกเป็นคนละชุด ยอดสองคำขอจะไม่ตรงกัน
- Render Free ปิดเครื่องเองหลังไม่มี traffic 15 นาที — cron ครอบเฉพาะเวลาทำงาน
  นอกเวลานั้นคำขอแรกยังรอ cold start (แต่ startup อุ่น cache ให้เอง)
