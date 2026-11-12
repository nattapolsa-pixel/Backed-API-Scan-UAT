# Pro Scanner UAT — Google Sheets only

ระบบนี้แยกจาก Production และ **ไม่มีโค้ด BigQuery เหลืออยู่แล้ว** ทุกอย่างอ่าน/เขียน Google Sheets

## แหล่งข้อมูล

- ยอดกล่อง, Wave, รหัส/ชื่อสาขา, BU: `Member Data` (`1MO3lu1GssPZZvaruwQ5trUB045dzh4HUHdH35mbyOtc`)
- Booking, Wave, Carrier, ชื่อคนส่ง, ทะเบียนรถ: `Booking & Wave` (`1jOnJnnwlWZ491FEAFXAMgc7BftssHZcZp8x17LOQj6k`)
- จังหวัด/ภาคของสาขา: `ข้อมูลสาขา` (`18-gD0iSI3ivMijKQi54Ds-7Gm2p-LFyovjEs1MelrKQ`)
- Planned Pick Date + สาขาที่คาดหวัง: `Wave_Monitoring` (`1TL-tj-BrvYM7i_wNHlA0x641_VOqfT9SLpmm2NZATOo`)
- วันที่โหลดและสถานะส่งรายสาขาสำหรับกู้รายงาน Wave เก่าภายใน 14 วัน: `TMS` ใน `Booking & Wave` (`1jOnJnnwlWZ491FEAFXAMgc7BftssHZcZp8x17LOQj6k`); ใช้เฉพาะเมื่อ Wave หลุดจาก `Wave_Monitoring` และจับคู่ Booking/Wave/สาขาได้ชัดเจน
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
| `APP_VERSION` | `1.9.32-free` | เวอร์ชัน API; เวอร์ชันเว็บแยกกันและส่ง `app_version`/`api_version` คนละช่อง |
| `UAT_DATABASE_SPREADSHEET_ID` | `1RJcs…` | ที่เก็บ event log รวม Debug Events และยอดจริงแยก Booking |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | (secret) | สิทธิ์เขียน Sheets |

`UAT_SHEETS_ONLY` ถูกถอดออกแล้ว — Sheets เป็นโหมดเดียวที่มี ตัวแปรนี้ไม่มีผลอะไรถ้ายังค้างอยู่ใน Render

## Release notes

- **API 1.9.32-free (2026-11-12):** skip TMS rows whose Booking no longer exists in `Booking & Wave`, and read an individual Wave from Member Data when the full cold-start export is incomplete, so one unrelated row or a cold cache does not stop recent report recovery.
- **API 1.9.31-free (2026-11-12):** restore missing completed Delivery Report rows for recent Waves after they roll out of `Wave_Monitoring`, using the branch's structured TMS load date and completed status. Prefer fresh Monitoring dates, keep existing historical row dates unchanged, and skip ambiguous multi-Wave matches.
- **Frontend 1.9.29-free / API 1.9.30-free (2026-11-10):** when a device edits only selected quantity fields, preserve all untouched fields from the latest Booking actual or split record so a stale device cannot overwrite newer values.
- **Frontend 1.9.29-free / API 1.9.29-free (2026-11-10):** shorten the successful count-variance alert to a one-line summary and report only fields the operator actually edited, avoiding long lists of untouched fields such as pallet count.
- **Frontend 1.9.28-free / API 1.9.28-free (2026-11-10):** save manually counted Booking totals even when a box type is over or under the system's remaining allocation. Return a per-branch/Wave warning with the entered amount, expected remaining amount, and variance; show the warning after saving without blocking the operator-entered actual.
- **Frontend 1.9.27-free / API 1.9.26-free (2026-11-10):** persist committed quantity edits on Enter/change and print from the latest editor values; keep in-progress branch edits and hide/restore choices during shared-state refreshes.
- **Frontend 1.9.26-free / API 1.9.26-free (2026-11-10):** preserve in-progress branch quantity edits across shared-state refreshes, commit numeric fields explicitly on Enter/change, and keep the operator's latest hide/restore choice until it is acknowledged by the server so printed totals and hidden branches match the edited document.
- **Frontend 1.9.25-free / API 1.9.25-free (2026-11-06):** link a no-vehicle parent Booking to its `/1`, `/2` truck Bookings from `Booking & Wave`; let operators allocate each branch's quantities to a child Booking in Pro Scanner and persist per-vehicle allocations in `Booking Branch Splits`. Child Bookings show only their saved allocations, parent totals show the unassigned remainder, and child Bookings cannot bypass allocation through whole-branch moves or Booking actuals.
- **Frontend 1.9.24-free / API 1.9.24-free (2026-11-06):** return available Wave/Booking data when monitored branches are missing, identify the missing branch codes, and let operators continue to print after acknowledging the warning. True lookup/network failures still block document opening.
- **Frontend 1.9.23-free / API 1.9.23-free (2026-11-03):** allow a Member Data Wave/branch to be added to the selected Booking when no source Booking can be resolved; record the operator-entered actual directly and preserve the Report date gate.
- **Frontend 1.9.22-free / API 1.9.22-free (2026-11-03):** determine Wave/branch existence from Member Data, not date-qualified Wave_Monitoring rows. Preserve unambiguous source Booking metadata when Monitor dates are missing; keep Delivery Report rows gated on complete dates and show the saved Booking branch while report dates are pending.
- **Frontend 1.9.21-free / API 1.9.21-free (2026-11-03):** restore saved Booking branch actuals into the document even if Member Data is delayed; retain zero-box monitored branches; wait for complete Wave/Booking branch sets before returning data.
- **Frontend 1.9.20-free / API 1.9.20-free (2026-11-03):** after adding a branch from a Wave-opened document, refresh the target Booking view so the added branch appears in the table and shipping document. Keep the existing fast cache refresh; do not reload the full Member Data sheet.
- **Frontend 1.9.19-free / API 1.9.19-free (2026-11-03):** reject Member Data refreshes and snapshots that contain fewer than 1,000 branch rows, preserving the last known-good cache. Re-read only Waves with missing monitored branches and return a retryable error instead of a successful partial Booking. After adding a branch, reuse the updated API cache rather than forcing a full Member Data reload; show separately when the save succeeded but the document refresh did not.
- **Frontend 1.9.18-free / API 1.9.18-free (2026-11-03):** require the operator to enter one Wave number and branch code; search only that exact Wave and accept it only when all three report dates are current and complete. Keep the open document Booking as the destination, recalculate any managed source remainder on each add, and sync affected Booking rows plus their combined total to Member Data and UAT Delivery Report. Amounts above the inspected total remain allowed with a warning; other actual-quantity saves keep their allocation limits.
- **Frontend 1.9.14-free (2026-11-03):** allow the add-branch panel when the current Booking was resolved from a Wave lookup.
- **Frontend 1.9.13-free / API 1.9.13-free (2026-11-02):** add branches by code and record actual quantities per Booking; cap additions by the remaining source quantity per box type, aggregate actual Booking quantities into Member Data, and sync each Booking's actual row to the UAT Delivery report.
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
