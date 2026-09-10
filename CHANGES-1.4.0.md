# 1.4.0-free — ตัด BigQuery ออก + เร่งความเร็วบน Render Free

สรุปสิ่งที่เปลี่ยนใน 2 repo ที่ต้อง deploy พร้อมกัน

- `Backed-API-Scan-UAT` — `main.py`, `render.yaml`, `UAT.md`, `.github/workflows/keep-alive.yml`
- `Pro-Scan-Uat` — `index.html`

> ⚠️ **ต้อง push ทั้งสอง repo** `APP_VERSION` และ `SCAN_FEATURE_ENABLED` ต้องตรงกันสองฝั่ง

---

## 1. ตัด BigQuery ออกหมด

`main.py` ลดจาก **5,244 → 3,850 บรรทัด** (−1,394 บรรทัด, −27%)

สิ่งที่ลบทั้งฟังก์ชัน:

| ที่ลบ | เดิมทำอะไร |
|---|---|
| `fetch_wave_data_from_bq` | ดึงข้อมูล Wave จาก BigQuery (446 บรรทัด) |
| `get_valid_lpns_for_wave` | ตรวจ LPN — เดิมล้มเงียบแล้วคืน set ว่าง ทำให้ **ข้ามการตรวจ** |
| `get_durable_close_summaries` | อ่าน CLOSE_SUMMARY — คืน `{}` เสมออยู่แล้ว |
| `load_pack_case_map`, `calculate_direct_total_qty` | แมป CASECNT จาก BigQuery |
| `load_numeric_branch_master`, `_build_numeric_branch_map` | รหัสสาขาตัวเลข |
| `query_pending_waves_from_bigquery` | → เขียนใหม่เป็น `query_pending_waves()` อ่านจาก Member Data |
| `recover_recent_report_syncs` | เดิม `return` ทันทีในโหมด Sheets |
| `run_close_job_queries_in_background` | ยิง AUTO_NOT_FOUND เข้า BigQuery |
| `write_delivery_report_summaries` | เขียน Delivery report ตัวจริง (ถูกปิดไว้อยู่แล้ว) |
| `background_refresh_wave` | ไม่มีใครเรียก |
| `GET /api/debug-carton` | BigQuery ล้วน 500 แน่นอน |

ลบ dependency: `google-cloud-bigquery`, `import math`, `ThreadPoolExecutor`
ลบ flag: `UAT_SHEETS_ONLY` (Sheets เป็นโหมดเดียวแล้ว), `QC_FEATURE_ENABLED`, `LEGACY_DELIVERY_REPORT_SYNC_ENABLED`

### เขียนใหม่ให้ทำงานบน Sheets จริง

3 endpoint ที่เคย **500 แน่นอน** ตอนนี้ทำงานได้จริงเมื่อเปิดสแกน:

| Endpoint | เดิม | ใหม่ |
|---|---|---|
| `POST /api/close-job` | `client.insert_rows_json` → 500 | เขียนแท็บ `Branch Close Status` แล้วเข้าคิวรายงาน |
| `POST /api/correct-lpn` | 4 BigQuery queries → 500 | เขียน audit + ค่าใหม่ลง `Scan Transactions` กันซ้ำด้วย `correction_id` |
| `GET /api/corrections` | BigQuery query → 500 | อ่านประวัติจาก `Scan Transactions` |

`start-pallet`, `submit-pallet`, `scan`, `scan-batch` ตัดสาขา BigQuery ออก เหลือทาง Sheets ที่มีอยู่แล้ว

### ตอนนี้ hold แล้วตอบ 423 ทุกตัว (ทดสอบแล้ว)

`SCAN_FEATURE_ENABLED=false` → `scan`, `scan-batch`, `start-pallet`, `submit-pallet`, `correct-lpn`, `close-job` ตอบ **423 Locked** ไม่มีตัวไหน 500

ฝั่งเว็บเพิ่ม `423` เข้า dead-letter list — เดิม `processQueue` ถือว่า retry ได้ จึงจะวนซ้ำไม่จบ

---

## 2. เร่งความเร็วบน Free plan (0.1 CPU / 512 MB / 1 worker)

### ต้นเหตุที่ช้าที่สุด: Member Data 39,000 แถว

| สิ่งที่ทำ | ผล |
|---|---|
| **stale-while-revalidate** | cache หมดอายุ → คืนข้อมูลเดิม **0.4 ms** แล้วรีเฟรชเบื้องหลัง เดิมผู้ใช้คนที่ซวยรอถึง **45 วินาที** |
| **gzip ทุกการอ่าน gviz** | `fetch_gviz_text()` ส่ง `Accept-Encoding: gzip` — CSV/JSON บีบอัดได้หลายเท่า |
| **index ตาม Wave** | เดิม 1 Wave = สแกน 39,000 แถว, Booking 5 Wave = 5 รอบ ตอนนี้เป็น dict lookup |
| **memoise รายการต่อ Wave** | 1 request เคยสร้างรายการเดิม 2+ ครั้ง (`build_uat_wave_data` + `merge_member_history`) วัดได้ 234 µs → **4 µs** |
| **snapshot บนดิสก์** (`/tmp`, gzip) | worker restart อ่านจากดิสก์ ไม่ต้องโหลดใหม่ |
| **เลิก deepcopy บนแถว string ล้วน** | `_copy_flat_records()` ใช้ `dict()` ต่อแถว — เดิม deepcopy ทุกการอ่าน Wave/Booking |
| **compile regex ครั้งเดียว + cache ชื่อสาขา** | `clean_branch_display_name` รัน 3 regex × 39,000 แถว ตอนนี้เป็น dict lookup |
| **write-back ไม่ต้องโหลดใหม่** | `_apply_member_history_writes()` ยัดแถวที่เพิ่งเขียนเข้า cache เดิมล้าง cache แล้วโหลด 39k แถวใหม่ |

### จุดอื่น

- **Booking fan-out เป็น sequential** — ตอนนี้เป็นงาน CPU ล้วน (อ่านจาก memory) thread pool แย่งกันเปล่า ๆ บน 0.1 CPU
- **health check ราคาถูก** — ไม่แตะเครือข่าย ใช้ `?deep=1` เมื่อต้องตรวจ Google credentials พร้อมรายงานจำนวนแถว Member Data ใน cache
- **warm cache ตอนบูต** เรียงใหม่: Member Data → pending waves (ฟรี ใช้ข้อมูลใน memory) → booking meta → pick dates
- **GZip middleware** `minimum_size` 1024 → 512
- **`force=true` ยังบังคับอ่านใหม่จริง** — ทดสอบแล้วว่าปุ่มรีเฟรชได้ข้อมูลสด และ 5 request พร้อมกันตอน cache ว่างโหลดแค่ **1 ครั้ง**

### ฝั่งเว็บ

- **`preconnect` ไป Render + Apps Script** — DNS + TLS handshake เกิดพร้อมโหลดหน้า ไม่ใช่หลังกดค้นหา
- **ปลุก Render ทันทีใน `<head>`** — ยิง `/api/health` ก่อน JS ทั้งไฟล์จะ parse ระหว่างคนพิมพ์รหัสพนักงานเครื่องก็ตื่นแล้ว `ensureBackendReady()` ใช้ผลนี้ต่อ ไม่ยิงซ้ำ
- **ลด retry ladder ของ Booking** 5 รอบ → 3 รอบ (เว้นห่าง 2/4/6 วิ)

### กัน cold start

`.github/workflows/keep-alive.yml` — ping `/api/health` ทุก 10 นาที **06:00–20:00 จ.–ส.** เวลาไทย
≈ 390 ชม./เดือน อยู่ในโควตา **750 ชม.** ของ Render Free

---

## 3. ยังไม่ได้แก้ (ตั้งใจ)

- **ไม่มี auth ที่ endpoint ใดเลย** — `emp_id` ยังเป็นค่าที่ client ส่งมาเอง ปลอมได้ CORS เป็นการควบคุมเดียว
- `GET /api/test-sheets-write` และ `POST /api/clear-device-states` ยังเปิดอยู่ ไม่มี auth และไม่มีใครเรียก
- ฟิลด์ 9 ตัวที่เว็บส่งแล้ว Pydantic ทิ้ง (`autoM`, `paperBoxCount`, `owner`, …) — ไม่ใช่บั๊กตอนนี้ แต่ถ้าจะใช้ต้องเพิ่มใน `DocumentSummaryData` ก่อน
- `index.html` 4443: หลาย Wave ต่อ comma ถูก `replace(/\D/g,'')` รวมเป็นเลขก้อนเดียว

## 4. ทดสอบอะไรไปแล้ว

- `py_compile` ผ่าน + ตรวจ undefined name ทุกฟังก์ชัน = **0**
- import module สำเร็จ, ลงทะเบียน **28 routes**
- 6 endpoint ที่ hold → ยืนยัน **423** ไม่ใช่ 500
- `check-wave`, `check-booking`, `pending-waves`, `health`, `health?deep=1`, `transport-meta`, `wave-branch-options`, `document-overrides` ทำงานครบ
- stale-while-revalidate / force / snapshot / single-flight / gzip header / write-back fold — ทดสอบทีละอย่าง
- `node --check` ผ่านทั้ง 3 script block ใน `index.html`

ยังไม่ได้ทดสอบกับ Google Sheets ตัวจริง (sandbox ต่อเน็ตออกไม่ได้) — หลัง deploy ให้เช็ก
`GET /api/health?deep=1` ว่า `google_credentials.ok = true` และ `member_data.rows` มากกว่า 0
