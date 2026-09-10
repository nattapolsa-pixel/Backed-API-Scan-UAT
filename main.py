from fastapi import FastAPI, HTTPException, BackgroundTasks, Response
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from typing import List, Optional       # ✅ แก้ไข #1: เพิ่ม Optional
import google.auth
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
import csv
import gzip
import json
import base64
import io
import os
import tempfile
import re
import time
import copy
import uuid
import datetime
import urllib.parse
import urllib.request
import threading
from threading import Lock
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# ✅ GZip Compression: ลด payload size สำหรับ response ขนาดใหญ่ (Wave data)
# ระดับ 4 ลด CPU บน Free 0.1 CPU แต่ยังลด payload Wave/Booking ได้มาก
app.add_middleware(GZipMiddleware, minimum_size=512, compresslevel=4)

# ✅ จำกัด CORS: อนุญาตเฉพาะหน้าเว็บบน GitHub Pages (*.github.io) + localhost สำหรับทดสอบ
#    ถ้าใช้โดเมนอื่น (custom domain) ให้เพิ่ม origin นั้นใน ALLOWED_ORIGINS ด้วย
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://pro-scanner-uat.onrender.com",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=r"https://[a-z0-9-]+\.github\.io",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Local development convenience only. On Render, credentials arrive through
# GOOGLE_SERVICE_ACCOUNT_JSON. The file keeps its historical name; it is used
# purely as a Google Sheets service account now.
if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    for candidate in ("service-account.json", "bq-key.json"):
        local_key_path = os.path.join(os.path.dirname(__file__), candidate)
        if os.path.exists(local_key_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_key_path
            break

# Google Sheets is the only data source. Credentials are loaded lazily by
# get_sheets_session(), so a cold start never waits on an auth round-trip.
APP_ENV = os.environ.get("APP_ENV", "uat").strip().lower()
APP_VERSION = os.environ.get("APP_VERSION", "1.4.0-free").strip()
SCAN_DEMO_ONLY = os.environ.get("SCAN_DEMO_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
# Scan is held by default.  The Render flag can be enabled briefly for an
# isolated presentation, without changing any document workflow.
SCAN_FEATURE_ENABLED = os.environ.get("SCAN_FEATURE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
PROCESS_STARTED_AT = time.time()


# UAT Google Sheets migration. Production code lives in a separate worktree/branch.

# ไฟล์ Control Outbound ที่ใช้งานจริง (Member Data เป็นแท็บแรก)
MEMBER_HISTORY_SPREADSHEET_ID = "1MO3lu1GssPZZvaruwQ5trUB045dzh4HUHdH35mbyOtc"
MEMBER_HISTORY_GID = "1628470483"
MEMBER_HISTORY_CACHE_TTL_SECONDS = 10 * 60
# The sheet is ~39k rows: the download is the slowest thing this service does,
# so an expired cache is served stale while one background thread refreshes it.
MEMBER_HISTORY_HTTP_TIMEOUT = 45
MEMBER_HISTORY_ERROR_BACKOFF_SECONDS = 60
# A gzipped snapshot on the container's own disk survives a worker restart, so a
# restarted worker answers from disk instead of re-downloading 39k rows.
MEMBER_HISTORY_SNAPSHOT_PATH = os.path.join(tempfile.gettempdir(), "pro_scanner_member_history.json.gz")
MEMBER_HISTORY_SNAPSHOT_MAX_AGE_SECONDS = 6 * 60 * 60
MEMBER_HISTORY_SNAPSHOT_TTL_SECONDS = 30
# Member Data มี ~44,000 แถว และ instance มี RAM แค่ 512 MB (เคย OOM มาแล้วหลายรอบ)
# cache นี้ถูกล้างทุกครั้งที่ history รีเฟรช (ทุก ~10 นาที) จึงไม่ต้องเก็บเยอะ
MEMBER_HISTORY_ITEMS_CACHE_MAX = 16
member_history_cache = {"expires_at": 0.0, "loaded_at": 0.0, "data": {}, "by_wave": {}, "generation": 0}
member_history_items_cache = {}
member_history_refreshing = False
member_history_lock = Lock()
member_history_refresh_lock = Lock()
member_history_row_cache = {"expires_at": 0.0, "existing_map": {}, "last_data_row": 1}

# UAT only: isolated reconciliation target.  This is deliberately separate
# from the live Delivery report while the direct-write path is being verified.
UAT_REPORT_TEST_SPREADSHEET_ID = os.environ.get(
    "UAT_REPORT_TEST_SPREADSHEET_ID", "1Am1cC8ORHgRfbyA_kfBEWQpDQZlKm1Ii8-wsx39o4xQ"
)
UAT_REPORT_TEST_SHEET_NAME = "Delivery report"
UAT_REPORT_TEST_SHEET_ID = 0
BRANCH_MASTER_SPREADSHEET_ID = "18-gD0iSI3ivMijKQi54Ds-7Gm2p-LFyovjEs1MelrKQ"
BRANCH_MASTER_SHEET_NAME = "ข้อมูลสาขา"
WAVE_MONITORING_SPREADSHEET_ID = "1TL-tj-BrvYM7i_wNHlA0x641_VOqfT9SLpmm2NZATOo"
WAVE_MONITORING_SHEET_GID = "0"
WAVE_MONITORING_CACHE_TTL_SECONDS = 5 * 60
delivery_report_lock = Lock()
branch_province_cache = {"expires_at": 0.0, "data": {}}
branch_province_refresh_lock = Lock()
branch_report_cache = {"expires_at": 0.0, "data": {}}
branch_report_refresh_lock = Lock()
wave_monitoring_pick_date_cache = {"expires_at": 0.0, "exact": {}, "waves": {}, "branches": {}}
wave_monitoring_pick_date_lock = Lock()
uat_report_test_row_cache = {"expires_at": 0.0, "existing_map": {}, "last_data_row": 1}
SHEET_ROW_CACHE_TTL_SECONDS = 15 * 60  # Free: ลดการอ่าน Sheet ซ้ำและรักษา RAM ให้อยู่ในขอบเขต
SHEETS_HTTP_TIMEOUT = (3, 45)
GVIZ_HTTP_TIMEOUT_SECONDS = 20
_sheets_session_local = threading.local()


def fetch_gviz_text(url: str, timeout: int = GVIZ_HTTP_TIMEOUT_SECONDS) -> str:
    """Read a Google Sheet through the gviz endpoint, asking for gzip.

    urllib does not request compression by default, so every one of these reads
    used to pull the full uncompressed CSV/JSON. Sheet exports are highly
    compressible, and decompression costs far less than the extra transfer time
    on a Free instance with a slow uplink.
    """
    request = urllib.request.Request(url, headers={
        "User-Agent": "Pro-Scanner-UAT/1.0",
        "Accept-Encoding": "gzip",
    })
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read()
        if "gzip" in (response.headers.get("Content-Encoding") or "").lower():
            payload = gzip.decompress(payload)
    return payload.decode("utf-8-sig")

# ฐานข้อมูลเหตุการณ์ของ UAT: ทุกอย่างที่หน้าเว็บเขียนกลับ เก็บเป็น event log 6 แท็บ
# แถวถูก append เท่านั้น และการอ่านใช้แถวล่าสุดต่อ key (ดู read_uat_event_records)
# ⚠️ เพิ่มคอลัมน์ได้เฉพาะต่อท้าย: การอ่านจับคู่ค่าตามลำดับ header ด้านล่างนี้
UAT_DATABASE_SPREADSHEET_ID = os.environ.get(
    "UAT_DATABASE_SPREADSHEET_ID", "1RJcsrbWnGO7gMiq9bhBR4bA9Twh1NjqP6816dXOW9DI"
)
UAT_EVENT_SHEETS = {
    "Document Overrides": [
        "Event_ID", "Action", "Wave_Number", "Booking_No", "Branch_Code", "Branch_Name",
        "M_Count", "Red_Count", "Blue_Count", "Green_Count", "Black_Count", "Total_Count",
        "Pallet_Count", "Is_Hidden", "Reason", "Emp_ID", "Created_At"
    ],
    "Booking Branch Moves": [
        "Event_ID", "Wave_Number", "Branch_Code", "Previous_Booking", "Assigned_Booking",
        "Reason", "Note", "Emp_ID", "Created_At"
    ],
    "Booking Branch Splits": [
        "Event_ID", "Wave_Number", "Branch_Code", "Source_Booking", "Target_Booking",
        "M_Count", "Red_Count", "Blue_Count", "Green_Count", "Black_Count", "Pallet_Count",
        "Is_Active", "Reason", "Note", "Emp_ID", "Created_At"
    ],
    "Branch Close Status": [
        "Event_ID", "Wave_Number", "Booking_No", "Branch_Code", "Branch_Name", "Status",
        "M_Count", "Red_Count", "Blue_Count", "Green_Count", "Black_Count", "Total_Count",
        "Pallet_Count", "Emp_ID", "Completed_At", "Created_At"
    ],
    "Usage Events": [
        "Event_ID", "Event_Type", "Emp_ID", "Emp_Name", "Wave_Number", "Booking_No",
        "Branch_Code", "Status", "Duration_Ms", "Detail", "Client_Time", "Created_At"
    ],
    # Scan events in this deployment are deliberately isolated from the live
    # live Production data. This makes the demo scanner usable without ever
    # writing anywhere outside this workbook.
    "Scan Transactions": [
        "Transaction_ID", "Wave_Number", "LPN", "Scan_Type", "Color", "Qty",
        "Branch_Code", "Branch_Name", "Emp_ID", "Pallet_No", "Created_At"
    ],
}
uat_event_sheet_lock = Lock()
uat_event_sheets_ready = False
uat_event_cache = {}
UAT_EVENT_CACHE_TTL_SECONDS = 45  # Free: ลด Sheets API calls; ทุกการเขียนยังล้าง cache ทันที
uat_event_read_lock = Lock()
dashboard_operations_cache = {"expires_at": 0.0, "rows": []}
dashboard_operations_lock = Lock()

def get_sheets_session():
    session = getattr(_sheets_session_local, "session", None)
    if session is not None:
        return session
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if credentials_json:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(credentials_json), scopes=scopes
        )
    else:
        credentials, _ = google.auth.default(scopes=scopes)
    session = AuthorizedSession(credentials)
    _sheets_session_local.session = session
    return session


def _uat_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7))).isoformat()


def ensure_uat_event_sheets():
    """Create UAT event-log tabs and headers once. Safe to call repeatedly."""
    global uat_event_sheets_ready
    if uat_event_sheets_ready:
        return
    with uat_event_sheet_lock:
        if uat_event_sheets_ready:
            return
        session = get_sheets_session()
        base = f"https://sheets.googleapis.com/v4/spreadsheets/{UAT_DATABASE_SPREADSHEET_ID}"
        metadata = session.get(base, params={"fields": "sheets.properties(title)"}, timeout=SHEETS_HTTP_TIMEOUT)
        metadata.raise_for_status()
        existing = {str(item.get("properties", {}).get("title") or "") for item in metadata.json().get("sheets", [])}
        missing = [name for name in UAT_EVENT_SHEETS if name not in existing]
        if missing:
            response = session.post(
                f"{base}:batchUpdate",
                json={"requests": [{"addSheet": {"properties": {"title": name}}} for name in missing]},
                timeout=SHEETS_HTTP_TIMEOUT,
            )
            response.raise_for_status()
        header_data = [
            {"range": f"'{name}'!A1:{chr(64 + len(headers))}1", "values": [headers]}
            for name, headers in UAT_EVENT_SHEETS.items()
        ]
        response = session.post(
            f"{base}/values:batchUpdate",
            json={"valueInputOption": "RAW", "data": header_data},
            timeout=SHEETS_HTTP_TIMEOUT,
        )
        response.raise_for_status()
        uat_event_sheets_ready = True


def append_uat_event_rows(sheet_name: str, rows: list):
    if not rows:
        return
    headers = UAT_EVENT_SHEETS[sheet_name]
    ensure_uat_event_sheets()
    values = [[row.get(header, "") for header in headers] for row in rows]
    session = get_sheets_session()
    encoded_range = urllib.parse.quote(f"'{sheet_name}'!A:{chr(64 + len(headers))}", safe="")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{UAT_DATABASE_SPREADSHEET_ID}/values/{encoded_range}:append"
    response = session.post(
        url,
        params={"valueInputOption": "USER_ENTERED", "insertDataOption": "INSERT_ROWS"},
        json={"values": values},
        timeout=SHEETS_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    with uat_event_read_lock:
        uat_event_cache.pop(sheet_name, None)


def _copy_flat_records(records: list) -> list:
    """Copy a list of flat string dicts.

    These rows only ever hold strings, so a shallow dict() per row is equivalent
    to deepcopy and roughly an order of magnitude cheaper — and this runs on
    every Wave and Booking read.
    """
    return [dict(row) for row in records]


def read_uat_event_records(sheet_name: str, force: bool = False) -> list:
    now = time.time()
    cached = uat_event_cache.get(sheet_name)
    if cached and not force and cached["expires_at"] > now:
        return _copy_flat_records(cached["records"])
    # Single-flight: คำขอ Wave/Booking ที่เข้าพร้อมกันใช้ผลโหลด Sheet ชุดเดียวกัน
    # แทนการยิง Google Sheets ซ้ำคนละ thread ตอน cache หมดอายุ
    with uat_event_read_lock:
        now = time.time()
        cached = uat_event_cache.get(sheet_name)
        if cached and not force and cached["expires_at"] > now:
            return _copy_flat_records(cached["records"])
        ensure_uat_event_sheets()
        headers = UAT_EVENT_SHEETS[sheet_name]
        rows = _sheet_values(get_sheets_session(), UAT_DATABASE_SPREADSHEET_ID, f"'{sheet_name}'!A:{chr(64 + len(headers))}")
        records = []
        for row in rows[1:]:
            values = list(row) + [""] * max(0, len(headers) - len(row))
            records.append(dict(zip(headers, values[:len(headers)])))
        uat_event_cache[sheet_name] = {"records": records, "expires_at": now + UAT_EVENT_CACHE_TTL_SECONDS}
        return _copy_flat_records(records)




def save_uat_scan_event(wave_no: str, lpn: str, branch_code: str, branch_name: str,
                        qty: int, scan_type: str, color: str, emp_id: str,
                        pallet_no: int, transaction_id: str = "") -> bool:
    """Durably save a scan into the isolated UAT workbook only."""
    txn = transaction_id or str(uuid.uuid4())
    # Presentation mode intentionally changes only the in-memory screen state.
    # It does not create Sheets tabs, append rows, or touch any document source.
    if SCAN_DEMO_ONLY:
        mark_transaction_processed(txn)
        record_local_scan(str(wave_no), str(lpn), str(branch_code), int(qty or 0),
                          str(scan_type), str(color), int(pallet_no or 0))
        return True
    # The durable check also protects a retry after a Render restart.
    if any(str(row.get("Transaction_ID") or "").strip() == txn
           for row in read_uat_event_records("Scan Transactions")):
        return False
    append_uat_event_rows("Scan Transactions", [{
        "Transaction_ID": txn, "Wave_Number": str(wave_no), "LPN": str(lpn).strip().upper(),
        "Scan_Type": str(scan_type), "Color": str(color), "Qty": int(qty or 0),
        "Branch_Code": str(branch_code).strip().upper(), "Branch_Name": str(branch_name).strip(),
        "Emp_ID": str(emp_id).strip(), "Pallet_No": int(pallet_no or 0), "Created_At": _uat_now_iso(),
    }])
    mark_transaction_processed(txn)
    record_local_scan(str(wave_no), str(lpn), str(branch_code), int(qty or 0),
                      str(scan_type), str(color), int(pallet_no or 0))
    return True


INVALID_BRANCH_STRINGS = {
    "", "-", "NONE", "NULL", "UNKNOWN", "FALSE", "TRUE",
    "#N/A", "#REF!", "#VALUE!", "#NAME?", "#DIV/0!", "#NUM!", "#NULL!", "N/A"
}

def _is_valid_wave_branch(wave_val, branch_val) -> bool:
    """Check whether a row contains a valid, non-error Wave number and Branch code."""
    wave_digits = re.sub(r"\D", "", str(wave_val or ""))
    branch_clean = str(branch_val or "").strip().upper()
    if not wave_digits or not branch_clean:
        return False
    if branch_clean in INVALID_BRANCH_STRINGS:
        return False
    try:
        if int(wave_digits) <= 0:
            return False
    except ValueError:
        return False
    return True


def scan_hold_error():
    raise HTTPException(
        status_code=423,
        detail="UAT นี้ Hold ระบบสแกน LPN/Tote ชั่วคราว กรุณาใช้งานเฉพาะเมนูเอกสาร",
    )

def member_data_bu(owner) -> str:
    """แปลงรหัส BU จากข้อมูล Wave ให้เป็นชื่อที่หน้างานใช้ใน Member Data."""
    code = str(owner or "").strip().upper()
    return {
        "DP02": "PUNTHAI",
        "DM02": "MAX MART",
        "MAXMART": "MAX MART",
        "MAX MART": "MAX MART",
    }.get(code, code or "Unknown")

def _apply_member_history_writes(written: list, date_str: str, time_str: str):
    """Fold rows we just wrote into the in-memory history so reads stay correct.

    The alternative — expiring the cache — would serve the pre-write numbers on
    the next read and pay for a full 39k-row reload to learn what we already know.
    """
    if not written:
        return
    with member_history_lock:
        history = member_history_cache.get("data")
        if not history:
            return
        by_wave = member_history_cache.get("by_wave") or {}
        for summary in written:
            wave = str(int(summary["wave"]))
            branch = str(summary["branch"]).strip().upper()
            row = {
                "date": date_str, "time": time_str, "wave": wave, "branch": branch,
                "branch_name": summary.get("branch_name") or branch,
                "bu": summary.get("bu") or "Unknown",
                "label_count": _history_int(summary.get("label_count")),
                "m": _history_int(summary.get("m")), "red": _history_int(summary.get("red")),
                "blue": _history_int(summary.get("blue")), "green": _history_int(summary.get("green")),
                "black": _history_int(summary.get("black")),
                "total": _history_int(summary.get("total")), "pallet": _history_int(summary.get("pallet")),
            }
            previous = history.get((wave, branch))
            history[(wave, branch)] = row
            wave_rows = by_wave.setdefault(wave, [])
            if previous is not None:
                for index, existing in enumerate(wave_rows):
                    if existing is previous or existing.get("branch") == branch:
                        wave_rows[index] = row
                        break
                else:
                    wave_rows.append(row)
            else:
                wave_rows.append(row)
        # Bump the generation so the per-Wave item cache rebuilds with the new totals.
        member_history_cache["generation"] = int(member_history_cache.get("generation") or 0) + 1
        member_history_items_cache.clear()


def write_member_history_summaries(summaries: list):
    """Upsert document totals using batchUpdate in 1 single HTTP request."""
    if not summaries:
        return
    session = get_sheets_session()
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{MEMBER_HISTORY_SPREADSHEET_ID}"
    now = time.time()
    if member_history_row_cache["existing_map"] and member_history_row_cache["expires_at"] > now:
        existing_map = dict(member_history_row_cache["existing_map"])
        last_data_row = int(member_history_row_cache["last_data_row"] or 1)
    else:
        lookup_range = urllib.parse.quote("Member Data!A:D", safe="")
        read_res = session.get(f"{base}/values/{lookup_range}", timeout=SHEETS_HTTP_TIMEOUT)
        read_res.raise_for_status()
        values = read_res.json().get("values") or []
        existing_map = {}
        last_data_row = 1
        for index in range(len(values), 1, -1):
            row = list(values[index - 1]) + [""] * max(0, 4 - len(values[index - 1]))
            wave_raw = row[2] if len(row) > 2 else ""
            branch_raw = row[3] if len(row) > 3 else ""
            if _is_valid_wave_branch(wave_raw, branch_raw):
                if index > last_data_row:
                    last_data_row = index
                wave_digits = re.sub(r"\D", "", str(wave_raw or ""))
                branch_code = str(branch_raw or "").strip().upper()
                key = (str(int(wave_digits)), branch_code)
                if key not in existing_map:
                    existing_map[key] = index

    now_bkk = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))
    date_str = now_bkk.strftime("%-d/%-m/%Y") if os.name != "nt" else f"{now_bkk.day}/{now_bkk.month}/{now_bkk.year}"
    time_str = now_bkk.strftime("%H:%M")

    batch_data = []
    written_rows = []
    current_append_row = last_data_row + 1

    for summary in summaries:
        target_wave = str(int(summary["wave"]))
        target_branch = str(summary["branch"]).strip().upper()
        key = (target_wave, target_branch)

        # Intentional zero corrections may clear an existing row, but never create a new zero row.
        if int(summary.get("total") or 0) <= 0 and key not in existing_map:
            print(f"Member Data zero update skipped (no existing row) | {target_wave}/{target_branch}")
            continue

        if key in existing_map:
            target_row = existing_map[key]
        else:
            target_row = current_append_row
            existing_map[key] = target_row
            current_append_row += 1

        row_values = [[
            date_str, time_str, target_wave, target_branch, summary["branch_name"], summary["bu"],
            summary["label_count"], "", summary["m"], summary["red"], summary["blue"],
            summary["green"], summary["black"], summary["total"], summary["pallet"], " Outbound"
        ]]
        batch_data.append({
            "range": f"Member Data!A{target_row}:P{target_row}",
            "values": row_values
        })
        written_rows.append(summary)

    if not batch_data:
        return

    batch_payload = {
        "valueInputOption": "USER_ENTERED",
        "data": batch_data
    }
    response = session.post(f"{base}/values:batchUpdate", json=batch_payload, timeout=SHEETS_HTTP_TIMEOUT)
    response.raise_for_status()

    member_history_row_cache["existing_map"] = dict(existing_map)
    member_history_row_cache["last_data_row"] = max(last_data_row, current_append_row - 1)
    member_history_row_cache["expires_at"] = time.time() + SHEET_ROW_CACHE_TTL_SECONDS

    # Apply what we just wrote straight into the in-memory rows instead of
    # expiring the cache. Expiring it would make the next read serve the stale
    # pre-write numbers (stale-while-revalidate) and cost a 39k-row reload for
    # data we already know.
    _apply_member_history_writes(written_rows, date_str, time_str)
    print(f"⚡ Member Data BATCH updated | {len(batch_data)} branches in 1 request")


def _sheet_values(session, spreadsheet_id: str, a1_range: str) -> list:
    encoded = urllib.parse.quote(a1_range, safe="")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded}"
    response = session.get(url, timeout=SHEETS_HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json().get("values") or []


def load_branch_province_map(session, force: bool = False) -> dict:
    now = time.time()
    if not force and branch_province_cache["expires_at"] > now:
        return copy.deepcopy(branch_province_cache["data"])

    with branch_province_refresh_lock:
        now = time.time()
        if not force and branch_province_cache["expires_at"] > now:
            return copy.deepcopy(branch_province_cache["data"])

        try:
            query = urllib.parse.urlencode({
                "tqx": "out:csv",
                "sheet": BRANCH_MASTER_SHEET_NAME,
                "tq": "select A,D",
            })
            url = (
                f"https://docs.google.com/spreadsheets/d/{BRANCH_MASTER_SPREADSHEET_ID}"
                f"/gviz/tq?{query}"
            )
            csv_rows = list(csv.reader(io.StringIO(fetch_gviz_text(url))))
            rows = [[row[0] if row else "", "", "", row[1] if len(row) > 1 else ""] for row in csv_rows]
        except Exception as source_error:
            print(f"Branch province source read unavailable: {source_error}")
            branch_province_cache["expires_at"] = now + 60
            return copy.deepcopy(branch_province_cache["data"])
        province_map = {}
        for row in rows[1:]:
            row = list(row) + [""] * max(0, 4 - len(row))
            code = str(row[0] or "").strip().upper()
            province = str(row[3] or "").strip()
            if code and province:
                province_map[code] = province

        branch_province_cache.update({
            "expires_at": time.time() + 600,
            "data": province_map,
        })
        return copy.deepcopy(province_map)


def load_branch_report_map(force: bool = False) -> dict:
    """Read province and region from the branch master, never from the staging tab."""
    now = time.time()
    if not force and branch_report_cache["expires_at"] > now:
        return copy.deepcopy(branch_report_cache["data"])

    with branch_report_refresh_lock:
        now = time.time()
        if not force and branch_report_cache["expires_at"] > now:
            return copy.deepcopy(branch_report_cache["data"])
        try:
            query = urllib.parse.urlencode({
                "tqx": "out:csv",
                "sheet": BRANCH_MASTER_SHEET_NAME,
                "tq": "select A,D,E",
            })
            url = f"https://docs.google.com/spreadsheets/d/{BRANCH_MASTER_SPREADSHEET_ID}/gviz/tq?{query}"
            rows = list(csv.reader(io.StringIO(fetch_gviz_text(url))))
        except Exception as source_error:
            print(f"Branch report source read unavailable: {source_error}")
            branch_report_cache["expires_at"] = now + 60
            return copy.deepcopy(branch_report_cache["data"])

        branch_map = {}
        for row in rows[1:]:
            row = list(row) + [""] * max(0, 3 - len(row))
            code = str(row[0] or "").strip().upper()
            if code:
                branch_map[code] = {"province": str(row[1] or "").strip(), "region": str(row[2] or "").strip()}
        branch_report_cache.update({"expires_at": time.time() + 600, "data": branch_map})
        return copy.deepcopy(branch_map)

BOOKING_WAVE_SHEET_ID = "1jOnJnnwlWZ491FEAFXAMgc7BftssHZcZp8x17LOQj6k"
BOOKING_WAVE_SHEET_GID = "499980322"
booking_wave_sheet_cache = {"expires_at": 0.0, "bookings": {}, "waves": {}}
booking_wave_sheet_lock = Lock()
booking_wave_sheet_refresh_lock = Lock()
BOOKING_WAVE_SHEET_CACHE_TTL_SECONDS = 10 * 60

def load_booking_wave_sheet_meta(force: bool = False) -> tuple:
    now = time.time()
    with booking_wave_sheet_lock:
        if not force and booking_wave_sheet_cache["expires_at"] > now:
            return booking_wave_sheet_cache["bookings"], booking_wave_sheet_cache["waves"]

    with booking_wave_sheet_refresh_lock:
        now = time.time()
        with booking_wave_sheet_lock:
            if not force and booking_wave_sheet_cache["expires_at"] > now:
                return booking_wave_sheet_cache["bookings"], booking_wave_sheet_cache["waves"]

        booking_map = {}
        wave_map = {}
        try:
            tq = "SELECT K, L, P, Q, R WHERE K IS NOT NULL OR L IS NOT NULL"
            url = f"https://docs.google.com/spreadsheets/d/{BOOKING_WAVE_SHEET_ID}/gviz/tq?gid={BOOKING_WAVE_SHEET_GID}&tqx=out:json&tq={urllib.parse.quote(tq)}"
            text = fetch_gviz_text(url)
            m = re.search(r"google\.visualization\.Query\.setResponse\((.*)\);", text, re.DOTALL)
            if m:
                data = json.loads(m.group(1))
                rows = data.get("table", {}).get("rows", [])
                for r in rows:
                    c = r.get("c") or []

                    def get_val(idx):
                        if idx < len(c) and c[idx]:
                            return str(c[idx].get("f") or c[idx].get("v") or "").strip()
                        return ""

                    b = get_val(0)
                    w = get_val(1)
                    carrier = get_val(2)
                    sender = get_val(3)
                    plate = get_val(4)
                    waves = [str(int(x)) for x in re.findall(r"\b\d{5,}\b", w)]
                    clean_b = re.sub(r"\s+", "", b.upper())
                    compact_b = clean_b.replace("-", "")
                    raw_num = re.sub(r"^B0*1*", "", compact_b)
                    entry = {
                        "booking": clean_b,
                        "waves": waves,
                        "carrier": carrier,
                        "sender": sender,
                        "plate": plate,
                    }
                    if clean_b:
                        booking_map[clean_b] = entry
                        booking_map[compact_b] = entry
                        if raw_num:
                            booking_map[raw_num] = entry
                            booking_map[f"B001-{raw_num}"] = entry
                    for wave_id in waves:
                        # One Wave may legitimately belong to more than one
                        # Booking.  Keep every booking instead of overwriting
                        # the previous row (which made the last row win).
                        for wave_key in (wave_id, f"{int(wave_id):010d}"):
                            entries = wave_map.setdefault(wave_key, [])
                            if not any(item.get("booking") == clean_b for item in entries):
                                entries.append(entry)

            with booking_wave_sheet_lock:
                booking_wave_sheet_cache["bookings"] = booking_map
                booking_wave_sheet_cache["waves"] = wave_map
                booking_wave_sheet_cache["expires_at"] = now + BOOKING_WAVE_SHEET_CACHE_TTL_SECONDS
        except Exception as e:
            print(f"⚠️ Error loading Booking & Wave Google Sheet meta: {e}")
            with booking_wave_sheet_lock:
                if booking_wave_sheet_cache["bookings"] or booking_wave_sheet_cache["waves"]:
                    booking_wave_sheet_cache["expires_at"] = time.time() + 30
                    return booking_wave_sheet_cache["bookings"], booking_wave_sheet_cache["waves"]

        return booking_map, wave_map

def get_sheet_metas_for_wave(wave_no: str, force: bool = False) -> list:
    clean_wave = re.sub(r"\D", "", str(wave_no or ""))
    if not clean_wave:
        return []
    _, wave_map = load_booking_wave_sheet_meta(force=force)
    value = wave_map.get(str(int(clean_wave))) or []
    # Backward-compatible with an already-warmed pre-fix cache.
    return list(value) if isinstance(value, list) else ([value] if value else [])


def get_sheet_meta_for_wave(wave_no: str, force: bool = False) -> dict:
    return (get_sheet_metas_for_wave(wave_no, force=force) or [{}])[0]

def get_sheet_meta_for_booking(booking_no: str, force: bool = False) -> dict:
    clean_b = re.sub(r"\s+", "", str(booking_no or "").upper())
    if not clean_b:
        return {}
    booking_map, _ = load_booking_wave_sheet_meta(force=force)
    compact = clean_b.replace("-", "")
    raw_num = re.sub(r"^B0*1*", "", compact)
    return booking_map.get(clean_b) or booking_map.get(compact) or booking_map.get(raw_num) or booking_map.get(f"B001-{raw_num}") or {}


def _wave_tokens(value) -> list:
    """Support single, zero-padded and multi-wave cell formats."""
    text = str(value or "").strip()
    if not text:
        return []
    tokens = []
    for match in re.findall(r"(?<!\d)\d[\d,]{4,12}(?!\d)", text):
        digits = re.sub(r"\D", "", match)
        if 5 <= len(digits) <= 10:
            tokens.append(str(int(digits)))
    return list(dict.fromkeys(tokens))


def _parse_pick_date(value):
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    text = text.split("T", 1)[0].strip()
    for pattern in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


def load_wave_monitoring_pick_dates(force: bool = False) -> tuple:
    """Load Wave(A), Planned Pick Date(B), Booking(I) without touching Delivery report."""
    now = time.time()
    with wave_monitoring_pick_date_lock:
        if not force and wave_monitoring_pick_date_cache["expires_at"] > now:
            return (copy.deepcopy(wave_monitoring_pick_date_cache["exact"]),
                    copy.deepcopy(wave_monitoring_pick_date_cache["waves"]),
                    copy.deepcopy(wave_monitoring_pick_date_cache["branches"]))

    exact = {}
    wave_dates = {}
    expected_branches = {}
    try:
        # The GViz query endpoint can inherit the sheet's active basic filter,
        # which previously hid older pick dates (for example 31/08/2026).
        # CSV export always returns the full tab regardless of the visible filter.
        url = (
            f"https://docs.google.com/spreadsheets/d/{WAVE_MONITORING_SPREADSHEET_ID}"
            f"/export?format=csv&gid={WAVE_MONITORING_SHEET_GID}"
        )
        rows = list(csv.reader(io.StringIO(fetch_gviz_text(url))))
        headers = rows[0] if rows else []
        header_index = {str(name).strip(): index for index, name in enumerate(headers)}
        wave_index = header_index.get("Wave_Number", 0)
        pick_date_index = header_index.get("Planned_Pick_Date", 1)
        booking_index = header_index.get("Vehicle_Booking_No", 8)
        branch_index = header_index.get("Branch_Code", 10)
        for row in rows[1:]:
            required_width = max(wave_index, pick_date_index, booking_index, branch_index) + 1
            row = list(row) + [""] * max(0, required_width - len(row))
            pick_date = _parse_pick_date(row[pick_date_index])
            booking = re.sub(r"\s+", "", str(row[booking_index] or "").upper())
            branch = str(row[branch_index] or "").strip().upper()
            if not pick_date:
                continue
            for wave in _wave_tokens(row[wave_index]):
                if booking:
                    exact[(booking, wave)] = pick_date
                    exact[(booking.replace("-", ""), wave)] = pick_date
                    if branch:
                        expected_branches.setdefault((booking, wave), set()).add(branch)
                wave_dates.setdefault(wave, pick_date)
        with wave_monitoring_pick_date_lock:
            wave_monitoring_pick_date_cache.update({
                "expires_at": time.time() + WAVE_MONITORING_CACHE_TTL_SECONDS,
                "exact": exact, "waves": wave_dates, "branches": expected_branches,
            })
    except Exception as exc:
        print(f"⚠️ Wave Monitoring pick-date read unavailable: {exc}")
        with wave_monitoring_pick_date_lock:
            if wave_monitoring_pick_date_cache["exact"] or wave_monitoring_pick_date_cache["waves"]:
                wave_monitoring_pick_date_cache["expires_at"] = time.time() + 30
                return (copy.deepcopy(wave_monitoring_pick_date_cache["exact"]),
                        copy.deepcopy(wave_monitoring_pick_date_cache["waves"]),
                        copy.deepcopy(wave_monitoring_pick_date_cache["branches"]))
    return exact, wave_dates, expected_branches


def get_wave_monitoring_pick_date(wave_no: str, booking_no: str = ""):
    waves = _wave_tokens(wave_no)
    if not waves:
        return None
    wave = waves[0]
    booking = re.sub(r"\s+", "", str(booking_no or "").upper())
    exact, wave_dates, _ = load_wave_monitoring_pick_dates()
    return (exact.get((booking, wave)) or exact.get((booking.replace("-", ""), wave))
            or wave_dates.get(wave))

# ==================== DOCUMENT OVERRIDES SYNC SYSTEM ====================
document_overrides_overlay = {}
document_overrides_lock = Lock()

def _document_override_scope_key(wave_no: str, booking_no: str = "") -> tuple:
    clean_wave = re.sub(r"\D", "", str(wave_no or ""))
    wave_key = str(int(clean_wave)) if clean_wave else ""
    booking_key = re.sub(r"\s+", "", str(booking_no or "").upper())
    return wave_key, booking_key


def get_document_overrides_for_wave(wave_no: str, booking_no: str = "") -> dict:
    clean_wave = re.sub(r"\D", "", str(wave_no or ""))
    if not clean_wave:
        return {}
    wave_key, booking_key = _document_override_scope_key(clean_wave, booking_no)
    cache_key = (wave_key, booking_key)
    with document_overrides_lock:
        if cache_key in document_overrides_overlay:
            # Intentional zero overrides must survive refresh and restart.
            return {
                branch: copy.deepcopy(value)
                for branch, value in document_overrides_overlay[cache_key].items()
            }

    exact_overrides = {}
    legacy_overrides = {}
    try:
        rows = [row for row in read_uat_event_records("Document Overrides")
                if re.sub(r"\D", "", str(row.get("Wave_Number") or "")) == clean_wave]
        for row in reversed(rows):
            action = str(row.get("Action") or "").strip().upper()
            if action == "RESET_ALL" or str(row.get("Branch_Code") or "").strip().upper() == "RESET_ALL":
                break
            row_booking = re.sub(r"\s+", "", str(row.get("Booking_No") or "").upper())
            # Booking splits share the same Wave+Branch. Never let an edit made
            # for the target booking overwrite the source booking (or vice versa).
            if booking_key:
                if row_booking not in (booking_key, ""):
                    continue
                target = exact_overrides if row_booking == booking_key else legacy_overrides
            else:
                if row_booking:
                    continue
                target = legacy_overrides
            branch = str(row.get("Branch_Code") or "").strip().upper()
            if not branch or branch in target:
                continue
            target[branch] = {
                "m": _history_int(row.get("M_Count")),
                "red": _history_int(row.get("Red_Count")),
                "blue": _history_int(row.get("Blue_Count")),
                "green": _history_int(row.get("Green_Count")),
                "black": _history_int(row.get("Black_Count")),
                "total": _history_int(row.get("Total_Count")),
                "pallet": _history_int(row.get("Pallet_Count")),
                "is_hidden": str(row.get("Is_Hidden") or "").strip().lower() in ("1", "true", "yes"),
                "emp_id": str(row.get("Emp_ID") or "").strip(),
                "updated_at": str(row.get("Created_At") or "").strip(),
                "branch_name": str(row.get("Branch_Name") or branch).strip(),
                "booking": str(row.get("Booking_No") or "").strip().upper(),
            }
        overrides = dict(legacy_overrides)
        overrides.update(exact_overrides)
        with document_overrides_lock:
            document_overrides_overlay[cache_key] = copy.deepcopy(overrides)
    except Exception as e:
        print(f"⚠️ Error reading document overrides from UAT Sheet: {e}")
    return overrides

def record_document_overrides(summaries: list, emp_id: str, reason: str = ""):
    if not summaries:
        return
    now_iso = _uat_now_iso()
    rows_to_insert = []
    for s in summaries:
        rows_to_insert.append({
            "Event_ID": str(uuid.uuid4()),
            "Action": "UPSERT",
            "Wave_Number": str(s["wave"]),
            "Booking_No": str(s.get("booking") or ""),
            "Branch_Code": str(s["branch"]).upper(),
            "Branch_Name": str(s.get("branch_name") or ""),
            "M_Count": int(s.get("m", 0) or 0),
            "Red_Count": int(s.get("red", 0) or 0),
            "Blue_Count": int(s.get("blue", 0) or 0),
            "Green_Count": int(s.get("green", 0) or 0),
            "Black_Count": int(s.get("black", 0) or 0),
            "Total_Count": int(s.get("total", 0) or 0),
            "Pallet_Count": int(s.get("pallet", 0) or 0),
            "Is_Hidden": 1 if s.get("is_hidden") else 0,
            "Reason": str(reason or "").strip(),
            "Emp_ID": str(emp_id or "").strip(),
            "Created_At": now_iso,
        })
    append_uat_event_rows("Document Overrides", rows_to_insert)
    print(f"✅ Saved {len(rows_to_insert)} document overrides to UAT Sheet")

def get_delivery_wave_meta(wave: str, booking: str = "") -> dict:
    meta = get_sheet_meta_for_wave(wave)
    resolved_booking = str(booking or meta.get("booking") or "").strip().upper()
    return {
        **meta,
        "pick_date": get_wave_monitoring_pick_date(wave, resolved_booking),
        "booking": resolved_booking,
    }

def delivery_business_dates(pick_date):
    if isinstance(pick_date, datetime.datetime):
        pick_date = pick_date.date()
    if not isinstance(pick_date, datetime.date):
        return None, None
    order_date = pick_date - datetime.timedelta(days=1)
    # ตัวอย่าง Pick จันทร์ 17 -> วันที่สั่งศุกร์ 14 (ไม่ใช้เสาร์-อาทิตย์เป็นวันสั่ง)
    if order_date.weekday() == 6:
        order_date -= datetime.timedelta(days=2)
    delivery_date = pick_date + datetime.timedelta(days=1)
    if delivery_date.weekday() == 6:
        delivery_date += datetime.timedelta(days=1)
    return order_date, delivery_date



def write_uat_report_test_summaries(summaries: list):
    """Upsert direct report rows to the isolated UAT reconciliation Sheet.

    The test Sheet has the final 20 report columns, so this path intentionally
    does not write or read the production \"วางข้อมูล\" staging tab.
    """
    if not summaries:
        return
    session = get_sheets_session()
    branch_map = load_branch_report_map()
    now_bkk = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))
    with delivery_report_lock:
        existing_date_updates = []
        now = time.time()
        if uat_report_test_row_cache["existing_map"] and uat_report_test_row_cache["expires_at"] > now:
            existing_map = dict(uat_report_test_row_cache["existing_map"])
            last_data_row = int(uat_report_test_row_cache["last_data_row"] or 1)
        else:
            existing = _sheet_values(session, UAT_REPORT_TEST_SPREADSHEET_ID, f"'{UAT_REPORT_TEST_SHEET_NAME}'!A:T")
            existing_map = {}
            last_data_row = 1
            for index, raw_row in enumerate(existing[1:], start=2):
                row = list(raw_row) + [""] * max(0, 20 - len(raw_row))
                booking = str(row[7] or "").strip().upper()
                wave_digits = re.sub(r"\D", "", str(row[9] or ""))
                branch = str(row[10] or "").strip().upper()
                if wave_digits and branch:
                    existing_map[(booking, str(int(wave_digits)), branch)] = index
                    last_data_row = max(last_data_row, index)
                # Convert legacy dd/MM/yyyy text to date serials once, so
                # Google Sheets sorts dates chronologically rather than as text.
                normalized_dates = []
                changed_dates = False
                for value in row[1:4]:
                    value = str(value or "").strip()
                    try:
                        parsed = datetime.datetime.strptime(value, "%d/%m/%Y").date()
                        normalized_dates.append(parsed.isoformat())
                        changed_dates = True
                    except ValueError:
                        normalized_dates.append(value)
                if changed_dates:
                    existing_date_updates.append({
                        "range": f"'{UAT_REPORT_TEST_SHEET_NAME}'!B{index}:D{index}", "values": [normalized_dates]
                    })

        batch_data = list(existing_date_updates)
        current_append_row = last_data_row + 1
        meta_cache = {}
        for summary in summaries:
            wave = str(int(str(summary["wave"]).strip()))
            branch = str(summary["branch"] or "").strip().upper()
            summary_booking = str(summary.get("booking") or "").strip().upper()
            meta_key = (wave, summary_booking)
            if meta_key not in meta_cache:
                meta_cache[meta_key] = get_delivery_wave_meta(wave, summary_booking)
            meta = meta_cache[meta_key]
            booking = str(summary.get("booking") or meta.get("booking") or "").strip().upper()
            key = (booking, wave, branch)
            target_row = existing_map.get(key)
            if not target_row:
                if int(summary.get("total") or 0) <= 0:
                    continue
                target_row = current_append_row
                current_append_row += 1
                existing_map[key] = target_row
            branch_meta = branch_map.get(branch, {})
            pick_date = meta.get("pick_date")
            # Never silently replace a missing planned pick date with today's
            # date: that creates an incorrect operational report.
            if not pick_date:
                print(f"UAT report skipped: planned pick date not found | {booking}/{wave}/{branch}")
                continue
            order_date, delivery_date = delivery_business_dates(pick_date)
            row = [[
                target_row - 1,
                order_date.isoformat() if order_date else "",
                pick_date.isoformat(),
                delivery_date.isoformat() if delivery_date else "",
                str(meta.get("carrier") or "").strip(),
                str(meta.get("sender") or "").strip(),
                str(meta.get("plate") or "").strip(),
                booking,
                member_data_bu(summary.get("bu")),
                wave,
                branch,
                clean_branch_display_name(summary.get("branch_name")),
                branch_meta.get("province", ""),
                branch_meta.get("region", ""),
                _history_int(summary.get("m")),
                _history_int(summary.get("red")),
                _history_int(summary.get("blue")),
                _history_int(summary.get("green")),
                _history_int(summary.get("black")),
                _history_int(summary.get("total")),
            ]]
            batch_data.append({"range": f"'{UAT_REPORT_TEST_SHEET_NAME}'!A{target_row}:T{target_row}", "values": row})

        if not batch_data:
            return
        base = f"https://sheets.googleapis.com/v4/spreadsheets/{UAT_REPORT_TEST_SPREADSHEET_ID}"
        # Google Sheets starts new tabs with 1,000 rows. A historical backfill
        # can exceed that limit, so extend the grid before writing A1 ranges
        # beyond the current row count. Existing rows/formulas are untouched.
        required_last_row = max(last_data_row, current_append_row - 1)
        metadata_response = session.get(
            base,
            params={"fields": "sheets(properties(sheetId,title,gridProperties(rowCount)))"},
            timeout=SHEETS_HTTP_TIMEOUT,
        )
        metadata_response.raise_for_status()
        target_properties = next((
            sheet.get("properties", {})
            for sheet in metadata_response.json().get("sheets", [])
            if int(sheet.get("properties", {}).get("sheetId", -1)) == UAT_REPORT_TEST_SHEET_ID
        ), {})
        current_row_count = int(target_properties.get("gridProperties", {}).get("rowCount", 0) or 0)
        if required_last_row > current_row_count:
            expand_response = session.post(f"{base}:batchUpdate", json={"requests": [{
                "appendDimension": {
                    "sheetId": UAT_REPORT_TEST_SHEET_ID,
                    "dimension": "ROWS",
                    "length": required_last_row - current_row_count,
                }
            }]}, timeout=SHEETS_HTTP_TIMEOUT)
            expand_response.raise_for_status()
        response = session.post(
            f"{base}/values:batchUpdate",
            json={"valueInputOption": "USER_ENTERED", "data": batch_data}, timeout=SHEETS_HTTP_TIMEOUT
        )
        response.raise_for_status()
        final_row = max(last_data_row, current_append_row - 1)
        # Format dates then sort data rows by pickup date (column C). The sort
        # excludes headers and runs only in the isolated UAT test sheet.
        sort_response = session.post(f"{base}:batchUpdate", json={"requests": [
            {"repeatCell": {
                "range": {"sheetId": UAT_REPORT_TEST_SHEET_ID, "startRowIndex": 1,
                          "endRowIndex": final_row, "startColumnIndex": 1, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            {"sortRange": {
                "range": {"sheetId": UAT_REPORT_TEST_SHEET_ID, "startRowIndex": 1,
                          "endRowIndex": final_row, "startColumnIndex": 0, "endColumnIndex": 20},
                "sortSpecs": [{"dimensionIndex": 2, "sortOrder": "ASCENDING"}]
            }},
        ]}, timeout=SHEETS_HTTP_TIMEOUT)
        sort_response.raise_for_status()
        # Re-number after sorting. Invalidate the row cache because positions
        # have changed and must be read again before the next upsert.
        sequence_values = [[row_no - 1] for row_no in range(2, final_row + 1)]
        sequence_response = session.post(f"{base}/values:batchUpdate", json={
            "valueInputOption": "RAW",
            "data": [{"range": f"'{UAT_REPORT_TEST_SHEET_NAME}'!A2:A{final_row}", "values": sequence_values}],
        }, timeout=SHEETS_HTTP_TIMEOUT)
        sequence_response.raise_for_status()
        uat_report_test_row_cache.update({"existing_map": {}, "last_data_row": 1, "expires_at": 0.0})
        dashboard_operations_cache.update({"expires_at": 0.0, "rows": []})
        print(f"⚡ UAT test Delivery report updated | {len(batch_data)} branches")


def summarize_branch_for_member_data(wave_data: dict, branch: str) -> dict:
    items = [item for item in wave_data.get("lpn_list", []) if str(item.get("branch") or "").strip().upper() == branch]
    # ตัดเฉพาะ transaction ที่ซ้ำกันทุกมิติจาก network retry แต่ LPN เดิมคนละพาเลทยังนับแยกตามปกติ
    unique_items = []
    seen_item_keys = set()
    for item in items:
        breakdown = item.get("color_breakdown") or []
        breakdown_key = (json.dumps(breakdown, sort_keys=True, ensure_ascii=False, default=str)
                         if isinstance(breakdown, (dict, list)) else str(breakdown))
        item_key = (
            str(item.get("lpn") or "").strip().upper(), str(item.get("status") or ""),
            int(item.get("qty") or 0), str(item.get("scan_type") or "").strip().upper(),
            str(item.get("color") or "").strip().upper(), int(item.get("pallet_no") or 0), breakdown_key,
        )
        if item_key in seen_item_keys:
            continue
        seen_item_keys.add(item_key)
        unique_items.append(item)
    items = unique_items
    split_summary = next((item.get("booking_split_summary") for item in items if item.get("booking_split_summary")), None)
    if split_summary:
        first = items[0] if items else {}
        totals = {field: max(0, int(split_summary.get(field) or 0)) for field in ("m", "red", "blue", "green", "black")}
        return {
            "wave": str(int(str(wave_data.get("wave_no") or 0))),
            "booking": str(wave_data.get("booking_no") or "").strip().upper(),
            "branch": branch, "branch_name": first.get("branch_name") or branch,
            "bu": member_data_bu(first.get("owner")), "label_count": len(items),
            **totals, "total": sum(totals.values()), "pallet": max(0, int(split_summary.get("pallet") or 0))
        }
    totals = {"m": 0, "red": 0, "blue": 0, "green": 0, "black": 0}
    def add(color, scan_type, qty, lpn=""):
        color = str(color or "None").upper(); scan_type = str(scan_type or "").upper(); prefix = str(lpn or "")[:2].upper()
        if prefix in DIRECT_QTY_PREFIXES or scan_type == "CARTON" or color in ("REUSE", "NONE", ""):
            totals["m"] += qty
        elif color == "RED": totals["red"] += qty
        elif color == "BLUE": totals["blue"] += qty
        elif color == "GREEN": totals["green"] += qty
        elif color == "BLACK": totals["black"] += qty
        else: totals["m"] += qty
    combined_children = {
        str(item.get("lpn") or "").strip().upper(): str(item.get("scan_type") or "").split(":", 1)[1].strip().upper()
        for item in items
        if str(item.get("scan_type") or "").upper().startswith("COMBINE:")
        and ":" in str(item.get("scan_type") or "")
    }
    combined_masters = set(combined_children.values())
    # หน้าจอถือ Combine หนึ่งกลุ่มเป็นภาชนะจริง 1 ใบ แม้ Master จะยังเป็น Pending
    # จึงนับจากรายการลูกที่สแกนแล้วหนึ่งครั้งต่อ Master ก่อน แล้วข้ามทั้งลูกและ Master ในลูปหลัก
    for master_lpn in combined_masters:
        master_item = next((item for item in items if str(item.get("lpn") or "").strip().upper() == master_lpn
                            and item.get("status") == "Scanned"), None)
        child_item = next((item for item in items if combined_children.get(str(item.get("lpn") or "").strip().upper()) == master_lpn
                           and item.get("status") == "Scanned"), None)
        physical_item = master_item or child_item
        if physical_item:
            physical_breakdown = physical_item.get("color_breakdown") or []
            if isinstance(physical_breakdown, str):
                parsed = []
                for part in physical_breakdown.split("|"):
                    bits = part.split("~", 2)
                    if len(bits) >= 2:
                        parsed.append({"color": bits[0], "qty": _history_int(bits[1]),
                                       "type": bits[2] if len(bits) > 2 else physical_item.get("scan_type")})
                physical_breakdown = parsed
            first_part = physical_breakdown[0] if physical_breakdown else {}
            add(first_part.get("color") or physical_item.get("color"),
                first_part.get("type") or physical_item.get("scan_type"), 1,
                master_lpn)
    for item in items:
        if item.get("status") != "Scanned": continue
        lpn = str(item.get("lpn") or "").strip().upper(); scan_type = str(item.get("scan_type") or "")
        if lpn in combined_children or lpn in combined_masters:
            continue
        qty = int(item.get("qty") or 0)
        prefix = lpn[:2].upper()
        # PP/SP (Direct Qty Pallets): Frontend นับ qty รวม 1 ครั้ง ไม่วน breakdown
        # ต้องทำเหมือนกันใน Backend มิฉะนั้นยอดจะพองขึ้นถ้า breakdown มีหลาย part
        if prefix in DIRECT_QTY_PREFIXES:
            totals["m"] += qty
            continue
        breakdown = item.get("color_breakdown") or []
        if isinstance(breakdown, str):
            parsed = []
            for part in breakdown.split("|"):
                bits = part.split("~", 2)
                if len(bits) >= 2: parsed.append({"color": bits[0], "qty": _history_int(bits[1]), "type": bits[2] if len(bits) > 2 else scan_type})
            breakdown = parsed
        if not breakdown:
            add(item.get("color"), scan_type, qty, lpn)
        else:
            for part in breakdown: add(part.get("color"), part.get("type"), _history_int(part.get("qty")), lpn)
    first = items[0] if items else {}
    scanned_items = [item for item in items if item.get("status") == "Scanned"]
    pallet_nos = {int(item.get("pallet_no") or 0) for item in scanned_items if int(item.get("pallet_no") or 0) > 0}
    if not pallet_nos:
        pallet_nos = {int(no) for item in scanned_items for no in (item.get("branch_pallet_nos") or []) if int(no) > 0}
    wave_key = str(int(str(wave_data.get("wave_no") or 0)))
    assignment = get_booking_branch_assignments().get((wave_key, branch))
    current_booking = str((assignment or {}).get("Assigned_Booking") or wave_data.get("booking_no") or "").strip().upper()
    return {"wave": wave_key, "booking": current_booking, "branch": branch,
            "branch_name": first.get("branch_name") or branch, "bu": member_data_bu(first.get("owner")),
            "label_count": len({str(item.get("lpn") or "") for item in items if item.get("lpn")}),
            **totals, "total": sum(totals.values()), "pallet": len(pallet_nos)}


def apply_document_override_to_summary(summary: dict) -> dict:
    """Apply only an intentional document override to a calculated scanner summary."""
    result = copy.deepcopy(summary)
    wave = str(result.get("wave") or "").strip()
    branch = str(result.get("branch") or "").strip().upper()
    if not wave or not branch:
        return result
    override = (get_document_overrides_for_wave(wave, result.get("booking")) or {}).get(branch) or {}
    if not override:
        return result
    for field in ("m", "red", "blue", "green", "black", "pallet"):
        if override.get(field) is not None:
            result[field] = max(0, int(override.get(field) or 0))
    result["total"] = sum(int(result.get(field) or 0) for field in ("m", "red", "blue", "green", "black"))
    result["is_hidden"] = bool(override.get("is_hidden", False))
    result["allow_zero_update"] = True
    return result


# ==================== RELIABLE REPORT SYNC COORDINATOR ====================
# API requests acknowledge the scanner immediately. A single background worker then
# coalesces rapid changes and writes the latest totals to both operational Sheets.
REPORT_SYNC_FAST_DELAY_SECONDS = 0.15
REPORT_SYNC_RECONCILE_DELAY_SECONDS = 1.75
REPORT_SYNC_MAX_DEBOUNCE_SECONDS = 3.0
report_sync_pending = {}
report_sync_pending_lock = Lock()
report_sync_wakeup = threading.Event()
report_sync_worker_started = False
report_sync_worker_start_lock = Lock()
REPORT_BOX_FIELDS = ("m", "red", "blue", "green", "black")


def normalize_report_summary(raw: dict, wave: str = "", branch: str = "") -> dict:
    """Normalize one report snapshot and always recalculate the box total."""
    summary = copy.deepcopy(raw or {})
    clean_wave = re.sub(r"\D", "", str(wave or summary.get("wave") or ""))
    summary["wave"] = str(int(clean_wave)) if clean_wave else ""
    summary["branch"] = str(branch or summary.get("branch") or "").strip().upper()
    for field in ("label_count", *REPORT_BOX_FIELDS, "pallet"):
        try:
            summary[field] = max(0, int(float(summary.get(field) or 0)))
        except (TypeError, ValueError):
            summary[field] = 0
    summary["total"] = sum(summary[field] for field in REPORT_BOX_FIELDS)
    summary["booking"] = str(summary.get("booking") or "").strip().upper()
    summary["branch_name"] = str(summary.get("branch_name") or summary["branch"]).strip()
    summary["bu"] = member_data_bu(summary.get("bu"))
    return summary




def _report_sync_key(wave: str, branch: str, mode: str) -> tuple:
    clean_wave = re.sub(r"\D", "", str(wave or ""))
    return (str(int(clean_wave)) if clean_wave else "", str(branch or "").strip().upper(), mode)


def queue_report_summary_snapshots(summaries: list, delay_seconds: float = REPORT_SYNC_FAST_DELAY_SECONDS):
    """Queue exact totals already shown by the web; latest snapshot wins per Wave+Branch."""
    now = time.time()
    with report_sync_pending_lock:
        for raw in summaries or []:
            summary = normalize_report_summary(raw)
            # Automatic zero snapshots are ignored; an intentional zero correction may clear an existing row.
            if summary.get("total", 0) <= 0 and not summary.get("allow_zero_update"):
                continue
            key = _report_sync_key(summary.get("wave"), summary.get("branch"), "snapshot")
            if not key[0] or not key[1]:
                continue
            previous = report_sync_pending.get(key) or {}
            first_at = float(previous.get("first_at") or now)
            report_sync_pending[key] = {
                "mode": "snapshot",
                "wave": key[0],
                "branch": key[1],
                "summary": summary,
                "first_at": first_at,
                "due_at": min(now + max(0.0, delay_seconds), first_at + REPORT_SYNC_MAX_DEBOUNCE_SECONDS),
                "attempts": int(previous.get("attempts") or 0),
            }
    report_sync_wakeup.set()


def queue_branch_totals_reconciliation(wave_branch_pairs, delay_seconds: float = REPORT_SYNC_RECONCILE_DELAY_SECONDS):
    """Queue a server-side recalculation from the Sheets sources."""
    now = time.time()
    with report_sync_pending_lock:
        for wave, branch in wave_branch_pairs or []:
            key = _report_sync_key(wave, branch, "reconcile")
            if not key[0] or not key[1]:
                continue
            previous = report_sync_pending.get(key) or {}
            first_at = float(previous.get("first_at") or now)
            report_sync_pending[key] = {
                "mode": "reconcile",
                "wave": key[0],
                "branch": key[1],
                "summary": None,
                "first_at": first_at,
                "due_at": min(now + max(0.0, delay_seconds), first_at + REPORT_SYNC_MAX_DEBOUNCE_SECONDS),
                "attempts": int(previous.get("attempts") or 0),
            }
    report_sync_wakeup.set()


def queue_wave_totals_reconciliation(waves, delay_seconds: float = REPORT_SYNC_RECONCILE_DELAY_SECONDS):
    """Rebuild every branch in a Wave, used after clearing document overrides."""
    now = time.time()
    with report_sync_pending_lock:
        for wave in waves or []:
            key = _report_sync_key(wave, "*", "reconcile_wave")
            if not key[0]:
                continue
            report_sync_pending[key] = {
                "mode": "reconcile_wave", "wave": key[0], "branch": "*", "summary": None,
                "first_at": now, "due_at": now + max(0.0, delay_seconds), "attempts": 0,
            }
    report_sync_wakeup.set()


def _build_reconciled_summaries(entries: list) -> list:
    summaries = []
    entries_by_wave = {}
    for entry in entries:
        entries_by_wave.setdefault(entry["wave"], []).append(entry)
    for wave, wave_entries in entries_by_wave.items():
        # One fresh Sheet read per Wave, even when many branches changed together.
        fresh = get_wave_data_internal(wave, force_refresh=True)
        fresh = apply_local_overlay(wave, fresh)
        available_branches = sorted({str(item.get("branch") or "").strip().upper()
                                     for item in fresh.get("lpn_list", []) if item.get("branch")})
        requested = set()
        for entry in wave_entries:
            if entry["mode"] == "reconcile_wave":
                requested.update(available_branches)
            else:
                requested.add(entry["branch"])
        for branch in sorted(requested):
            branch_items = [item for item in fresh.get("lpn_list", [])
                            if str(item.get("branch") or "").strip().upper() == branch]
            # Google Sheets เป็นรายงานงานที่ปิดจบแล้วเท่านั้น ห้ามสร้างแถว 0/ยอดระหว่างทำงาน
            if not any(item.get("branch_closed_at") for item in branch_items):
                continue
            # ยอดคำนวณจาก Sheet คือแหล่งจริง แล้วให้ยอดที่ผู้ใช้แก้เองชนะท้ายสุด
            summary = normalize_report_summary(summarize_branch_for_member_data(fresh, branch), wave, branch)
            summary = normalize_report_summary(apply_document_override_to_summary(summary), wave, branch)
            if summary.get("total", 0) > 0 or summary.get("allow_zero_update"):
                summaries.append(summary)
    return summaries


def _requeue_failed_report_entries(entries: list):
    now = time.time()
    with report_sync_pending_lock:
        for entry in entries:
            attempts = int(entry.get("attempts") or 0) + 1
            retry = copy.deepcopy(entry)
            retry["attempts"] = attempts
            retry["first_at"] = now
            retry["due_at"] = now + min(60.0, 3.0 * (2 ** min(attempts, 4)))
            key = _report_sync_key(retry["wave"], retry["branch"], retry["mode"])
            # Never replace a newer change that arrived while this write was running.
            report_sync_pending.setdefault(key, retry)
    report_sync_wakeup.set()


def report_sync_worker_loop():
    """Serialize Sheet writes, batch branches, retry failures, and keep scanner requests fast."""
    while True:
        report_sync_wakeup.wait(timeout=0.5)
        report_sync_wakeup.clear()
        now = time.time()
        due_entries = []
        with report_sync_pending_lock:
            for key, entry in list(report_sync_pending.items()):
                if float(entry.get("due_at") or 0) <= now:
                    due_entries.append(entry)
                    report_sync_pending.pop(key, None)
        if not due_entries:
            continue
        try:
            snapshots = [copy.deepcopy(entry["summary"]) for entry in due_entries
                         if entry["mode"] == "snapshot" and entry.get("summary")]
            reconciles = [entry for entry in due_entries if entry["mode"] != "snapshot"]
            # Snapshot gives users a fast update; reconciliation re-reads the Sheet after.
            if snapshots:
                sync_document_summary_reports(snapshots)
            if reconciles:
                sync_document_summary_reports(_build_reconciled_summaries(reconciles))
            print(f"✅ REPORT SYNC | snapshots={len(snapshots)} reconciles={len(reconciles)}")
        except Exception as exc:
            print(f"🚨 REPORT SYNC RETRY | entries={len(due_entries)} | {exc}")
            _requeue_failed_report_entries(due_entries)


def ensure_report_sync_worker_started():
    global report_sync_worker_started
    with report_sync_worker_start_lock:
        if report_sync_worker_started:
            return
        threading.Thread(target=report_sync_worker_loop, daemon=True, name="report-sync-worker").start()
        report_sync_worker_started = True

def _history_int(value) -> int:
    try:
        return max(0, int(float(str(value or "0").replace(",", "").strip() or 0)))
    except (TypeError, ValueError):
        return 0

_NON_DIGIT_RE = re.compile(r"\D")


def _parse_member_history_csv(csv_text: str) -> dict:
    """Turn the Member Data CSV export into {(wave, branch): summary}."""
    history = {}
    rows = csv.reader(io.StringIO(csv_text))
    next(rows, None)
    for row in rows:
        if len(row) < 16:
            row = list(row) + [""] * (16 - len(row))
        wave_digits = _NON_DIGIT_RE.sub("", row[2])
        branch = str(row[3] or "").strip().upper()
        if not wave_digits or not branch:
            continue
        wave = str(int(wave_digits))
        history[(wave, branch)] = {
            "date": str(row[0] or "").strip(), "time": str(row[1] or "").strip(),
            "wave": wave, "branch": branch, "branch_name": clean_branch_display_name(row[4]),
            "bu": str(row[5] or "").strip() or "Unknown", "label_count": _history_int(row[6]),
            "m": _history_int(row[8]), "red": _history_int(row[9]), "blue": _history_int(row[10]),
            "green": _history_int(row[11]), "black": _history_int(row[12]),
            "total": _history_int(row[13]), "pallet": _history_int(row[14])
        }
    return history


def _store_member_history(history: dict, ttl_seconds: float):
    """Publish a loaded history plus the per-Wave index every read needs.

    Without the index, every Wave lookup scanned all ~39k rows; a Booking with
    Waves did that five times per request.
    """
    by_wave = {}
    for row in history.values():
        by_wave.setdefault(row["wave"], []).append(row)
    with member_history_lock:
        member_history_cache["data"] = history
        member_history_cache["by_wave"] = by_wave
        member_history_cache["expires_at"] = time.time() + ttl_seconds
        member_history_cache["loaded_at"] = time.time()
        member_history_cache["generation"] = int(member_history_cache.get("generation") or 0) + 1
        member_history_items_cache.clear()


def _save_member_history_snapshot(history: dict):
    """Keep a compressed copy on local disk so a worker restart skips the download."""
    try:
        with gzip.open(MEMBER_HISTORY_SNAPSHOT_PATH, "wt", encoding="utf-8", compresslevel=1) as handle:
            json.dump(list(history.values()), handle, ensure_ascii=False, separators=(",", ":"))
    except Exception as exc:
        print(f"⚠️ Member Data snapshot not saved: {exc}")


def _load_member_history_snapshot() -> dict:
    """Read the local snapshot if it is recent enough to serve while refreshing."""
    try:
        age = time.time() - os.path.getmtime(MEMBER_HISTORY_SNAPSHOT_PATH)
        if age > MEMBER_HISTORY_SNAPSHOT_MAX_AGE_SECONDS:
            return {}
        with gzip.open(MEMBER_HISTORY_SNAPSHOT_PATH, "rt", encoding="utf-8") as handle:
            rows = json.load(handle)
        history = {(row["wave"], row["branch"]): row for row in rows
                   if row.get("wave") and row.get("branch")}
        if history:
            print(f"⚡ Member Data restored from local snapshot: {len(history)} rows ({int(age)}s old)")
        return history
    except FileNotFoundError:
        return {}
    except Exception as exc:
        print(f"⚠️ Member Data snapshot unreadable: {exc}")
        return {}


def _refresh_member_history() -> dict:
    """Download and publish Member Data. Only one thread does this at a time.

    The refresh lock also collapses concurrent bursts: whoever arrives second
    finds the cache already fresh and returns that read instead of downloading
    the sheet again.
    """
    global member_history_refreshing
    with member_history_refresh_lock:
        now = time.time()
        with member_history_lock:
            cached = member_history_cache.get("data") or {}
            if cached and member_history_cache.get("expires_at", 0) > now:
                return cached
        url = (
            f"https://docs.google.com/spreadsheets/d/{MEMBER_HISTORY_SPREADSHEET_ID}"
            f"/gviz/tq?tqx=out:csv&gid={MEMBER_HISTORY_GID}"
        )
        try:
            started = time.time()
            history = _parse_member_history_csv(fetch_gviz_text(url, timeout=MEMBER_HISTORY_HTTP_TIMEOUT))
            if not history:
                raise ValueError("Member Data returned no usable rows")
            _store_member_history(history, MEMBER_HISTORY_CACHE_TTL_SECONDS)
            _save_member_history_snapshot(history)
            print(f"✅ Member Data loaded: {len(history)} branch summaries in {time.time() - started:.1f}s")
            return history
        except Exception as exc:
            print(f"⚠️ Member Data load failed, keeping previous rows: {exc}")
            with member_history_lock:
                # Retry soon, but do not hammer Google while it is unhappy.
                member_history_cache["expires_at"] = time.time() + MEMBER_HISTORY_ERROR_BACKOFF_SECONDS
                return member_history_cache.get("data") or {}
        finally:
            with member_history_lock:
                member_history_refreshing = False


def load_member_history(force: bool = False) -> dict:
    """Member Data as {(wave, branch): summary}, served without ever blocking on Google.

    Stale-while-revalidate: an expired cache is returned immediately and refreshed
    in the background. Only a genuinely empty cache waits for the download, and it
    tries the local snapshot first. This is what stops a 10-minute cache expiry from
    turning one unlucky user's search into a 45-second wait.

    force=True is the refresh button and the "data is still arriving" retry: it
    always blocks on a real read, because the caller specifically wants new rows.
    """
    global member_history_refreshing
    if force:
        with member_history_lock:
            member_history_cache["expires_at"] = 0.0
        return _refresh_member_history()
    now = time.time()
    with member_history_lock:
        cached = member_history_cache.get("data") or {}
        fresh = member_history_cache.get("expires_at", 0) > now
        if cached and fresh:
            return cached
        should_refresh = bool(cached) and not member_history_refreshing
        if should_refresh:
            member_history_refreshing = True

    if cached:
        if should_refresh:
            threading.Thread(target=_refresh_member_history, daemon=True,
                             name="member-history-refresh").start()
        return cached

    # Cold cache: a snapshot from this container lets the first request answer now.
    snapshot = _load_member_history_snapshot()
    if snapshot:
        _store_member_history(snapshot, MEMBER_HISTORY_SNAPSHOT_TTL_SECONDS)
        with member_history_lock:
            if not member_history_refreshing:
                member_history_refreshing = True
                threading.Thread(target=_refresh_member_history, daemon=True,
                                 name="member-history-refresh").start()
        return snapshot
    return _refresh_member_history()


def member_history_rows_for_wave(wave: str) -> list:
    """All Member Data rows for one Wave, via the index instead of a full scan."""
    load_member_history()
    with member_history_lock:
        return list((member_history_cache.get("by_wave") or {}).get(str(wave)) or [])


def build_member_history_items(wave_no: str) -> list:
    wave = str(int(str(wave_no).strip()))
    with member_history_lock:
        generation = int(member_history_cache.get("generation") or 0)
    cache_key = (wave, generation)
    cached_items = member_history_items_cache.get(cache_key)
    if cached_items is not None:
        return cached_items
    rows = member_history_rows_for_wave(wave)
    items = []
    for row in rows:
        values = [("M", "None", "Carton", row["m"]), ("RED", "Red", "TOTE", row["red"]),
                  ("BLUE", "Blue", "TOTE", row["blue"]), ("GREEN", "Green", "TOTE", row["green"]),
                  ("BLACK", "Black", "TOTE", row["black"])]
        component_total = sum(qty for _, _, _, qty in values)
        if component_total == 0 and row["total"] > 0:
            values[0] = ("M", "None", "Carton", row["total"])
        pallet_nos = list(range(1, row["pallet"] + 1))
        for category, color, scan_type, qty in values:
            if qty <= 0:
                continue
            items.append({
                "lpn": f"SUMMARY-{wave}-{row['branch']}-{category}", "zone": "HISTORY",
                "branch": row["branch"], "branch_name": row["branch_name"] or row["branch"],
                "status": "Scanned", "total_qty": qty, "qty": qty, "scan_type": scan_type,
                "owner": row["bu"], "color": color,
                "color_breakdown": [{"color": color, "qty": qty, "type": scan_type}],
                "pallet_breakdown": [], "pallet_no": 0, "branch_pallet_nos": pallet_nos,
                "pallet_color": "", "branch_submitted_pallet_nos": pallet_nos,
                "branch_closed_at": f"{row['date']} {row['time']}".strip(), "branch_closed_by": "Member Data",
                "wave_no": f"{int(wave):010d}", "historical_summary": True,
                "historical_label_count": row["label_count"], "historical_date": row["date"]
            })
    # Callers treat these items as read-only, and one Wave is rebuilt several
    # times per request (document build, overlay merge, Booking fan-out).
    # Keyed by history generation so a refresh invalidates it automatically.
    if len(member_history_items_cache) > MEMBER_HISTORY_ITEMS_CACHE_MAX:
        member_history_items_cache.clear()
    member_history_items_cache[cache_key] = items
    return items

def merge_member_history(raw_data: dict, wave_no: str) -> dict:
    history_items = build_member_history_items(wave_no)
    if not history_items:
        return raw_data
    result = copy.deepcopy(raw_data)
    existing = list(result.get("lpn_list") or [])
    history_branches = {item["branch"] for item in history_items}
    live_branches = {
        str(item.get("branch") or "").strip().upper() for item in existing
        if item.get("status") == "Scanned" and not item.get("historical_summary")
    }
    replace_branches = history_branches - live_branches
    existing = [item for item in existing if str(item.get("branch") or "").strip().upper() not in replace_branches]
    existing.extend(item for item in history_items if item["branch"] in replace_branches)
    result["lpn_list"] = existing
    result["historical_summary_source"] = "Member Data"
    return result


def build_uat_wave_data(wave_no: str) -> dict:
    """Build the UAT document model entirely from Member Data + Booking & Wave Sheets."""
    try:
        wave = str(int(str(wave_no).strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ต้องเป็นตัวเลขเท่านั้น")
    items = build_member_history_items(wave)
    meta = get_sheet_meta_for_wave(wave)
    booking = str(meta.get("booking") or "").strip().upper()
    overrides = get_document_overrides_for_wave(wave, booking)
    existing_branches = {str(item.get("branch") or "").strip().upper() for item in items}
    # A manual correction is durable UAT source data too.  Do not make the
    # document disappear (or snap back) merely because Member Data is delayed
    # or temporarily unreadable.
    for branch, override in overrides.items():
        if branch in existing_branches or bool(override.get("is_hidden")):
            continue
        values = [("M", "None", "Carton", "m"), ("RED", "Red", "TOTE", "red"),
                  ("BLUE", "Blue", "TOTE", "blue"), ("GREEN", "Green", "TOTE", "green"),
                  ("BLACK", "Black", "TOTE", "black")]
        for category, color, scan_type, field in values:
            qty = _history_int(override.get(field))
            if qty <= 0:
                continue
            items.append({
                "lpn": f"OVERRIDE-{wave}-{branch}-{category}", "zone": "OVERRIDE",
                "branch": branch,
                "branch_name": str(override.get("branch_name") or branch),
                "status": "Scanned", "total_qty": qty, "qty": qty,
                "scan_type": scan_type, "owner": str(override.get("bu") or "Unknown"),
                "color": color, "color_breakdown": [{"color": color, "qty": qty, "type": scan_type}],
                "pallet_breakdown": [], "pallet_no": 0,
                "branch_pallet_nos": list(range(1, _history_int(override.get("pallet")) + 1)),
                "pallet_color": "", "branch_submitted_pallet_nos": [],
                "branch_closed_at": str(override.get("updated_at") or ""),
                "branch_closed_by": str(override.get("emp_id") or "Manual override"),
                "wave_no": f"{int(wave):010d}", "historical_summary": True,
                "historical_label_count": _history_int(override.get("label_count")),
            })
    if not items:
        raise HTTPException(status_code=404, detail=f"ไม่พบ Wave [{wave}] ใน Member Data")
    return {
        "wave_no": f"{int(wave):010d}",
        "booking_no": str(meta.get("booking") or "").strip().upper(),
        "license_plate": str(meta.get("plate") or "").strip(),
        "carrier": str(meta.get("carrier") or "").strip(),
        "sender": str(meta.get("sender") or "").strip(),
        "lpn_list": items,
        "zone_summary": [],
        "document_overrides": overrides,
        "source": "Google Sheets UAT",
        "scan_feature_enabled": SCAN_FEATURE_ENABLED,
    }

DIRECT_QTY_PREFIXES = ("PP", "SP")

def is_direct_qty_lpn_value(lpn: str) -> bool:
    return str(lpn or "").strip().upper().startswith(DIRECT_QTY_PREFIXES)

# Compiled once: these three run on every one of ~39k Member Data rows, so
# re-compiling them per row was a measurable slice of parse time on 0.1 CPU.
_BRANCH_PREFIX_RE = re.compile(
    r"^\s*(?:ห้างหุ้นส่วนสามัญนิติบุคคล|ห้างหุ้นส่วนจำกัด|หจก\.?|บริษัทจำกัด|บริษัท|บจก\.?)\s*",
    re.IGNORECASE,
)
_BRANCH_SUFFIX_RE = re.compile(
    r"\s*(?:จำกัด\s*\(มหาชน\)|จำกัด|\(มหาชน\)|มหาชน|บจก\.?|หจก\.?)\s*$",
    re.IGNORECASE,
)
_WHITESPACE_RE = re.compile(r"\s+")
_BRANCH_NAME_BLANKS = {"unknown", "null", "none", "-", "ไม่ระบุ", ""}
_branch_name_cache = {}


def clean_branch_display_name(value) -> str:
    """Return a short operational branch name instead of a legal-entity name."""
    name = str(value or "").strip()
    # The same few hundred branch names repeat across tens of thousands of rows,
    # so memoizing turns three regex passes per row into one dict lookup.
    cached = _branch_name_cache.get(name)
    if cached is not None:
        return cached
    if name.lower() in _BRANCH_NAME_BLANKS:
        _branch_name_cache[name] = "Unknown"
        return "Unknown"

    cleaned = _BRANCH_PREFIX_RE.sub("", name)
    cleaned = _BRANCH_SUFFIX_RE.sub("", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip(" -") or "Unknown"
    if len(_branch_name_cache) < 20000:
        _branch_name_cache[name] = cleaned
    return cleaned

PENDING_WAVES_CACHE_TTL_SECONDS = 300
PENDING_WAVES_BOOTSTRAP = [
    {"wave_no": "0000054949"},
    {"wave_no": "0000054978"},
    {"wave_no": "0000055026"},
    {"wave_no": "0000055027"},
    {"wave_no": "0000055031"},
    {"wave_no": "0000055002"},
    {"wave_no": "0000054992"},
]
pending_waves_cache = {
    "data": {"success": True, "waves": PENDING_WAVES_BOOTSTRAP, "cached": True, "bootstrap": True},
    "expires_at": 0
}
pending_waves_cache_lock = Lock()
is_refreshing_pending_waves = False
is_refreshing_pending_waves_lock = Lock()


BOOKING_WAVES_CACHE_TTL = 1800  # 30 นาที cache
BOOKING_FORCE_REFRESH_COOLDOWN_SECONDS = 5.0
booking_waves_cache = {}  # booking_clean -> {"mapping": dict, "expires_at": float, "fetched_at": float}
booking_waves_cache_lock = Lock()
booking_waves_query_locks = {}
booking_waves_query_locks_guard = Lock()
BOOKING_METADATA_CACHE_TTL_SECONDS = 15
booking_assignments_cache = {"data": {}, "expires_at": 0.0}
booking_assignments_cache_lock = Lock()
booking_splits_cache = {"data": {}, "expires_at": 0.0}
booking_splits_cache_lock = Lock()
booking_override_table_ready = False
booking_override_table_lock = Lock()
booking_split_table_ready = False
booking_split_table_lock = Lock()

# Local scans overlay to ensure instant read-after-write across all users.
# Key: wave_clean -> {(lpn_upper, branch_upper): latest scan state including qty/color/pallet_no}
local_scans_overlay = {}
local_scans_lock = Lock()
processed_transaction_ids = {}
processed_transaction_lock = Lock()
device_pending_states = {}
device_pending_states_lock = Lock()
TRANSACTION_TTL_SECONDS = 86400
DEVICE_STATE_TTL_SECONDS = 90
pallet_allocation_lock = Lock()
pallet_counter_cache = {}
pallet_shared_state = {}
pallet_shared_state_lock = Lock()

def record_shared_pallet_state(wave_ids, branch_code: str, pallet_no: int, color: str = "", submitted: bool = False):
    """Keep pallet color/submission visible to every handheld before the Sheet cache catches up."""
    branch = str(branch_code or "").strip().upper()
    no = int(pallet_no or 0)
    if not branch or no <= 0:
        return
    with pallet_shared_state_lock:
        for wave in wave_ids:
            try:
                wave_clean = str(int(str(wave).strip()))
            except ValueError:
                continue
            branch_state = pallet_shared_state.setdefault(wave_clean, {}).setdefault(branch, {
                "colors": {},
                "submitted": set(),
                "closed_at": "",
                "closed_by": "",
                "updated_at": 0.0,
            })
            if color:
                branch_state["colors"][no] = str(color).strip().title()
            if submitted:
                branch_state["submitted"].add(no)
            branch_state["updated_at"] = time.time()

def record_shared_branch_closed(wave_no: str, branch_code: str, completed_at: str, emp_id: str):
    try:
        wave_clean = str(int(str(wave_no).strip()))
    except ValueError:
        return
    branch = str(branch_code or "").strip().upper()
    if not branch:
        return
    with pallet_shared_state_lock:
        branch_state = pallet_shared_state.setdefault(wave_clean, {}).setdefault(branch, {
            "colors": {}, "submitted": set(), "closed_at": "", "closed_by": "", "updated_at": 0.0
        })
        branch_state["closed_at"] = completed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
        branch_state["closed_by"] = str(emp_id or "").strip()
        branch_state["updated_at"] = time.time()

def record_local_scan(wave_no: str, lpn: str, branch_code: str, qty: int, scan_type: str, color: str, pallet_no: int = 0, base_pallet_breakdown=None):
    try:
        wave_clean = str(int(wave_no.strip()))
    except ValueError:
        return

    lpn_upper = lpn.strip().upper()
    branch_upper = branch_code.strip().upper()

    status = "Pending"
    if qty == 0 or scan_type in ("RESET_BOX", "CANCEL_COMBINE"):
        status = "Pending"
        qty = 0
    else:
        status = "Scanned"

    with local_scans_lock:
        if wave_clean not in local_scans_overlay:
            local_scans_overlay[wave_clean] = {}

        lpn_key = (lpn_upper, branch_upper)
        if status == "Pending":
            local_scans_overlay[wave_clean][lpn_key] = {
                "qty": 0,
                "scan_type": scan_type,
                "color": color,
                "color_breakdown": [],
                "pallet_breakdown": [],
                "status": status,
                "pallet_no": 0,
                "timestamp": time.time()
            }
            return

        current = local_scans_overlay[wave_clean].get(lpn_key, {})
        pallet_parts = list(current.get("pallet_breakdown") or base_pallet_breakdown or [])
        if not pallet_parts and int(current.get("qty", 0) or 0) > 0:
            pallet_parts = [{"pallet_no": int(current.get("pallet_no", 0) or 0), "color": current.get("color") or "None", "qty": int(current.get("qty") or 0), "type": current.get("scan_type") or "TOTE"}]
        part_key = (int(pallet_no or 0), color.upper())
        pallet_map = {(int(part.get("pallet_no", 0) or 0), str(part.get("color") or "None").upper()): part for part in pallet_parts}
        
        if (scan_type == "Carton" or is_direct_qty_lpn_value(lpn_upper)) and part_key in pallet_map:
            prev_pallet_qty = int(pallet_map[part_key].get("qty", 0) or 0)
            pallet_map[part_key] = {"pallet_no": int(pallet_no or 0), "color": color, "qty": prev_pallet_qty + qty, "type": scan_type}
        else:
            pallet_map[part_key] = {"pallet_no": int(pallet_no or 0), "color": color, "qty": qty, "type": scan_type}

        pallet_breakdown = list(pallet_map.values())
        color_map = {}
        for part in pallet_breakdown:
            color_key = str(part.get("color") or "None").upper()
            aggregate = color_map.setdefault(color_key, {"color": part.get("color") or "None", "qty": 0, "type": part.get("type") or "TOTE"})
            aggregate["qty"] += int(part.get("qty", 0) or 0)
        color_breakdown = list(color_map.values())
        total_qty = sum(int(part.get("qty", 0) or 0) for part in pallet_breakdown)

        local_scans_overlay[wave_clean][lpn_key] = {
            "qty": total_qty,
            "scan_type": scan_type if len(color_breakdown) == 1 else "TOTE_MULTI",
            "color": color if len(color_breakdown) == 1 else "Multiple",
            "color_breakdown": color_breakdown,
            "pallet_breakdown": pallet_breakdown,
            "status": status,
            "pallet_no": int(pallet_no or 0),
            "timestamp": time.time()
        }

def apply_local_overlay(wave_detail_str: str, raw_data: dict) -> dict:
    try:
        search_wave_id = int(wave_detail_str.strip())
        wave_clean = str(search_wave_id)
        wave_detail_str = f"{search_wave_id:010d}"
    except ValueError:
        return raw_data

    with local_scans_lock:
        scans_copy = dict(local_scans_overlay.get(wave_clean) or {})
    with pallet_shared_state_lock:
        shared_pallet_copy = copy.deepcopy(pallet_shared_state.get(wave_clean) or {})
    if not scans_copy and not shared_pallet_copy:
        return raw_data

    data = copy.deepcopy(raw_data)
    lpn_list = data.get("lpn_list", [])

    for item in lpn_list:
        lpn_key = (item["lpn"].strip().upper(), item["branch"].strip().upper())
        if lpn_key in scans_copy:
            scan_info = scans_copy[lpn_key]
            item["status"] = scan_info["status"]
            item["qty"] = scan_info["qty"]
            item["scan_type"] = scan_info["scan_type"]
            item["color"] = scan_info["color"]
            item["color_breakdown"] = scan_info.get("color_breakdown", [])
            item["pallet_breakdown"] = scan_info.get("pallet_breakdown", [])
            item["pallet_no"] = int(scan_info.get("pallet_no", 0) or 0)

        branch = str(item.get("branch") or "").strip().upper()
        pallet_state = shared_pallet_copy.get(branch) or {}
        pallet_no = int(item.get("pallet_no") or 0)
        shared_color = (pallet_state.get("colors") or {}).get(pallet_no)
        if shared_color:
            item["pallet_color"] = shared_color
        shared_submitted = set(item.get("branch_submitted_pallet_nos") or [])
        shared_submitted.update(pallet_state.get("submitted") or set())
        item["branch_submitted_pallet_nos"] = sorted(int(no) for no in shared_submitted if int(no) > 0)
        if pallet_state.get("closed_at"):
            item["branch_closed_at"] = pallet_state["closed_at"]
            item["branch_closed_by"] = pallet_state.get("closed_by") or ""

    # Recalculate zone_summary
    zones_calc = {}
    for item in lpn_list:
        z = item.get("zone") or "N/A"
        if z not in zones_calc:
            zones_calc[z] = {"zone": z, "scanned": 0, "total": 0}
        zones_calc[z]["total"] += 1
        if item["status"] == "Scanned":
            zones_calc[z]["scanned"] += 1

    data["zone_summary"] = list(zones_calc.values())
    return data

def get_wave_data_internal(wave_no: str, force_refresh: bool = False) -> dict:
    """Build one Wave's document model from the Sheets caches.

    force_refresh expires the two source caches so the next read reloads them;
    build_uat_wave_data() then works from freshly loaded rows.
    """
    if force_refresh:
        # force must actually re-read: load_member_history() alone would return the
        # stale rows and refresh in background, which is wrong for a refresh button.
        load_member_history(force=True)
        load_booking_wave_sheet_meta(force=True)
    return build_uat_wave_data(wave_no)


def fetch_booking_waves(booking_no: str) -> dict:
    """Resolve a Booking to its Waves and transport details from the Booking & Wave sheet.

    The cached sheet answers first. A forced re-read costs a full gviz download,
    so it is worth paying only when the Booking is genuinely missing — typically a
    row added minutes ago that the 10-minute cache has not picked up yet.
    """
    sheet_meta = get_sheet_meta_for_booking(booking_no)
    if not sheet_meta or not sheet_meta.get("waves"):
        sheet_meta = get_sheet_meta_for_booking(booking_no, force=True)
    if not sheet_meta or not sheet_meta.get("waves"):
        raise HTTPException(status_code=404, detail=f"ไม่พบ Booking [{booking_no}] ใน Sheet Booking & Wave")
    return {
        "waves": list(sheet_meta.get("waves") or []),
        "license_plate": str(sheet_meta.get("plate") or ""),
        "carrier": str(sheet_meta.get("carrier") or ""),
        "sender": str(sheet_meta.get("sender") or ""),
    }

def get_booking_waves_mapping(booking_no: str, force_refresh: bool = False) -> dict:
    clean_booking = booking_no.strip().upper()
    now = time.time()
    with booking_waves_cache_lock:
        cached = booking_waves_cache.get(clean_booking)
        if cached:
            cache_fresh = float(cached.get("expires_at") or 0) > now
            fetched_recently = now - float(cached.get("fetched_at") or 0) < BOOKING_FORCE_REFRESH_COOLDOWN_SECONDS
            if (not force_refresh and cache_fresh) or (force_refresh and cache_fresh and fetched_recently):
                return cached["mapping"]
                
    with booking_waves_query_locks_guard:
        query_lock = booking_waves_query_locks.setdefault(clean_booking, Lock())
    with query_lock:
        with booking_waves_cache_lock:
            cached = booking_waves_cache.get(clean_booking)
            current_time = time.time()
            if cached:
                cache_fresh = float(cached.get("expires_at") or 0) > current_time
                fetched_recently = current_time - float(cached.get("fetched_at") or 0) < BOOKING_FORCE_REFRESH_COOLDOWN_SECONDS
                if (not force_refresh and cache_fresh) or (force_refresh and cache_fresh and fetched_recently):
                    return cached["mapping"]
        mapping = fetch_booking_waves(booking_no)
        fetched_at = time.time()
        with booking_waves_cache_lock:
            booking_waves_cache[clean_booking] = {
                "mapping": mapping,
                "expires_at": fetched_at + BOOKING_WAVES_CACHE_TTL,
                "fetched_at": fetched_at,
            }
        return mapping

def get_booking_data_internal(booking_no: str, force_refresh: bool = False) -> dict:
    mapping = get_booking_waves_mapping(booking_no, force_refresh)
    booking_clean = booking_no.strip().upper()
    wave_force_refresh = force_refresh
    if force_refresh:
        # Refresh the large Member Data sheet once before fan-out. Refreshing
        # per Wave is slower and can expose different snapshots of the same read
        # while the Sheet is still being written.
        load_member_history(force=True)
        load_booking_wave_sheet_meta(force=True)
        wave_force_refresh = False
    assignments = get_booking_branch_assignments(force_refresh=force_refresh)
    splits = get_booking_branch_splits(force_refresh=force_refresh)
    override_waves = [wave for (wave, branch), move in assignments.items()
                      if str(move.get("Assigned_Booking") or "").strip().upper() == booking_clean]
    split_waves = [wave for (wave, branch, target), split in splits.items()
                   if target == booking_clean or str(split.get("Source_Booking") or "").strip().upper() == booking_clean]
    waves = list(dict.fromkeys(
        str(int(re.sub(r"\D", "", str(wave)))) for wave in (list(mapping["waves"]) + override_waves + split_waves)
        if re.sub(r"\D", "", str(wave or ""))
    ))
    license_plate = mapping["license_plate"]
    native_waves = {
        str(int(re.sub(r"\D", "", str(wave))))
        for wave in (mapping.get("waves") or [])
        if re.sub(r"\D", "", str(wave or ""))
    }
    
    lpn_list = []
    waves_included = set()
    wave_results = []
    
    # Every Wave is now built from the in-memory Sheets caches, so this loop is
    # CPU-bound under the GIL: a thread pool added contention and peak RAM on a
    # 0.1 CPU instance without overlapping any real I/O.
    for wave in waves:
        try:
            wave_data = get_wave_data_internal(wave, wave_force_refresh)
            wave_results.append(merge_member_history(apply_local_overlay(wave, wave_data), wave))
        except HTTPException as e:
            # A stale/failed branch-move record can reference a foreign Wave
            # that is not part of this Booking's real mapping. It must not
            # make confirmation of the newly selected Wave fail with the
            # unrelated old Wave number.
            if wave not in native_waves and e.status_code == 404:
                print(f"⚠️ Ignoring unavailable transferred Wave {wave} while loading Booking {booking_clean}")
                continue
            raise
        except Exception as e:
            print(f"🚨 Error fetching wave {wave} in booking {booking_no}: {e}")
            raise

    for wave_data_overlaid in wave_results:
        wave_key = str(int(str(wave_data_overlaid["wave_no"]).strip()))
        native_booking = str(wave_data_overlaid.get("booking_no") or "").strip().upper()
        is_native_wave = (wave_key in native_waves) or (bool(native_booking) and native_booking == booking_clean)
        items_by_branch = {}
        for item in wave_data_overlaid.get("lpn_list", []):
            items_by_branch.setdefault(str(item.get("branch") or "").strip().upper(), []).append(item)
        wave_has_items = False
        for branch, branch_items in items_by_branch.items():
            assignment = assignments.get((wave_key, branch))
            assigned_booking = str((assignment or {}).get("Assigned_Booking") or "").strip().upper()
            branch_splits = [split for (split_wave, split_branch, target), split in splits.items()
                             if split_wave == wave_key and split_branch == branch and bool(split.get("Is_Active", True))]
            if assigned_booking:
                if assigned_booking != booking_clean:
                    continue
                for item in branch_items:
                    item = copy.deepcopy(item)
                    item["booking_override"] = {
                        "previous_booking": assignment.get("Previous_Booking") or native_booking,
                        "assigned_booking": assigned_booking,
                        "reason": assignment.get("Reason") or "",
                        "emp_id": assignment.get("Emp_ID") or "",
                        "created_at": (
                            assignment.get("Created_At").isoformat()
                            if assignment.get("Created_At") and hasattr(assignment.get("Created_At"), "isoformat")
                            else str(assignment.get("Created_At") or "")
                        )
                    }
                    lpn_list.append(item)
                    wave_has_items = True
                continue

            if branch_splits:
                base_summary = summarize_branch_for_member_data(
                    {"wave_no": wave_key, "booking_no": native_booking, "lpn_list": branch_items}, branch
                )
                fields = ("m", "red", "blue", "green", "black", "pallet")
                split_columns = {"m": "M_Count", "red": "Red_Count", "blue": "Blue_Count",
                                 "green": "Green_Count", "black": "Black_Count", "pallet": "Pallet_Count"}
                outgoing = {field: sum(int(split.get(split_columns[field]) or 0) for split in branch_splits)
                            for field in fields}
                target_split = next((split for split in branch_splits
                                     if str(split.get("Target_Booking") or "").strip().upper() == booking_clean), None)

                if is_native_wave:
                    visible_totals = {field: max(0, int(base_summary.get(field) or 0) - outgoing[field]) for field in fields}
                    for item in branch_items:
                        item = copy.deepcopy(item)
                        item["booking_split_summary"] = visible_totals
                        item["booking_split_outgoing"] = True
                        lpn_list.append(item)
                        wave_has_items = True
                elif target_split:
                    visible_totals = {field: max(0, int(target_split.get(split_columns[field]) or 0)) for field in fields}
                    first = copy.deepcopy(branch_items[0])
                    first.update({
                        "lpn": f"แบ่งยอดจาก {native_booking}", "zone": "TRANSFER", "status": "Scanned",
                        "qty": sum(visible_totals[field] for field in ("m", "red", "blue", "green", "black")),
                        "total_qty": sum(visible_totals[field] for field in ("m", "red", "blue", "green", "black")),
                        "scan_type": "BOOKING_SPLIT", "color": "None", "historical_summary": True,
                        "booking_split_summary": visible_totals,
                        "booking_split_source": native_booking,
                        "booking_split_target": booking_clean,
                    })
                    lpn_list.append(first)
                    wave_has_items = True
            else:
                # สาขาที่ไม่มีการย้ายหรือแบ่งยอด จะต้องอยู่ใน Booking นี้เฉพาะเมื่อ Wave นี้เป็น Wave ดั้งเดิมของ Booking เท่านั้น
                # ป้องกันไม่ให้ Wave อื่นที่ถูกดึงเข้ามาเพราะย้ายแค่บางสาขา เอาสาขาอื่นทั้งหมดใน Wave นั้นติดมาด้วย
                if is_native_wave:
                    for item in branch_items:
                        lpn_list.append(item)
                        wave_has_items = True
        if wave_has_items:
            waves_included.add(wave_data_overlaid["wave_no"])
        
    zones_calc = {}
    for item in lpn_list:
        z = item.get("zone") or "N/A"
        if z not in zones_calc:
            zones_calc[z] = {"zone": z, "scanned": 0, "total": 0}
        zones_calc[z]["total"] += 1
        if item["status"] == "Scanned":
            zones_calc[z]["scanned"] += 1
            
    combined_overrides = {}
    for wave_no_inc in waves_included:
        clean_w = re.sub(r"\D", "", str(wave_no_inc))
        if clean_w:
            combined_overrides.update(get_document_overrides_for_wave(clean_w, booking_clean))

    return {
        "booking_no": booking_no,
        "license_plate": license_plate,
        "carrier": str(mapping.get("carrier") or ""),
        "sender": str(mapping.get("sender") or ""),
        "waves": list(waves_included),
        "lpn_list": lpn_list,
        "zone_summary": list(zones_calc.values()),
        "document_overrides": combined_overrides
    }


# ==================== DATA MODELS ====================
class ScanData(BaseModel):
    wave_no: str
    branch_code: Optional[str] = None
    branch_name: Optional[str] = None
    lpn: str
    type: str
    color: str
    qty: int = 1
    emp_id: Optional[str] = None
    pallet_no: Optional[int] = 0   # ✅ เลขพาเลทที่ LPN นี้อยู่ (sync ข้ามเครื่อง)
    expected_previous_qty: Optional[int] = None  # optimistic concurrency: กันหลายเครื่องเขียน LPN เดียวกันทับกัน
    transaction_id: Optional[str] = None  # idempotency key จากเครื่องสแกน ป้องกัน request สำเร็จแต่ response หลุดแล้วบันทึกซ้ำ

ScanData.model_rebuild()   # ← เพิ่มบรรทัดนี้

class ScanBatchData(BaseModel):
    scans: List[ScanData]

ScanBatchData.model_rebuild()

class DeviceStateData(BaseModel):
    device_id: str
    waves: List[str] = []
    branch_code: Optional[str] = None
    pending_count: int = 0
    pending_lpns: List[str] = []
    emp_id: Optional[str] = None


class CombineData(BaseModel):
    master_lpn: str
    child_lpns: List[str]

class CloseData(BaseModel):
    wave_no: str
    branch_code: str = "ALL"
    close_type: str

class PalletStartData(BaseModel):
    waves: List[str]
    booking_no: Optional[str] = None
    branch_code: str
    branch_name: Optional[str] = None
    color: str
    emp_id: Optional[str] = None

class PalletSubmitData(BaseModel):
    waves: List[str]
    booking_no: Optional[str] = None
    branch_code: str
    branch_name: Optional[str] = None
    pallet_no: int
    color: Optional[str] = None
    emp_id: Optional[str] = None

class CorrectionData(BaseModel):
    correction_id: Optional[str] = None
    wave_no: str
    branch_code: str
    branch_name: Optional[str] = None
    lpn: str
    new_qty: int
    reason: str
    note: Optional[str] = None
    emp_id: Optional[str] = None
    pallet_no: Optional[int] = 0
    scan_type: Optional[str] = "Carton"
    color: Optional[str] = "None"

class BookingBranchMoveData(BaseModel):
    target_booking: str
    wave_no: str
    branch_code: str
    reason: str
    note: Optional[str] = None
    emp_id: str

class BookingBranchSplitData(BookingBranchMoveData):
    m: int = 0
    red: int = 0
    blue: int = 0
    green: int = 0
    black: int = 0
    pallet: int = 0

class DocumentSummaryData(BaseModel):
    wave: str
    booking: Optional[str] = None
    branch: str
    branch_name: Optional[str] = None
    bu: Optional[str] = None
    label_count: int = 0
    m: int = 0
    red: int = 0
    blue: int = 0
    green: int = 0
    black: int = 0
    total: int = 0
    pallet: int = 0
    is_hidden: Optional[bool] = False
    is_closed: Optional[bool] = False
    booking_split: Optional[bool] = False

class DocumentSummaryBatchData(BaseModel):
    summaries: List[DocumentSummaryData]
    emp_id: Optional[str] = None
    # False = snapshot ยอดอัตโนมัติจากหน้าจอ (ใช้ซิงก์รายงานเท่านั้น)
    # True = ผู้ใช้แก้ยอด/ซ่อนสาขาเอง จึงค่อยบันทึกเป็น override ถาวร
    persist_overrides: bool = False
    reason: Optional[str] = None

class ResetDocumentOverridesData(BaseModel):
    wave: Optional[str] = None
    booking: Optional[str] = None
    emp_id: Optional[str] = None

class UsageEventData(BaseModel):
    event_id: Optional[str] = None
    event_type: str
    emp_id: Optional[str] = None
    emp_name: Optional[str] = None
    wave: Optional[str] = None
    booking: Optional[str] = None
    branch: Optional[str] = None
    status: Optional[str] = "SUCCESS"
    duration_ms: Optional[int] = 0
    detail: Optional[str] = None
    client_time: Optional[str] = None

class CloseJobData(BaseModel):
    wave: str
    branch: str
    emp_id: Optional[str] = None
    completed_at: Optional[str] = None
    summary: Optional[DocumentSummaryData] = None

def ensure_booking_override_table():
    global booking_override_table_ready
    if booking_override_table_ready:
        return
    with booking_override_table_lock:
        if booking_override_table_ready:
            return
        ensure_uat_event_sheets()
        booking_override_table_ready = True

def get_booking_branch_assignments(force_refresh: bool = False) -> dict:
    now = time.time()
    with booking_assignments_cache_lock:
        if not force_refresh and booking_assignments_cache["expires_at"] > now:
            return {key: dict(row) for key, row in booking_assignments_cache["data"].items()}
        try:
            ensure_booking_override_table()
            rows = read_uat_event_records("Booking Branch Moves", force=force_refresh)
            data = {}
            for row in reversed(rows):
                key = (str(row.get("Wave_Number") or "").strip(), str(row.get("Branch_Code") or "").strip().upper())
                if all(key) and key not in data:
                    data[key] = row
            booking_assignments_cache.update({
                "data": data,
                "expires_at": time.time() + BOOKING_METADATA_CACHE_TTL_SECONDS,
            })
            return {key: dict(row) for key, row in data.items()}
        except Exception as exc:
            print(f"BOOKING OVERRIDE READ ERROR: {exc}")
            return {key: dict(row) for key, row in booking_assignments_cache["data"].items()}

def ensure_booking_split_table():
    global booking_split_table_ready
    if booking_split_table_ready:
        return
    with booking_split_table_lock:
        if booking_split_table_ready:
            return
        ensure_uat_event_sheets()
        booking_split_table_ready = True

def get_booking_branch_splits(force_refresh: bool = False) -> dict:
    now = time.time()
    with booking_splits_cache_lock:
        if not force_refresh and booking_splits_cache["expires_at"] > now:
            return {key: dict(row) for key, row in booking_splits_cache["data"].items()}
        try:
            ensure_booking_split_table()
            rows = read_uat_event_records("Booking Branch Splits", force=force_refresh)
            data = {}
            for row in reversed(rows):
                key = (str(row.get("Wave_Number") or "").strip(), str(row.get("Branch_Code") or "").strip().upper(),
                       str(row.get("Target_Booking") or "").strip().upper())
                if all(key) and key not in data:
                    data[key] = row
            booking_splits_cache.update({
                "data": data,
                "expires_at": time.time() + BOOKING_METADATA_CACHE_TTL_SECONDS,
            })
            return {key: dict(row) for key, row in data.items()}
        except Exception as exc:
            print(f"BOOKING SPLIT READ ERROR: {exc}")
            return {key: dict(row) for key, row in booking_splits_cache["data"].items()}


def persist_target_booking_split_edits(summaries: list, emp_id: str, reason: str = "") -> list:
    """Make manual edits to a split target authoritative for both booking views."""
    splits = get_booking_branch_splits(force_refresh=True)
    rows = []
    affected_sources = []
    now_iso = _uat_now_iso()
    for item in summaries or []:
        if not item.get("booking_split"):
            continue
        key = (str(item.get("wave") or ""), str(item.get("branch") or "").strip().upper(),
               str(item.get("booking") or "").strip().upper())
        current = splits.get(key)
        if not current or not bool(current.get("Is_Active", True)):
            # The native/source side also carries booking_split_summary. Only
            # a target allocation row may be edited directly here.
            continue
        source = str(current.get("Source_Booking") or "").strip().upper()
        rows.append({
            "Event_ID": str(uuid.uuid4()), "Wave_Number": key[0], "Branch_Code": key[1],
            "Source_Booking": source, "Target_Booking": key[2],
            "M_Count": item["m"], "Red_Count": item["red"], "Blue_Count": item["blue"],
            "Green_Count": item["green"], "Black_Count": item["black"],
            "Pallet_Count": item["pallet"], "Is_Active": True,
            "Reason": str(reason or "MANUAL_TOTAL_SAVE").strip(),
            "Note": "Updated from UAT shipping document", "Emp_ID": str(emp_id or "").strip(),
            "Created_At": now_iso,
        })
        affected_sources.append((source, key[0], key[1]))
    if rows:
        append_uat_event_rows("Booking Branch Splits", rows)
        with booking_splits_cache_lock:
            booking_splits_cache["expires_at"] = 0.0
        with booking_waves_cache_lock:
            for source, _, _ in affected_sources:
                booking_waves_cache.pop(source, None)
            for item in rows:
                booking_waves_cache.pop(item["Target_Booking"], None)
    return affected_sources

# ==================== ROUTES & APIs ====================

@app.get("/")
async def read_root():
    return {
        "status": "ok",
        "message": "Scanner API UAT is running",
        "environment": APP_ENV,
        "data_source": "google_sheets",
        "legacy_transport_workbook": "read_only",
        "scan_feature_enabled": SCAN_FEATURE_ENABLED,
        "scan_demo_only": SCAN_DEMO_ONLY,
    }

# ✅ Health Check Endpoint: ตอบสนองเร็ว <5ms สำหรับ keep-alive heartbeat
@app.get("/api/health")
async def health_check(response: Response, deep: bool = False):
    """Cheap by default so the keep-alive ping costs nothing; ?deep=1 verifies Google auth."""
    response.headers["Cache-Control"] = "no-store"
    response.headers["Connection"] = "keep-alive"
    if os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip():
        creds_source = "GOOGLE_SERVICE_ACCOUNT_JSON"
    elif os.path.exists(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")):
        creds_source = f"file:{os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}"
    else:
        creds_source = "default_adc"
    creds_ok = None
    creds_error = None
    if deep:
        # Building the session touches the token endpoint, so only do it on request.
        try:
            get_sheets_session()
            creds_ok = True
        except Exception as e:
            creds_ok = False
            creds_error = str(e)[:200]
    with member_history_lock:
        history_rows = len(member_history_cache.get("data") or {})
        history_fresh = member_history_cache.get("expires_at", 0) > time.time()
    return {
        "status": "ok", "version": APP_VERSION, "timestamp": time.time(),
        "environment": APP_ENV,
        "data_source": "google_sheets",
        "legacy_transport_workbook": "read_only",
        "scan_feature_enabled": SCAN_FEATURE_ENABLED,
        "uptime_seconds": int(time.time() - PROCESS_STARTED_AT),
        "member_data": {"rows": history_rows, "fresh": history_fresh},
        "google_credentials": {
            "source": creds_source,
            "ok": creds_ok,
            "error": creds_error,
            "checked": bool(deep),
        },
    }


@app.get("/api/test-sheets-write")
def test_sheets_write():
    """ทดสอบการเขียน Google Sheets จริงๆ — ใช้สำหรับ debug เท่านั้น"""
    results = {}
    # ทดสอบ auth
    try:
        session = get_sheets_session()
        results["auth"] = "ok"
    except Exception as e:
        results["auth"] = f"FAIL: {e}"
        return {"success": False, "results": results}
    # ทดสอบ read Member Data
    try:
        lookup_range = urllib.parse.quote("Member Data!A1:D3", safe="")
        read_res = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{MEMBER_HISTORY_SPREADSHEET_ID}/values/{lookup_range}",
            timeout=SHEETS_HTTP_TIMEOUT
        )
        read_res.raise_for_status()
        results["member_data_read"] = f"ok ({len(read_res.json().get('values', []))} rows)"
    except Exception as e:
        results["member_data_read"] = f"FAIL: {e}"
    # ทดสอบ read UAT Report
    try:
        lookup_range = urllib.parse.quote(f"'{UAT_REPORT_TEST_SHEET_NAME}'!A1:D3", safe="")
        read_res = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{UAT_REPORT_TEST_SPREADSHEET_ID}/values/{lookup_range}",
            timeout=SHEETS_HTTP_TIMEOUT
        )
        read_res.raise_for_status()
        results["uat_report_read"] = f"ok ({len(read_res.json().get('values', []))} rows)"
    except Exception as e:
        results["uat_report_read"] = f"FAIL: {e}"
    return {
        "success": all("FAIL" not in str(v) for v in results.values()),
        "spreadsheets": {
            "member_data": f"https://docs.google.com/spreadsheets/d/{MEMBER_HISTORY_SPREADSHEET_ID}",
            "uat_report": f"https://docs.google.com/spreadsheets/d/{UAT_REPORT_TEST_SPREADSHEET_ID}",
        },
        "results": results
    }



@app.get("/api/branch-provinces")
def get_branch_provinces(force: bool = False):
    """Serve the branch master using authenticated Google Sheets access."""
    session = get_sheets_session()
    province_map = load_branch_province_map(session, force=force)
    return {
        "success": True,
        "source": BRANCH_MASTER_SHEET_NAME,
        "count": len(province_map),
        "branches": province_map,
    }


USAGE_EVENT_TYPES = {
    "LOGIN", "LOGOUT", "SEARCH", "OPEN_DOCUMENT", "PRINT_DOCUMENT",
    "SAVE_DOCUMENT", "VIEW_DASHBOARD"
}


def _parse_event_datetime(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=7)))
        return parsed.astimezone(datetime.timezone.utc)
    except (TypeError, ValueError):
        return None


def _parse_report_date(value):
    raw = str(value or "").strip().split(" ")[0]
    for pattern in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            parsed = datetime.datetime.strptime(raw, pattern).date()
            if parsed.year > 2400:
                parsed = parsed.replace(year=parsed.year - 543)
            return parsed
        except ValueError:
            continue
    return None


def load_dashboard_operations_rows() -> list:
    """Read the isolated UAT Delivery report at most once every five minutes."""
    now = time.time()
    if dashboard_operations_cache["expires_at"] > now:
        return copy.deepcopy(dashboard_operations_cache["rows"])
    with dashboard_operations_lock:
        now = time.time()
        if dashboard_operations_cache["expires_at"] > now:
            return copy.deepcopy(dashboard_operations_cache["rows"])
        values = _sheet_values(
            get_sheets_session(), UAT_REPORT_TEST_SPREADSHEET_ID,
            f"'{UAT_REPORT_TEST_SHEET_NAME}'!C:T"
        )
        rows = []
        for raw in values[1:]:
            row = list(raw) + [""] * max(0, 18 - len(raw))
            pick_date = _parse_report_date(row[0])
            if not pick_date:
                continue
            numbers = []
            for value in row[12:18]:
                try:
                    numbers.append(max(0, int(float(str(value or 0).replace(",", "")))))
                except (TypeError, ValueError):
                    numbers.append(0)
            rows.append({
                "date": pick_date.isoformat(), "booking": str(row[5] or "0").strip() or "0",
                "wave": str(row[7] or "").strip(), "branch": str(row[8] or "").strip(),
                "m": numbers[0], "red": numbers[1], "blue": numbers[2],
                "green": numbers[3], "black": numbers[4], "total": numbers[5],
            })
        dashboard_operations_cache.update({"expires_at": time.time() + 300, "rows": rows})
        return copy.deepcopy(rows)


def _dashboard_reference_matches(row: dict, query: str) -> bool:
    """Match a Booking, Wave, or branch code without issuing another Sheets read."""
    if not query:
        return True
    return any(query in str(row.get(field) or "").upper()
               for field in ("booking", "wave", "branch"))


@app.post("/api/usage-event")
def record_usage_event(data: UsageEventData, background_tasks: BackgroundTasks):
    """Queue one lightweight UAT usage event; never block the user's document flow."""
    event_type = re.sub(r"[^A-Z_]", "", str(data.event_type or "").strip().upper())
    if event_type not in USAGE_EVENT_TYPES:
        raise HTTPException(status_code=400, detail="unsupported event_type")
    status = re.sub(r"[^A-Z_]", "", str(data.status or "SUCCESS").strip().upper())[:20] or "SUCCESS"
    row = {
        "Event_ID": str(data.event_id or uuid.uuid4())[:80],
        "Event_Type": event_type,
        "Emp_ID": str(data.emp_id or "").strip()[:40],
        "Emp_Name": str(data.emp_name or "").strip()[:120],
        "Wave_Number": str(data.wave or "").strip()[:120],
        "Booking_No": str(data.booking or "").strip().upper()[:120],
        "Branch_Code": str(data.branch or "").strip().upper()[:40],
        "Status": status,
        "Duration_Ms": max(0, min(int(data.duration_ms or 0), 600000)),
        "Detail": str(data.detail or "").strip()[:240],
        "Client_Time": str(data.client_time or "").strip()[:60],
        "Created_At": _uat_now_iso(),
    }
    background_tasks.add_task(append_uat_event_rows, "Usage Events", [row])
    return {"status": "queued", "event_id": row["Event_ID"]}


@app.get("/api/dashboard")
def get_usage_dashboard(response: Response, days: int = 7, q: str = "", employee: str = "",
                        date_from: str = "", date_to: str = ""):
    """Aggregate UAT usage without exposing raw Sheet access to the browser."""
    days = max(1, min(int(days or 7), 30))
    query = re.sub(r"\s+", " ", str(q or "").strip()).upper()[:120]
    employee_filter = str(employee or "").strip().upper()[:40]
    started = time.perf_counter()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    bangkok_tz = datetime.timezone(datetime.timedelta(hours=7))
    today_bkk = now_utc.astimezone(bangkok_tz).date()
    requested_from = _parse_report_date(date_from) if date_from else None
    requested_to = _parse_report_date(date_to) if date_to else None
    if date_from and not requested_from:
        raise HTTPException(status_code=400, detail="date_from must be YYYY-MM-DD")
    if date_to and not requested_to:
        raise HTTPException(status_code=400, detail="date_to must be YYYY-MM-DD")
    range_to = requested_to or today_bkk
    range_from = requested_from or (range_to - datetime.timedelta(days=days - 1))
    if range_from > range_to:
        raise HTTPException(status_code=400, detail="date_from must not be after date_to")
    if (range_to - range_from).days >= 366:
        raise HTTPException(status_code=400, detail="date range must not exceed 366 days")
    period_days = (range_to - range_from).days + 1
    records = read_uat_event_records("Usage Events")
    events = []
    employee_options_map = {}
    for row in records:
        created = _parse_event_datetime(row.get("Created_At"))
        created_day = created.astimezone(bangkok_tz).date() if created else None
        if not created_day or created_day < range_from or created_day > range_to:
            continue
        try:
            duration = max(0, int(float(row.get("Duration_Ms") or 0)))
        except (TypeError, ValueError):
            duration = 0
        event = {
            "event_type": str(row.get("Event_Type") or "").strip().upper(),
            "emp_id": str(row.get("Emp_ID") or "").strip(),
            "emp_name": str(row.get("Emp_Name") or "").strip(),
            "wave": str(row.get("Wave_Number") or "").strip(),
            "booking": str(row.get("Booking_No") or "").strip(),
            "branch": str(row.get("Branch_Code") or "").strip(),
            "status": str(row.get("Status") or "SUCCESS").strip().upper(),
            "duration_ms": duration,
            "detail": str(row.get("Detail") or "").strip(),
            "created_at": created.isoformat(),
        }
        events.append(event)
        employee_key = event["emp_id"].upper()
        if employee_key:
            option = employee_options_map.setdefault(employee_key, {
                "emp_id": event["emp_id"], "emp_name": event["emp_name"],
            })
            if event["emp_name"]:
                option["emp_name"] = event["emp_name"]

    employee_options = sorted(employee_options_map.values(),
                              key=lambda item: ((item["emp_name"] or item["emp_id"]).casefold(), item["emp_id"].casefold()))
    events = [event for event in events
              if _dashboard_reference_matches(event, query)
              and (not employee_filter or event["emp_id"].upper() == employee_filter)]

    type_counts = {event_type: 0 for event_type in sorted(USAGE_EVENT_TYPES)}
    user_map = {}
    date_keys = []
    daily_map = {}
    for offset in range(period_days):
        key = (range_from + datetime.timedelta(days=offset)).isoformat()
        date_keys.append(key)
        daily_map[key] = {"date": key, "actions": 0, "searches": 0, "prints": 0, "errors": 0, "users": set()}

    successful = 0
    search_durations = []
    for event in events:
        event_type = event["event_type"]
        type_counts[event_type] = type_counts.get(event_type, 0) + 1
        is_error = event["status"] in {"ERROR", "FAILED", "NOT_FOUND", "TIMEOUT"}
        if not is_error:
            successful += 1
        if event_type == "SEARCH" and event["duration_ms"] > 0:
            search_durations.append(event["duration_ms"])

        parsed_created = _parse_event_datetime(event["created_at"])
        day_key = parsed_created.astimezone(bangkok_tz).date().isoformat() if parsed_created else ""
        if day_key in daily_map:
            daily = daily_map[day_key]
            daily["actions"] += 1
            daily["searches"] += 1 if event_type == "SEARCH" else 0
            daily["prints"] += 1 if event_type == "PRINT_DOCUMENT" else 0
            daily["errors"] += 1 if is_error else 0
            if event["emp_id"]:
                daily["users"].add(event["emp_id"])

        emp_id = event["emp_id"] or "ไม่ระบุ"
        user = user_map.setdefault(emp_id, {
            "emp_id": emp_id, "emp_name": event["emp_name"] or "", "actions": 0,
            "searches": 0, "documents_opened": 0, "prints": 0, "saves": 0,
            "errors": 0, "search_duration_total": 0, "search_duration_count": 0,
            "last_active": event["created_at"],
        })
        if event["emp_name"]:
            user["emp_name"] = event["emp_name"]
        user["actions"] += 1
        user["searches"] += 1 if event_type == "SEARCH" else 0
        user["documents_opened"] += 1 if event_type == "OPEN_DOCUMENT" else 0
        user["prints"] += 1 if event_type == "PRINT_DOCUMENT" else 0
        user["saves"] += 1 if event_type == "SAVE_DOCUMENT" else 0
        user["errors"] += 1 if is_error else 0
        if event_type == "SEARCH" and event["duration_ms"] > 0:
            user["search_duration_total"] += event["duration_ms"]
            user["search_duration_count"] += 1
        if event["created_at"] > user["last_active"]:
            user["last_active"] = event["created_at"]

    users = []
    for user in user_map.values():
        count = user.pop("search_duration_count")
        total = user.pop("search_duration_total")
        user["avg_search_ms"] = round(total / count) if count else 0
        users.append(user)
    users.sort(key=lambda item: (-item["prints"], -item["documents_opened"], -item["actions"], item["emp_id"]))

    operations_error = None
    try:
        operation_rows = load_dashboard_operations_rows()
    except Exception as exc:
        operations_error = str(exc)[:160]
        operation_rows = []
        print(f"Dashboard operations unavailable: {operations_error}")

    operation_daily_map = {}
    operation_bookings = set()
    operation_waves = set()
    operation_branches = set()
    operation_totals = {key: 0 for key in ("m", "red", "blue", "green", "black", "total")}
    for row in operation_rows:
        if not _dashboard_reference_matches(row, query):
            continue
        pick_date = _parse_report_date(row.get("date"))
        if not pick_date or pick_date < range_from or pick_date > range_to:
            continue
        key = pick_date.isoformat()
        if key not in daily_map:
            continue
        item = operation_daily_map.setdefault(key, {
            "date": key, "m": 0, "red": 0, "blue": 0, "green": 0,
            "black": 0, "total": 0, "bookings": set(), "waves": set(), "branches": set()
        })
        for field in operation_totals:
            value = max(0, int(row.get(field) or 0))
            item[field] += value
            operation_totals[field] += value
        if row.get("booking"):
            item["bookings"].add(row["booking"])
            operation_bookings.add(row["booking"])
        if row.get("wave"):
            item["waves"].add(row["wave"])
            operation_waves.add(row["wave"])
        if row.get("branch"):
            item["branches"].add(row["branch"])
            operation_branches.add(row["branch"])

    operation_daily = []
    for key in date_keys:
        item = operation_daily_map.get(key, {
            "date": key, "m": 0, "red": 0, "blue": 0, "green": 0,
            "black": 0, "total": 0, "bookings": set(), "waves": set(), "branches": set()
        })
        item["booking_count"] = len(item.pop("bookings"))
        item["wave_count"] = len(item.pop("waves"))
        item["branch_count"] = len(item.pop("branches"))
        operation_daily.append(item)
    peak_day = max(operation_daily, key=lambda item: item["total"], default=None)
    tote_total = sum(operation_totals[key] for key in ("red", "blue", "green", "black"))
    operation_summary = {
        **operation_totals,
        "tote_total": tote_total,
        "booking_count": len(operation_bookings),
        "wave_count": len(operation_waves),
        "branch_count": len(operation_branches),
        "avg_boxes_per_booking": round(operation_totals["total"] / len(operation_bookings), 1) if operation_bookings else 0,
        "m_share_pct": round(operation_totals["m"] / operation_totals["total"] * 100, 1) if operation_totals["total"] else 0,
        "tote_share_pct": round(tote_total / operation_totals["total"] * 100, 1) if operation_totals["total"] else 0,
        "peak_date": peak_day["date"] if peak_day and peak_day["total"] else None,
        "peak_total": peak_day["total"] if peak_day else 0,
    }

    daily = []
    for key in date_keys:
        item = daily_map[key]
        item["active_users"] = len(item.pop("users"))
        daily.append(item)
    events.sort(key=lambda item: item["created_at"], reverse=True)
    searches = type_counts.get("SEARCH", 0)
    avg_search_ms = round(sum(search_durations) / len(search_durations)) if search_durations else 0
    success_rate = round((successful / len(events)) * 100, 1) if events else 0
    payload = {
        "success": True,
        "generated_at": now_utc.isoformat(),
        "period_days": period_days,
        "summary": {
            "active_users": len([user for user in users if user["emp_id"] != "ไม่ระบุ"]),
            "actions": len(events),
            "searches": searches,
            "documents_opened": type_counts.get("OPEN_DOCUMENT", 0),
            "prints": type_counts.get("PRINT_DOCUMENT", 0),
            "saves": type_counts.get("SAVE_DOCUMENT", 0),
            "errors": len(events) - successful,
            "success_rate": success_rate,
            "avg_search_ms": avg_search_ms,
        },
        "event_counts": type_counts,
        "daily": daily,
        "operations": {"summary": operation_summary, "daily": operation_daily},
        "filters": {
            "q": query,
            "employee": employee_filter,
            "date_from": range_from.isoformat(),
            "date_to": range_to.isoformat(),
            "custom_date_range": bool(date_from or date_to),
            "employee_applies_to": "usage_only",
            "reference_search_applies_to": "usage_and_delivery",
        },
        "employee_options": employee_options,
        "users": users[:50],
        "recent_events": events[:30],
        "system": {
            "api_version": APP_VERSION,
            "environment": APP_ENV,
            "data_source": "google_sheets",
            "scan_feature_enabled": SCAN_FEATURE_ENABLED,
            "uptime_seconds": round(time.time() - PROCESS_STARTED_AT),
            "dashboard_query_ms": round((time.perf_counter() - started) * 1000),
            "operations_error": operations_error,
        },
    }
    response.headers["Cache-Control"] = "private, max-age=30"
    return payload

def sync_document_summary_reports(summaries: list):
    """Write the given totals to both writable Sheets; the legacy workbook stays read-only.

    Automatic zero snapshots are dropped here so a branch that has not been
    counted yet never creates a zero row. An intentional zero correction carries
    allow_zero_update and is allowed through, but only to clear an existing row.
    """
    normalized = [normalize_report_summary(summary) for summary in summaries or []]
    normalized = [summary for summary in normalized
                  if not summary.get("is_hidden")
                  and (summary.get("total", 0) > 0 or summary.get("allow_zero_update"))]
    if not normalized:
        return
    # Member Data ไม่มีคอลัมน์ Booking จึงเก็บได้แค่ 1 แถวต่อ Wave+Branch
    # ส่วนรายงานรองรับแยก Booking และรับยอดที่แบ่งได้
    member_summaries = [summary for summary in normalized if not summary.get("booking_split")]
    failures = []
    try:
        write_member_history_summaries(member_summaries)
    except Exception as exc:
        failures.append(f"Member Data: {exc}")
    try:
        write_uat_report_test_summaries(normalized)
    except Exception as exc:
        failures.append(f"UAT Delivery report: {exc}")
    if failures:
        raise RuntimeError(" | ".join(failures))

@app.post("/api/document-summary")
def save_document_summary(data: DocumentSummaryBatchData, background_tasks: BackgroundTasks):
    if not data.summaries or len(data.summaries) > 100:
        raise HTTPException(status_code=400, detail="summary count must be 1-100")
    normalized = []
    emp_id = str(data.emp_id or "").strip()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for item in data.summaries:
        try:
            wave = str(int(str(item.wave).strip()))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"invalid wave: {item.wave}")
        branch = str(item.branch or "").strip().upper()
        if not branch:
            raise HTTPException(status_code=400, detail="branch is required")
        values = {key: max(0, int(getattr(item, key) or 0)) for key in
                  ("label_count", "m", "red", "blue", "green", "black", "total", "pallet")}
        calculated_total = values["m"] + values["red"] + values["blue"] + values["green"] + values["black"]
        values["total"] = calculated_total
        is_hidden = bool(getattr(item, "is_hidden", False))

        normalized.append({
            "wave": wave,
            "booking": str(item.booking or "").strip().upper(),
            "branch": branch,
            "branch_name": str(item.branch_name or branch).strip(),
            "bu": member_data_bu(item.bu),
            "is_hidden": is_hidden,
            "is_closed": bool(getattr(item, "is_closed", False)),
            "booking_split": bool(getattr(item, "booking_split", False)),
            **values
        })

    affected_split_sources = []
    if data.persist_overrides:
        # The frontend sends dirty branches only. Persist zero so old totals cannot return later.
        persistent_items = normalized
        for item in persistent_items:
            item["allow_zero_update"] = True
        # If this is the target side of a split, update the allocation record
        # itself. Otherwise refresh/print would rebuild the old split amount.
        affected_split_sources = persist_target_booking_split_edits(
            persistent_items, emp_id, data.reason or "MANUAL_TOTAL_SAVE"
        )
        # Apply the edit to the live web overlay first. A temporary Google auth
        # outage must never make the UI snap back to the calculated old total.
        with document_overrides_lock:
            for item in persistent_items:
                scope_key = _document_override_scope_key(item["wave"], item.get("booking"))
                wave_ov = document_overrides_overlay.setdefault(scope_key, {})
                wave_ov[item["branch"]] = {
                    **{field: item[field] for field in ("m", "red", "blue", "green", "black", "total", "pallet")},
                    "is_hidden": bool(item.get("is_hidden")),
                    "emp_id": emp_id,
                    "updated_at": now_iso,
                    "branch_name": item.get("branch_name") or item["branch"],
                    "bu": item.get("bu") or "Unknown",
                    "booking": item.get("booking") or "",
                    "label_count": item.get("label_count") or 0,
                }
        override_sheet_error = None
        try:
            record_document_overrides(copy.deepcopy(persistent_items), emp_id, data.reason or "")
        except Exception as exc:
            override_sheet_error = str(exc)
            print(f"🚨 Override Sheet pending; web overlay retained: {exc}")
    # รายงานใช้ยอดล่าสุดที่หน้าเอกสารแสดงเสมอ ไม่รอให้สาขาปิดจบก่อน
    report_summaries = list(normalized)
    # A target split edit also changes the source booking's remaining amount.
    # Refresh that exact Wave+Branch so Delivery report stays balanced.
    for source_booking, split_wave, split_branch in dict.fromkeys(affected_split_sources):
        if not source_booking:
            continue
        source_view = get_booking_data_internal(source_booking, force_refresh=True)
        source_items = [
            item for item in source_view.get("lpn_list", [])
            if str(item.get("branch") or "").strip().upper() == split_branch
            and re.sub(r"\D", "", str(item.get("wave_no") or ""))
            and str(int(re.sub(r"\D", "", str(item.get("wave_no") or "")))) == split_wave
        ]
        if not source_items:
            continue
        source_summary = summarize_branch_for_member_data(
            {"wave_no": split_wave, "booking_no": source_booking, "lpn_list": source_items},
            split_branch,
        )
        source_summary.update({
            "booking": source_booking, "booking_split": True,
            "is_closed": any(item.get("branch_closed_at") for item in source_items),
            "allow_zero_update": True,
        })
        report_summaries.append(source_summary)
    if data.persist_overrides:
        # A manual save is transactional from the user's perspective: do not
        # say success until every totals Sheet has accepted the new values.
        try:
            sync_document_summary_reports(copy.deepcopy(report_summaries))
        except Exception as exc:
            print(f"🚨 Manual totals Sheet pending; web overlay retained: {exc}")
            report_sync = "pending_google_credentials"
            sheet_warning = str(exc)
        else:
            report_sync = "completed" if not override_sheet_error else "pending_google_credentials"
            sheet_warning = override_sheet_error
    else:
        queue_report_summary_snapshots(copy.deepcopy(report_summaries))
        report_sync = "queued" if report_summaries else "waiting_for_branch_close"
    return {
        "status": "success",
        "updated": len(report_summaries),
        "report_sync": report_sync,
        "persist_overrides": bool(data.persist_overrides),
        "sheet_warning": sheet_warning if data.persist_overrides else None,
    }

@app.post("/api/reset-document-overrides")
def reset_document_overrides(data: ResetDocumentOverridesData, background_tasks: BackgroundTasks):
    waves_to_clear = []
    if data.wave:
        for w in str(data.wave).split(","):
            digits = re.sub(r"\D", "", w.strip())
            if digits:
                waves_to_clear.append(str(int(digits)))
    if data.booking:
        clean_b = str(data.booking).strip().upper()
        mapping = get_sheet_meta_for_booking(clean_b)
        if mapping and mapping.get("waves"):
            waves_to_clear.extend(mapping["waves"])

    waves_to_clear = list(dict.fromkeys(waves_to_clear))
    if waves_to_clear:
        def tombstone_sheet_overrides(waves, emp):
            now_iso = _uat_now_iso()
            rows = [{
                    "Event_ID": str(uuid.uuid4()),
                    "Action": "RESET_ALL",
                    "Wave_Number": str(w),
                    "Booking_No": "",
                    "Branch_Code": "RESET_ALL",
                    "M_Count": 0,
                    "Red_Count": 0,
                    "Blue_Count": 0,
                    "Green_Count": 0,
                    "Black_Count": 0,
                    "Total_Count": 0,
                    "Pallet_Count": 0,
                    "Is_Hidden": 0,
                    "Reason": "RESET_OVERRIDE",
                    "Emp_ID": str(emp or "").strip(),
                    "Created_At": now_iso,
                } for w in waves]
            append_uat_event_rows("Document Overrides", rows)
            print(f"✅ Tombstoned document overrides for waves {waves} in UAT Sheet")
        # เช่นเดียวกับการแก้ยอด: reset ต้อง durable ก่อนจึงแจ้งว่าสำเร็จ
        tombstone_sheet_overrides(waves_to_clear, data.emp_id)
        with document_overrides_lock:
            for w in waves_to_clear:
                for cache_key in [key for key in document_overrides_overlay if key[0] == w]:
                    document_overrides_overlay.pop(cache_key, None)
        queue_wave_totals_reconciliation(waves_to_clear, delay_seconds=0.5)

    return {"status": "success", "cleared_waves": waves_to_clear}

@app.get("/api/document-overrides")
def get_document_overrides_endpoint(wave_no: str, booking: str = ""):
    clean_w = re.sub(r"\D", "", str(wave_no or ""))
    if not clean_w:
        return {"overrides": {}}
    return {"wave_no": clean_w, "booking": str(booking or "").strip().upper(),
            "overrides": get_document_overrides_for_wave(clean_w, booking)}

@app.get("/api/transport-meta")
def get_transport_meta(booking: str):
    """ข้อมูลรถและขนส่งจาก Booking & Wave source (ไม่อ่าน Delivery report เดิม)."""
    clean_booking = str(booking or "").strip().upper()
    if not clean_booking:
        return {"booking": "", "carrier": "", "driver": "", "plate": ""}
    meta = get_sheet_meta_for_booking(clean_booking)
    return {
        "booking": clean_booking,
        "carrier": str(meta.get("carrier") or ""),
        "driver": str(meta.get("sender") or ""),
        "plate": str(meta.get("plate") or ""),
    }

def transaction_already_processed(transaction_id: str) -> bool:
    tx_id = str(transaction_id or "").strip()
    if not tx_id:
        return False
    now = time.time()
    with processed_transaction_lock:
        expired = [key for key, saved_at in processed_transaction_ids.items() if now - saved_at > TRANSACTION_TTL_SECONDS]
        for key in expired:
            processed_transaction_ids.pop(key, None)
        return tx_id in processed_transaction_ids

def mark_transaction_processed(transaction_id: str):
    tx_id = str(transaction_id or "").strip()
    if tx_id:
        with processed_transaction_lock:
            processed_transaction_ids[tx_id] = time.time()

@app.post("/api/device-state")
def update_device_state(data: DeviceStateData):
    device_id = str(data.device_id or "").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="device_id is required")
    waves = sorted({str(int(str(wave).strip())) for wave in data.waves if str(wave).strip().isdigit()})
    with device_pending_states_lock:
        device_pending_states[device_id] = {
            "device_id": device_id,
            "waves": waves,
            "branch_code": str(data.branch_code or "").strip().upper(),
            "pending_count": max(0, int(data.pending_count or 0)),
            "pending_lpns": [str(lpn).strip().upper() for lpn in data.pending_lpns[:10] if str(lpn).strip()],
            "emp_id": str(data.emp_id or "").strip(),
            "updated_at": time.time(),
        }
    return {"status": "success"}

@app.get("/api/device-states")
def get_device_states(waves: str = "", branch_code: str = "", exclude_device_id: str = ""):
    requested = {str(int(part.strip())) for part in waves.split(",") if part.strip().isdigit()}
    requested_branch = str(branch_code or "").strip().upper()
    now = time.time()
    active = []
    with device_pending_states_lock:
        expired = [key for key, state in device_pending_states.items() if now - state.get("updated_at", 0) > DEVICE_STATE_TTL_SECONDS]
        for key in expired:
            device_pending_states.pop(key, None)
        for state in device_pending_states.values():
            if state["device_id"] == exclude_device_id or state["pending_count"] <= 0:
                continue
            if requested_branch and state["branch_code"] != requested_branch:
                continue
            if requested and not requested.intersection(state["waves"]):
                continue
            active.append(dict(state))
    return {"status": "success", "devices": active}

@app.post("/api/clear-device-states")
def clear_device_states(data: DeviceStateData):
    branch = str(data.branch_code or "").strip().upper()
    waves = {str(int(str(wave).strip())) for wave in data.waves if str(wave).strip().isdigit()}
    with device_pending_states_lock:
        to_del = []
        for key, state in device_pending_states.items():
            if branch and state.get("branch_code") == branch:
                to_del.append(key)
            elif waves and waves.intersection(state.get("waves") or []):
                to_del.append(key)
        for key in to_del:
            device_pending_states.pop(key, None)
    return {"status": "cleared", "count": len(to_del)}

# 🚀 [API 1] โหลดข้อมูล Wave
# 🚀 [API 1] โหลดข้อมูล Wave
@app.get("/api/check-wave")
def check_wave(wave_no: str, force: bool = False):
    try:
        # การค้นหาปกติใช้ cache ที่ warm ไว้ ส่วนปุ่มรีเฟรชส่ง force=true เมื่อต้องอ่าน
        # Google Sheets ใหม่จริง ๆ การบังคับ force ทุกครั้งทำให้โหลด Member Data 39k+ แถวซ้ำ
        try:
            raw_data = get_wave_data_internal(wave_no, force_refresh=force)
        except HTTPException as exc:
            history_items = build_member_history_items(wave_no)
            if exc.status_code != 404 or not history_items:
                raise
            raw_data = {
                "wave_no": f"{int(str(wave_no).strip()):010d}", "booking_no": "",
                "license_plate": "", "lpn_list": history_items, "zone_summary": []
            }
        # Apply the in-memory scan overlays dynamically
        overlaid_data = apply_local_overlay(wave_no, raw_data)
        result = merge_member_history(overlaid_data, wave_no)
        # 1 Wave อยู่ได้หลาย Booking: ส่งตัวเลือกกลับไปให้หน้าเว็บถาม ไม่เดาให้
        result["booking_options"] = [
            str(item.get("booking") or "").strip().upper()
            for item in get_sheet_metas_for_wave(wave_no, force=force)
            if str(item.get("booking") or "").strip()
        ]
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"🚨 CHECK WAVE ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 [API 1.5] โหลดข้อมูล Booking
def ensure_booking_source_complete(booking_no: str, booking_data: dict):
    """Report progressive Member Data snapshots that are missing planned branches."""
    booking = re.sub(r"\s+", "", str(booking_no or "").upper())
    _, _, expected_map = load_wave_monitoring_pick_dates(force=False)
    missing = []
    for raw_wave in booking_data.get("waves") or []:
        digits = re.sub(r"\D", "", str(raw_wave or ""))
        if not digits:
            continue
        wave = str(int(digits))
        expected = set(expected_map.get((booking, wave)) or set())
        if not expected:
            continue
        actual = {
            str(item.get("branch") or "").strip().upper()
            for item in build_member_history_items(wave)
            if str(item.get("branch") or "").strip()
        }
        # Saved manual corrections are complete, durable branch records even
        # while the large Member Data import is delayed.
        actual.update(get_document_overrides_for_wave(wave, booking).keys())
        absent = sorted(expected - actual)
        if absent:
            missing.append({"wave": wave, "missing": absent, "expected": len(expected), "actual": len(actual)})
    if missing:
        details = "; ".join(
            f"Wave {item['wave']} ขาด {len(item['missing'])} สาขา ({','.join(item['missing'][:5])})"
            for item in missing
        )
        print(f"⚠️ UAT Booking [{booking}] info: {details}")



@app.get("/api/check-booking")
def check_booking(booking_no: str, force: bool = False):
    try:
        try:
            booking_data = get_booking_data_internal(booking_no, force_refresh=force)
            ensure_booking_source_complete(booking_no, booking_data)
            return booking_data
        except HTTPException as first_error:
            if first_error.status_code not in (404, 409):
                raise
            # Member Data is populated progressively. Re-read one consistent
            # snapshot before declaring a multi-Wave Booking incomplete.
            time.sleep(1.0)
            try:
                booking_data = get_booking_data_internal(booking_no, force_refresh=True)
                ensure_booking_source_complete(booking_no, booking_data)
                return booking_data
            except HTTPException as retry_error:
                if retry_error.status_code in (404, 409):
                    raise HTTPException(
                        status_code=409,
                        detail=str(retry_error.detail),
                    )
                raise
    except HTTPException:
        raise
    except Exception as e:
        print(f"🚨 CHECK BOOKING ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/booking-branch-preview")
def preview_booking_branch(wave_no: str, branch_code: str):
    wave_clean = str(int(str(wave_no).strip()))
    branch = str(branch_code or "").strip().upper()
    data = apply_local_overlay(wave_clean, get_wave_data_internal(wave_clean, force_refresh=True))
    items = [item for item in data.get("lpn_list", []) if str(item.get("branch") or "").strip().upper() == branch]
    if not items:
        raise HTTPException(status_code=404, detail=f"ไม่พบสาขา {branch} ใน Wave {wave_clean}")
    assignments = get_booking_branch_assignments()
    assignment = assignments.get((wave_clean, branch))
    current_booking = str((assignment or {}).get("Assigned_Booking") or data.get("booking_no") or "").strip().upper()
    scanned = [item for item in items if item.get("status") == "Scanned"]
    summary = summarize_branch_for_member_data(data, branch)
    return {
        "status": "success", "wave_no": wave_clean, "branch_code": branch,
        "branch_name": items[0].get("branch_name") or "Unknown",
        "current_booking": current_booking, "lpn_total": len(items),
        "lpn_scanned": len(scanned), "box_qty": sum(int(item.get("qty") or 0) for item in scanned),
        "pallet_count": len({int(item.get("pallet_no") or 0) for item in scanned if int(item.get("pallet_no") or 0) > 0}),
        "totals": {field: int(summary.get(field) or 0) for field in ("m", "red", "blue", "green", "black", "pallet")},
        "closed_at": next((item.get("branch_closed_at") for item in items if item.get("branch_closed_at")), "")
    }

@app.get("/api/wave-branch-options")
def get_wave_branch_options(wave_no: str):
    wave_clean = str(int(str(wave_no).strip()))
    data = apply_local_overlay(wave_clean, get_wave_data_internal(wave_clean, force_refresh=True))
    assignments = get_booking_branch_assignments()
    grouped = {}
    for item in data.get("lpn_list", []):
        branch = str(item.get("branch") or "").strip().upper()
        if not branch:
            continue
        row = grouped.setdefault(branch, {
            "branch_code": branch, "branch_name": item.get("branch_name") or "Unknown",
            "lpn_total": 0, "lpn_scanned": 0, "box_qty": 0, "pallet_nos": set(),
            "closed_at": item.get("branch_closed_at") or ""
        })
        row["lpn_total"] += 1
        if item.get("status") == "Scanned":
            row["lpn_scanned"] += 1
            row["box_qty"] += int(item.get("qty") or 0)
            pallet_no = int(item.get("pallet_no") or 0)
            if pallet_no > 0:
                row["pallet_nos"].add(pallet_no)
        if item.get("branch_closed_at"):
            row["closed_at"] = item.get("branch_closed_at")
    native_booking = str(data.get("booking_no") or "").strip().upper()
    options = []
    for branch, row in grouped.items():
        assignment = assignments.get((wave_clean, branch))
        row["current_booking"] = str((assignment or {}).get("Assigned_Booking") or native_booking).strip().upper()
        row["pallet_count"] = len(row.pop("pallet_nos"))
        summary = summarize_branch_for_member_data(data, branch)
        row["totals"] = {field: int(summary.get(field) or 0) for field in ("m", "red", "blue", "green", "black", "pallet")}
        row["is_closed"] = bool(row["closed_at"])
        options.append(row)
    options.sort(key=lambda item: item["branch_code"])
    return {"status": "success", "wave_no": wave_clean, "branches": options}

@app.post("/api/move-booking-branch")
def move_booking_branch(data: BookingBranchMoveData):
    target = str(data.target_booking or "").strip().upper()
    wave_clean = str(int(str(data.wave_no).strip()))
    branch = str(data.branch_code or "").strip().upper()
    reason = str(data.reason or "").strip()
    emp_id = str(data.emp_id or "").strip()
    if not target or not branch or not reason or not emp_id:
        raise HTTPException(status_code=400, detail="กรุณาระบุ Booking, สาขา, เหตุผล และผู้ดำเนินการให้ครบ")
    preview = preview_booking_branch(wave_clean, branch)
    previous = str(preview.get("current_booking") or "").strip().upper()
    if previous == target:
        raise HTTPException(status_code=409, detail=f"สาขา {branch} อยู่ใน Booking {target} แล้ว")
    # ปลายทางต้องเป็น Booking จริงที่มีอยู่ เพื่อป้องกันพิมพ์ผิดแล้วสาขาหายจากเอกสาร
    get_booking_waves_mapping(target, force_refresh=True)
    append_uat_event_rows("Booking Branch Moves", [{
        "Event_ID": str(uuid.uuid4()), "Wave_Number": wave_clean, "Branch_Code": branch,
        "Previous_Booking": previous, "Assigned_Booking": target, "Reason": reason,
        "Note": str(data.note or "").strip(), "Emp_ID": emp_id, "Created_At": _uat_now_iso(),
    }])
    with booking_assignments_cache_lock:
        booking_assignments_cache["expires_at"] = 0.0
    with booking_waves_cache_lock:
        booking_waves_cache.pop(previous, None)
        booking_waves_cache.pop(target, None)
    queue_branch_totals_reconciliation([(wave_clean, branch)], delay_seconds=0.5)
    # ส่งมุมมองปลายทางที่อ่านหลังบันทึกกลับใน response เดียว ป้องกันหน้าเว็บ
    # ยิง GET ถัดไปเร็วเกินไปแล้วเห็นข้อมูลก่อนย้าย.
    target_view = get_booking_data_internal(target, force_refresh=True)
    return {"status": "success", "message": "ย้ายสาขาเรียบร้อย", "previous_booking": previous,
            "target_booking": target, "preview": preview, "target_view": target_view}

@app.post("/api/split-booking-branch")
def split_booking_branch(data: BookingBranchSplitData):
    target = str(data.target_booking or "").strip().upper()
    wave_clean = str(int(str(data.wave_no).strip()))
    branch = str(data.branch_code or "").strip().upper()
    reason = str(data.reason or "").strip()
    emp_id = str(data.emp_id or "").strip()
    if not target or not branch or not reason or not emp_id:
        raise HTTPException(status_code=400, detail="กรุณาระบุ Booking, สาขา, เหตุผล และผู้ดำเนินการให้ครบ")
    preview = preview_booking_branch(wave_clean, branch)
    source = str(preview.get("current_booking") or "").strip().upper()
    if source == target:
        raise HTTPException(status_code=409, detail=f"สาขา {branch} อยู่ใน Booking {target} แล้ว")
    requested = {field: max(0, int(getattr(data, field) or 0)) for field in
                 ("m", "red", "blue", "green", "black", "pallet")}
    if sum(requested[field] for field in ("m", "red", "blue", "green", "black")) <= 0:
        raise HTTPException(status_code=400, detail="กรุณาระบุยอดกล่องที่ต้องการแบ่งอย่างน้อย 1 กล่อง")
    available = preview.get("totals") or {}
    existing_splits = [split for (split_wave, split_branch, split_target), split in get_booking_branch_splits().items()
                       if split_wave == wave_clean and split_branch == branch and bool(split.get("Is_Active", True))
                       and split_target != target]
    split_columns = {"m": "M_Count", "red": "Red_Count", "blue": "Blue_Count",
                     "green": "Green_Count", "black": "Black_Count", "pallet": "Pallet_Count"}
    for field, value in requested.items():
        remaining = max(0, int(available.get(field) or 0) - sum(int(split.get(split_columns[field]) or 0) for split in existing_splits))
        if value > remaining:
            raise HTTPException(status_code=409, detail=f"ยอด {field} ที่แบ่ง ({value}) มากกว่ายอดคงเหลือ ({remaining})")
    get_booking_waves_mapping(target, force_refresh=True)
    append_uat_event_rows("Booking Branch Splits", [{
        "Event_ID": str(uuid.uuid4()), "Wave_Number": wave_clean, "Branch_Code": branch,
        "Source_Booking": source, "Target_Booking": target,
        "M_Count": requested["m"], "Red_Count": requested["red"], "Blue_Count": requested["blue"],
        "Green_Count": requested["green"], "Black_Count": requested["black"],
        "Pallet_Count": requested["pallet"], "Is_Active": True, "Reason": reason,
        "Note": str(data.note or "").strip(), "Emp_ID": emp_id, "Created_At": _uat_now_iso(),
    }])
    with booking_splits_cache_lock:
        booking_splits_cache["expires_at"] = 0.0
    with booking_waves_cache_lock:
        booking_waves_cache.pop(source, None)
        booking_waves_cache.pop(target, None)
    split_report_summaries = []
    booking_views = {}
    for booking in (source, target):
        booking_view = get_booking_data_internal(booking, force_refresh=True)
        booking_views[booking] = booking_view
        # A booking can contain the same branch in several waves. Build the
        # report from the exact Wave+Branch only, otherwise another wave's
        # split summary can overwrite this row or make one side look missing.
        branch_view_items = [
            item for item in booking_view.get("lpn_list", [])
            if str(item.get("branch") or "").strip().upper() == branch
            and re.sub(r"\D", "", str(item.get("wave_no") or ""))
            and str(int(re.sub(r"\D", "", str(item.get("wave_no") or "")))) == wave_clean
        ]
        if not branch_view_items:
            continue
        summary = summarize_branch_for_member_data(
            {"wave_no": wave_clean, "booking_no": booking, "lpn_list": branch_view_items}, branch
        )
        is_closed = any(item.get("branch_closed_at") for item in branch_view_items)
        summary.update({"booking": booking, "booking_split": True, "is_closed": is_closed})
        split_report_summaries.append(summary)
    queue_report_summary_snapshots(split_report_summaries, delay_seconds=0.0)
    return {"status": "success", "message": "แบ่งยอดเข้าสอง Booking เรียบร้อย",
            "source_booking": source, "target_booking": target, "allocated": requested,
            "target_view": booking_views.get(target)}

@app.post("/api/start-pallet")
def start_pallet(data: PalletStartData):
    """Allocate one branch-wide pallet number so multiple handhelds cannot reuse the same number."""
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    wave_ids = {int(str(w).strip()) for w in data.waves if str(w).strip().isdigit()}
    if (data.booking_no or "").strip():
        try:
            mapping = get_booking_waves_mapping((data.booking_no or "").strip())
            wave_ids.update(int(str(w).strip()) for w in mapping.get("waves", []) if str(w).strip().isdigit())
        except Exception:
            pass
    wave_ids = sorted(wave_ids)
    branch = (data.branch_code or "").strip().upper()
    branch_name = (data.branch_name or "").strip()
    color = (data.color or "").strip().title()
    emp_id = (data.emp_id or "").strip()
    if not wave_ids or not branch:
        raise HTTPException(status_code=400, detail="ข้อมูล Wave หรือสาขาไม่ครบ")
    if color not in ("Green", "Blue", "Red"):
        raise HTTPException(status_code=400, detail="สีพาเลทไม่ถูกต้อง")

    wave_keys = {str(w) for w in wave_ids}
    cache_key = (tuple(wave_ids), branch)
    with pallet_allocation_lock:
        prior = [] if SCAN_DEMO_ONLY else [
            row for row in read_uat_event_records("Scan Transactions")
            if str(row.get("Branch_Code") or "").strip().upper() == branch
            and str(row.get("Wave_Number") or "").strip() in wave_keys
        ]
        next_no = max([_history_int(row.get("Pallet_No")) for row in prior]
                      + [_history_int(pallet_counter_cache.get(cache_key, 0))]) + 1
        pallet_counter_cache[cache_key] = next_no
    for wave_id in wave_ids:
        save_uat_scan_event(str(wave_id), f"PALLET_{branch}_{next_no}", branch, branch_name,
                            0, "PALLET_START", color, emp_id, next_no,
                            f"uat-pallet:{wave_id}:{branch}:{next_no}")
    record_shared_pallet_state(wave_ids, branch, next_no, color=color, submitted=False)
    return {"status": "success", "pallet_no": next_no, "color": color, "allocated_by": emp_id}

@app.post("/api/submit-pallet")
def submit_pallet(data: PalletSubmitData):
    """Share a completed pallet with every handheld and keep an auditable marker."""
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    wave_ids = {int(str(w).strip()) for w in data.waves if str(w).strip().isdigit()}
    if (data.booking_no or "").strip():
        try:
            mapping = get_booking_waves_mapping((data.booking_no or "").strip())
            wave_ids.update(int(str(w).strip()) for w in mapping.get("waves", []) if str(w).strip().isdigit())
        except Exception:
            pass
    wave_ids = sorted(wave_ids)
    branch = (data.branch_code or "").strip().upper()
    branch_name = (data.branch_name or "").strip()
    pallet_no = int(data.pallet_no or 0)
    color = (data.color or "").strip().title()
    emp_id = (data.emp_id or "").strip()
    if not wave_ids or not branch or pallet_no <= 0:
        raise HTTPException(status_code=400, detail="ข้อมูล Wave สาขา หรือเลขพาเลทไม่ครบ")

    # The marker key comes from Wave+branch+pallet, so a network retry can never
    # append a second submission row for the same pallet.
    for wave_id in wave_ids:
        save_uat_scan_event(str(wave_id), f"PALLET_SUBMIT_{branch}_{pallet_no}", branch, branch_name,
                            0, "PALLET_SUBMIT", color or "None", emp_id, pallet_no,
                            f"uat-pallet-submit:{wave_id}:{branch}:{pallet_no}")
    record_shared_pallet_state(wave_ids, branch, pallet_no, color=color, submitted=True)
    # รายงาน Google Sheet ใช้เฉพาะยอดตอนปิดสาขา จึงไม่ต้องซิงก์ตอนส่งทุกพาเลท
    return {"status": "success", "pallet_no": pallet_no, "submitted_by": emp_id, "report_sync": "not_needed"}

def encode_correction_audit(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def decode_correction_audit(scan_type: str) -> Optional[dict]:
    try:
        encoded = str(scan_type or "").split("|", 1)[1]
        encoded += "=" * (-len(encoded) % 4)
        return json.loads(base64.urlsafe_b64decode(encoded.encode("ascii")).decode("utf-8"))
    except Exception:
        return None

@app.post("/api/correct-lpn")
def correct_lpn(data: CorrectionData, background_tasks: BackgroundTasks):
    """Record an intentional quantity correction with a full audit trail in the UAT workbook."""
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    try:
        wave_clean = str(int((data.wave_no or "").strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ไม่ถูกต้อง")

    branch = (data.branch_code or "").strip().upper()
    lpn = (data.lpn or "").strip().upper()
    reason = (data.reason or "").strip()
    note = (data.note or "").strip()
    emp_id = (data.emp_id or "").strip()
    correction_id = (data.correction_id or str(uuid.uuid4())).strip()
    new_qty = int(data.new_qty or 0)
    pallet_no = int(data.pallet_no or 0)
    scan_type = (data.scan_type or "Carton").strip()
    color = (data.color or "None").strip()
    if not branch or not lpn or not reason:
        raise HTTPException(status_code=400, detail="กรุณาระบุ LPN สาขา และเหตุผลการแก้ไข")
    if new_qty < 0:
        raise HTTPException(status_code=400, detail="ยอดใหม่ต้องเป็น 0 ขึ้นไป")

    audit_txn = f"correction:{correction_id}"
    # A retry of the same correction must never apply the new quantity twice.
    if transaction_already_processed(audit_txn):
        queue_branch_totals_reconciliation([(wave_clean, branch)])
        return {"status": "success", "correction_id": correction_id, "duplicate": True,
                "report_sync": "queued"}

    fresh = apply_local_overlay(wave_clean, get_wave_data_internal(wave_clean))
    current = next((item for item in fresh.get("lpn_list", [])
                    if str(item.get("lpn", "")).strip().upper() == lpn
                    and str(item.get("branch", "")).strip().upper() == branch), None)
    if not current:
        raise HTTPException(status_code=404, detail=f"ไม่พบ LPN [{lpn}] ใน Wave {wave_clean} สาขา {branch}")

    old_snapshot = {
        "qty": int(current.get("qty") or 0),
        "status": current.get("status") or "Pending",
        "scan_type": current.get("scan_type") or "",
        "color": current.get("color") or "None",
        "pallet_no": int(current.get("pallet_no") or 0),
        "color_breakdown": current.get("color_breakdown") or [],
    }
    new_snapshot = {
        "qty": new_qty,
        "status": "Scanned" if new_qty > 0 else "Pending",
        "scan_type": scan_type if new_qty > 0 else "RESET_BOX",
        "color": color if new_qty > 0 else "None",
        "pallet_no": pallet_no if new_qty > 0 else 0,
    }
    audit_payload = {
        "correction_id": correction_id,
        "wave_no": wave_clean,
        "branch": branch,
        "lpn": lpn,
        "old": old_snapshot,
        "new": new_snapshot,
        "reason": reason,
        "note": note,
        "emp_id": emp_id,
        "corrected_at": _uat_now_iso(),
    }
    audit_type = "CORRECTION|" + encode_correction_audit(audit_payload)
    branch_name = (data.branch_name or "").strip()

    # Audit row first: the history must survive even if the value row is retried.
    save_uat_scan_event(wave_clean, lpn, branch, branch_name, 0, audit_type,
                        f"AUDIT:{correction_id}", emp_id,
                        int(old_snapshot["pallet_no"] or 0), audit_txn)
    if new_qty > 0:
        save_uat_scan_event(wave_clean, lpn, branch, branch_name, new_qty, scan_type,
                            color, emp_id, pallet_no, f"correction-value:{correction_id}")
    else:
        save_uat_scan_event(wave_clean, lpn, branch, branch_name, 0, "RESET_BOX",
                            "None", emp_id, 0, f"correction-reset:{correction_id}")
    queue_branch_totals_reconciliation([(wave_clean, branch)])
    return {"status": "success", "correction_id": correction_id, "audit": audit_payload,
            "report_sync": "queued"}

@app.get("/api/corrections")
def get_corrections(waves: str, branch: str, lpn: Optional[str] = None):
    """Correction history for one branch, newest first, read from the UAT scan log."""
    wave_ids = {str(int(part.strip())) for part in str(waves or "").split(",") if part.strip().isdigit()}
    branch_clean = (branch or "").strip().upper()
    lpn_clean = (lpn or "").strip().upper()
    if not wave_ids or not branch_clean:
        raise HTTPException(status_code=400, detail="ข้อมูล Wave หรือสาขาไม่ครบ")
    history = []
    for row in reversed(read_uat_event_records("Scan Transactions")):
        scan_type = str(row.get("Scan_Type") or "")
        if not scan_type.startswith("CORRECTION|"):
            continue
        wave_digits = re.sub(r"\D", "", str(row.get("Wave_Number") or ""))
        if not wave_digits or str(int(wave_digits)) not in wave_ids:
            continue
        if str(row.get("Branch_Code") or "").strip().upper() != branch_clean:
            continue
        if lpn_clean and str(row.get("LPN") or "").strip().upper() != lpn_clean:
            continue
        decoded = decode_correction_audit(scan_type)
        if decoded:
            history.append(decoded)
        if len(history) >= 200:
            break
    return {"status": "success", "history": history}

# 🚀 [API 2] บันทึกข้อมูลสแกนทีละกล่อง
# 🚀 [API 2] บันทึกข้อมูลสแกนทีละกล่อง
@app.post("/api/scan")
def process_scan(data: ScanData, background_tasks: BackgroundTasks):
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    try:
        wave_clean = str(int(data.wave_no))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ไม่ถูกต้อง")

    lpn_val = (data.lpn or "").strip()
    branch_val = (data.branch_code or "").strip()
    branch_name_val = (data.branch_name or "").strip()
    emp_val = (data.emp_id or "").strip()
    type_val = (data.type or "").strip()
    color_val = (data.color or "").strip()
    transaction_id = (data.transaction_id or "").strip()
    if transaction_already_processed(transaction_id):
        return {"status": "success", "message": "Already saved", "duplicate": True}
    try:
        pallet_no_val = int(data.pallet_no or 0)
    except (ValueError, TypeError):
        pallet_no_val = 0

    # Member Data is the read-only plan for this Wave. Validate the branch there,
    # then write the scan only into the isolated UAT workbook.
    source = build_uat_wave_data(wave_clean)
    known_branches = {str(item.get("branch") or "").strip().upper()
                      for item in source.get("lpn_list") or []}
    if branch_val.upper() not in known_branches:
        raise HTTPException(status_code=400, detail=f"ไม่พบสาขา [{branch_val}] ใน Wave {wave_clean}")

    # PP/SP ใช้ยอดสะสม: ตรวจว่าระหว่างนั้นไม่มีเครื่องอื่นแก้ยอดเดียวกัน
    # ถ้าค่าเริ่มต้นไม่ตรง ให้ผู้ใช้โหลดค่าล่าสุดแทนการเขียนทับข้อมูลของอีกเครื่อง
    base_pallet_breakdown = []
    if data.expected_previous_qty is not None and type_val not in ("RESET_BOX", "CANCEL_COMBINE"):
        current_data = apply_local_overlay(wave_clean, source)
        current_item = next((item for item in current_data.get("lpn_list", [])
                             if str(item.get("lpn", "")).strip().upper() == lpn_val.upper()
                             and str(item.get("branch", "")).strip().upper() == branch_val.upper()), None)
        current_qty = int((current_item or {}).get("qty") or 0)
        base_pallet_breakdown = list((current_item or {}).get("pallet_breakdown") or [])
        expected_qty = int(data.expected_previous_qty or 0)
        if current_qty != expected_qty:
            raise HTTPException(
                status_code=409,
                detail=f"LPN [{lpn_val}] ถูกอีกเครื่องอัปเดตแล้ว (ยอดล่าสุด {current_qty} กล่อง / เครื่องนี้เริ่มจาก {expected_qty}) กรุณารอหน้าจออัปเดตแล้วสแกนใหม่"
            )

    saved = save_uat_scan_event(wave_clean, lpn_val, branch_val, branch_name_val,
                                data.qty, type_val, color_val, emp_val, pallet_no_val,
                                transaction_id, base_pallet_breakdown)
    return {"status": "success", "message": "Saved" if saved else "Already saved",
            "duplicate": not saved, "report_sync": "not_needed"}


# 🚀 [API 2.5] บันทึกข้อมูลสแกนเป็นชุด (Batch)
@app.post("/api/scan-batch")
def process_scan_batch(data: ScanBatchData, background_tasks: BackgroundTasks):
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    if not data.scans:
        return {"status": "success", "message": "No scans to process", "processed_count": 0}

    processed, failed, ids = 0, [], []
    branch_cache = {}
    for item in data.scans:
        txn = (item.transaction_id or "").strip()
        try:
            # A queue retry resends the same transaction_id; report it as done
            # instead of appending a second row for the same box.
            if txn and transaction_already_processed(txn):
                ids.append(txn)
                continue
            wave_clean = str(int(item.wave_no))
            if wave_clean not in branch_cache:
                branch_cache[wave_clean] = {str(row.get("branch") or "").strip().upper()
                                            for row in build_uat_wave_data(wave_clean).get("lpn_list") or []}
            branch = (item.branch_code or "").strip().upper()
            if branch not in branch_cache[wave_clean]:
                raise ValueError(f"ไม่พบสาขา [{branch}] ใน Wave {wave_clean}")
            saved = save_uat_scan_event(wave_clean, item.lpn, branch, item.branch_name,
                                        item.qty, item.type, item.color, item.emp_id,
                                        int(item.pallet_no or 0), txn)
            if saved:
                processed += 1
            if txn:
                ids.append(txn)
        except Exception as exc:
            failed.append({"lpn": item.lpn, "transaction_id": item.transaction_id, "reason": str(exc)})
    return {"status": "success" if not failed else ("partial_success" if processed else "failed"),
            "processed_count": processed, "failed_count": len(failed),
            "processed_transaction_ids": ids, "errors": failed, "report_sync": "not_needed"}

# 🚀 [API 5] ปิดจบงานสาขา
@app.post("/api/close-job")
def close_job(data: CloseJobData, background_tasks: BackgroundTasks):
    """Close one branch and record the exact totals shown on screen into the UAT workbook."""
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    try:
        wave_clean = str(int(data.wave.strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ไม่ถูกต้อง")

    branch = data.branch.strip().upper()
    if not branch:
        raise HTTPException(status_code=400, detail="กรุณาระบุรหัสสาขา")
    emp_id = (data.emp_id or "").strip()
    completed_at = (data.completed_at or "").strip() or _uat_now_iso()

    frontend_summary = None
    if data.summary:
        frontend_summary = normalize_report_summary(data.summary.dict(), wave_clean, branch)
        frontend_summary["is_closed"] = True
        if int(frontend_summary.get("label_count") or 0) > 0 and frontend_summary.get("total", 0) <= 0:
            raise HTTPException(
                status_code=409,
                detail="ยังปิดสาขาไม่ได้: พบรายการ LPN แต่ยอดรวมเป็น 0 กรุณารีเฟรชข้อมูลและตรวจคิวส่งก่อนกดปิดอีกครั้ง",
            )

    # The close marker must be durable before the API reports success, so a
    # restart can never lose the fact that this branch was finished.
    summary = frontend_summary or {}
    try:
        append_uat_event_rows("Branch Close Status", [{
            "Event_ID": str(uuid.uuid4()),
            "Wave_Number": wave_clean,
            "Booking_No": str(summary.get("booking") or "").strip().upper(),
            "Branch_Code": branch,
            "Branch_Name": str(summary.get("branch_name") or branch),
            "Status": "CLOSED",
            "M_Count": int(summary.get("m") or 0),
            "Red_Count": int(summary.get("red") or 0),
            "Blue_Count": int(summary.get("blue") or 0),
            "Green_Count": int(summary.get("green") or 0),
            "Black_Count": int(summary.get("black") or 0),
            "Total_Count": int(summary.get("total") or 0),
            "Pallet_Count": int(summary.get("pallet") or 0),
            "Emp_ID": emp_id,
            "Completed_At": completed_at,
            "Created_At": _uat_now_iso(),
        }])
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"บันทึกสถานะปิดสาขายังไม่สำเร็จ: {exc}")

    record_shared_branch_closed(wave_clean, branch, completed_at, emp_id)
    if frontend_summary:
        # The screen totals are authoritative; the report worker writes them next.
        queue_report_summary_snapshots([frontend_summary], delay_seconds=0.0)

    return {
        "status": "success",
        "message": f"ปิดจบงานสาขา {branch} เรียบร้อย ระบบกำลังบันทึกยอดเข้ารายงาน",
        "completed_at": completed_at,
    }

@app.get("/api/member-history-status")
def member_history_status():
    try:
        session = get_sheets_session()
        target = urllib.parse.quote("Member Data!A1:P2", safe="")
        response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{MEMBER_HISTORY_SPREADSHEET_ID}/values/{target}",
            timeout=30
        )
        response.raise_for_status()
        # เขียนค่า A1 เดิมกลับที่เดิม เพื่อยืนยันสิทธิ์ Editor โดยไม่เปลี่ยนข้อมูลจริง
        first_value = ((response.json().get("values") or [[" วันที่"]])[0] or [" วันที่"])[0]
        verify_target = urllib.parse.quote("Member Data!A1", safe="")
        verify = session.put(
            f"https://sheets.googleapis.com/v4/spreadsheets/{MEMBER_HISTORY_SPREADSHEET_ID}/values/{verify_target}?valueInputOption=RAW",
            json={"values": [[first_value]]}, timeout=30
        )
        verify.raise_for_status()
        return {"status": "ready", "write_back": True}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Google Sheet ยังเขียนไม่ได้: {exc}")


def query_pending_waves():
    """Latest 50 Waves from Member Data, newest recorded first (no extra Sheet I/O)."""
    def history_sort_key(row):
        try:
            return datetime.datetime.strptime(
                f"{str(row.get('date') or '').strip()} {str(row.get('time') or '').strip()}",
                "%d/%m/%Y %H:%M",
            )
        except ValueError:
            return datetime.datetime.min

    # Member Data อาจมี Wave เก่าที่กรอกเลขผิด จึงเรียงตามวัน/เวลาบันทึกจริง
    # และคืนแต่ละ Wave เพียงครั้งเดียว แทนการเรียงจากเลข Wave อย่างเดียว
    recent_rows = sorted(load_member_history().values(), key=history_sort_key, reverse=True)
    wave_ids = []
    seen = set()
    for row in recent_rows:
        wave = str(row.get("wave") or "").strip()
        if not wave.isdigit() or wave in seen:
            continue
        seen.add(wave)
        wave_ids.append(int(wave))
        if len(wave_ids) >= 50:
            break
    return {
        "success": True,
        "waves": [{"wave_no": f"{wave_id:010d}"} for wave_id in wave_ids],
        "cached": False,
        "source": "Member Data",
    }


def _startup_warm_cache():
    """Free plan: warm every Sheet cache in one background flow so the first request is fast."""
    # 1) Member Data first: every document read depends on it, so loading it here
    #    means a request arriving during warm-up waits once instead of twice.
    try:
        load_member_history()
    except Exception as exc:
        print(f"⚠️ Member Data warm-up skipped: {exc}")

    # 2) Pending waves are derived from the history already in memory (no extra I/O).
    try:
        data = query_pending_waves()
        with pending_waves_cache_lock:
            pending_waves_cache["data"] = data
            pending_waves_cache["expires_at"] = time.time() + PENDING_WAVES_CACHE_TTL_SECONDS
        print(f"✅ Pending waves warm | {len(data.get('waves') or [])} waves")
    except Exception as exc:
        print(f"⚠️ Pending waves warm-up skipped: {exc}")

    # 3) Everything the first Wave/Booking search would otherwise load on demand.
    for label, warm in (
        ("Booking & Wave", lambda: load_booking_wave_sheet_meta(force=True)),
        ("UAT event tabs", ensure_uat_event_sheets),
        ("branch moves", get_booking_branch_assignments),
        ("branch splits", get_booking_branch_splits),
        ("pick dates", lambda: load_wave_monitoring_pick_dates(force=True)),
    ):
        try:
            warm()
        except Exception as exc:
            # A cold start must never fail because one Sheet is briefly unreachable.
            print(f"⚠️ {label} warm-up skipped: {exc}")
    print("✅ Startup warm-up complete (Google Sheets only)")


@app.on_event("startup")
async def startup_event():
    """Free plan: answer the health check immediately, then warm Sheet caches in background."""
    ensure_report_sync_worker_started()
    threading.Thread(target=_startup_warm_cache, daemon=True, name="startup-warm").start()

def run_pending_waves_refresh_in_background():
    global is_refreshing_pending_waves
    try:
        data = query_pending_waves()
        with pending_waves_cache_lock:
            pending_waves_cache["data"] = data
            pending_waves_cache["expires_at"] = time.time() + PENDING_WAVES_CACHE_TTL_SECONDS
    except Exception as e:
        print(f"🚨 Background refresh pending waves error: {e}")
    finally:
        with is_refreshing_pending_waves_lock:
            is_refreshing_pending_waves = False

# 🚀 [API] โหลดรายการ Wave
@app.get("/api/pending-waves")
def get_pending_waves(background_tasks: BackgroundTasks, force: bool = False):
    global is_refreshing_pending_waves
    now = time.time()
    with pending_waves_cache_lock:
        cached_data = pending_waves_cache["data"]
        is_fresh = pending_waves_cache["expires_at"] > now

    if cached_data and not force:
        response = {**cached_data, "cached": True, "stale": not is_fresh}
        if not is_fresh:
            should_spawn = False
            with is_refreshing_pending_waves_lock:
                if not is_refreshing_pending_waves:
                    is_refreshing_pending_waves = True
                    should_spawn = True
            if should_spawn:
                background_tasks.add_task(run_pending_waves_refresh_in_background)
        return response

    # Force load or no cache
    with is_refreshing_pending_waves_lock:
        is_refreshing_pending_waves = True
    try:
        data = query_pending_waves()
        with pending_waves_cache_lock:
            pending_waves_cache["data"] = data
            pending_waves_cache["expires_at"] = time.time() + PENDING_WAVES_CACHE_TTL_SECONDS
        return data
    except Exception as e:
        if cached_data:
            return {**cached_data, "cached": True, "stale": True, "error": str(e)}
        return {"success": False, "error": str(e)}
    finally:
        with is_refreshing_pending_waves_lock:
            is_refreshing_pending_waves = False

