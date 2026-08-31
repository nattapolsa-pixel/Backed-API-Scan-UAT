from fastapi import FastAPI, HTTPException, BackgroundTasks, Response
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from typing import List, Optional       # ✅ แก้ไข #1: เพิ่ม Optional
from google.cloud import bigquery
import google.auth
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
import csv
import json
import base64
import io
import math
import os
import re
import time
import copy
import uuid
import datetime
import urllib.parse
import urllib.request
import threading
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# ✅ GZip Compression: ลด payload size สำหรับ response ขนาดใหญ่ (Wave data)
app.add_middleware(GZipMiddleware, minimum_size=512)  # ✅ Standard: compress aggressively from 512 bytes

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

if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    local_key_path = os.path.join(os.path.dirname(__file__), "bq-key.json")
    if os.path.exists(local_key_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_key_path

# UAT must not initialize a BigQuery client at all.  Google credentials are
# loaded lazily by get_sheets_session() only when a Sheet operation is needed.
APP_ENV = os.environ.get("APP_ENV", "uat").strip().lower()
APP_VERSION = os.environ.get("APP_VERSION", "1.2.5-uat").strip()
UAT_SHEETS_ONLY = os.environ.get("UAT_SHEETS_ONLY", "true").strip().lower() in ("1", "true", "yes", "on")
SCAN_FEATURE_ENABLED = os.environ.get("SCAN_FEATURE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")

client = None if UAT_SHEETS_ONLY else bigquery.Client(
    project=os.environ.get("GOOGLE_CLOUD_PROJECT", "pro-analytics-db")
)

# ✅ BigQuery Job Timeout: Standard Plan 1 CPU + 2 GB RAM → 60 วินาที
BQ_JOB_TIMEOUT_SECONDS = 60

# 🔒 QC Feature Toggle: ตั้ง False เพื่อ Hold ระบบ QC ไว้ก่อน
#    เปลี่ยนเป็น True เมื่อต้องการเปิดใช้งานระบบ QC
QC_FEATURE_ENABLED = False

# UAT Google Sheets migration. Production code lives in a separate worktree/branch.

NUMERIC_BRANCH_MASTER_SPREADSHEET_ID = "1zI5YAq0JvlM-WsaCfDVYVZgiCn5pWx_HVJjQMiTFwoI"
NUMERIC_BRANCH_MASTER_SHEET_NAME = "Master"
NUMERIC_BRANCH_MASTER_GID = "606346592"
NUMERIC_BRANCH_MASTER_CACHE_TTL_SECONDS = 30 * 60
numeric_branch_master_cache = {"expires_at": 0.0, "data": {}}
numeric_branch_master_lock = Lock()

# ไฟล์ Control Outbound ที่ใช้งานจริง (Member Data เป็นแท็บแรก)
MEMBER_HISTORY_SPREADSHEET_ID = "1MO3lu1GssPZZvaruwQ5trUB045dzh4HUHdH35mbyOtc"
MEMBER_HISTORY_GID = "1628470483"
MEMBER_HISTORY_CACHE_TTL_SECONDS = 10 * 60
member_history_cache = {"expires_at": 0.0, "data": {}}
member_history_lock = Lock()
member_history_refresh_lock = Lock()
member_history_row_cache = {"expires_at": 0.0, "existing_map": {}, "last_data_row": 1}

DELIVERY_REPORT_SPREADSHEET_ID = "14kBtY2tdMXi3I9rbNleokmyJ_WWGRmKXPPU2VaVstZQ"
DELIVERY_REPORT_SHEET_NAME = "Delivery report"
# The legacy transport workbook is a read-only lookup source in UAT.
# Keep Sheet3/Data Booking&Car available to the web, but never write back to
# any tab in this spreadsheet—even if an old Render env var still exists.
DELIVERY_REPORT_READ_ONLY = True
LEGACY_DELIVERY_REPORT_SYNC_ENABLED = False
# UAT only: isolated reconciliation target.  This is deliberately separate
# from the live Delivery report while the direct-write path is being verified.
UAT_REPORT_TEST_SPREADSHEET_ID = os.environ.get(
    "UAT_REPORT_TEST_SPREADSHEET_ID", "1Am1cC8ORHgRfbyA_kfBEWQpDQZlKm1Ii8-wsx39o4xQ"
)
UAT_REPORT_TEST_SHEET_NAME = "Delivery report"
UAT_REPORT_TEST_SHEET_ID = 0
DELIVERY_SOURCE_SHEET_NAME = "วางข้อมูล"
DELIVERY_SOURCE_SHEET_ID = 0
DELIVERY_REPORT_SHEET_ID = 1686001204
DELIVERY_CAR_SHEET_NAME = "Data Booking&Car"
DELIVERY_BRANCH_SHEET_NAME = "Sheet3"
DELIVERY_BRANCH_SHEET_GID = "500149916"
BRANCH_MASTER_SPREADSHEET_ID = "18-gD0iSI3ivMijKQi54Ds-7Gm2p-LFyovjEs1MelrKQ"
BRANCH_MASTER_SHEET_NAME = "ข้อมูลสาขา"
WAVE_MONITORING_SPREADSHEET_ID = "1TL-tj-BrvYM7i_wNHlA0x641_VOqfT9SLpmm2NZATOo"
WAVE_MONITORING_SHEET_NAME = "Wave_Monitoring"
WAVE_MONITORING_SHEET_GID = "0"
WAVE_MONITORING_CACHE_TTL_SECONDS = 5 * 60
delivery_report_lock = Lock()
delivery_lookup_cache = {"expires_at": 0.0, "cars": {}, "branches": {}}
branch_province_cache = {"expires_at": 0.0, "data": {}}
branch_province_refresh_lock = Lock()
branch_report_cache = {"expires_at": 0.0, "data": {}}
branch_report_refresh_lock = Lock()
wave_monitoring_pick_date_cache = {"expires_at": 0.0, "exact": {}, "waves": {}, "branches": {}}
wave_monitoring_pick_date_lock = Lock()
delivery_report_row_cache = {"expires_at": 0.0, "existing_map": {}, "last_data_row": 1}
uat_report_test_row_cache = {"expires_at": 0.0, "existing_map": {}, "last_data_row": 1}
SHEET_ROW_CACHE_TTL_SECONDS = 10 * 60  # ✅ Standard: เพิ่ม cache row เป็น 10 นาที ลด API round-trips
SHEETS_HTTP_TIMEOUT = (3, 45)           # ✅ Standard: connect 3s (เร็วกว่า), read 45s (รองรับ large sheet)
_sheets_session_local = threading.local()

# ฐานข้อมูลเหตุการณ์ของ UAT (แทนตาราง BigQuery ที่เคยรับข้อมูลเขียนจากหน้าเว็บ)
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
}
uat_event_sheet_lock = Lock()
uat_event_sheets_ready = False
uat_event_cache = {}
UAT_EVENT_CACHE_TTL_SECONDS = 30  # ✅ Standard: UAT event cache 30s เพื่อลด Sheets API calls
uat_event_read_lock = Lock()

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
    elif client is not None:
        credentials = client._credentials
        if hasattr(credentials, "with_scopes"):
            credentials = credentials.with_scopes(scopes)
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


def read_uat_event_records(sheet_name: str, force: bool = False) -> list:
    now = time.time()
    cached = uat_event_cache.get(sheet_name)
    if cached and not force and cached["expires_at"] > now:
        return copy.deepcopy(cached["records"])
    # Single-flight: คำขอ Wave/Booking ที่เข้าพร้อมกันใช้ผลโหลด Sheet ชุดเดียวกัน
    # แทนการยิง Google Sheets ซ้ำคนละ thread ตอน cache หมดอายุ
    with uat_event_read_lock:
        now = time.time()
        cached = uat_event_cache.get(sheet_name)
        if cached and not force and cached["expires_at"] > now:
            return copy.deepcopy(cached["records"])
        ensure_uat_event_sheets()
        headers = UAT_EVENT_SHEETS[sheet_name]
        rows = _sheet_values(get_sheets_session(), UAT_DATABASE_SPREADSHEET_ID, f"'{sheet_name}'!A:{chr(64 + len(headers))}")
        records = []
        for row in rows[1:]:
            values = list(row) + [""] * max(0, len(headers) - len(row))
            records.append(dict(zip(headers, values[:len(headers)])))
        uat_event_cache[sheet_name] = {"records": records, "expires_at": now + UAT_EVENT_CACHE_TTL_SECONDS}
        return copy.deepcopy(records)


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

    with member_history_lock:
        member_history_cache["expires_at"] = 0
    print(f"⚡ Member Data BATCH updated | {len(batch_data)} branches in 1 request")

def write_member_history_summary(summary: dict):
    """Upsert one completed Wave+Branch row in Member Data."""
    write_member_history_summaries([summary])

def _sheet_values(session, spreadsheet_id: str, a1_range: str) -> list:
    encoded = urllib.parse.quote(a1_range, safe="")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded}"
    response = session.get(url, timeout=SHEETS_HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json().get("values") or []

def load_delivery_lookup_maps(session) -> tuple:
    now = time.time()
    with delivery_report_lock:
        if delivery_lookup_cache["expires_at"] > now:
            return delivery_lookup_cache["cars"], delivery_lookup_cache["branches"]
        car_rows = _sheet_values(session, DELIVERY_REPORT_SPREADSHEET_ID, f"'{DELIVERY_CAR_SHEET_NAME}'!A:H")
        branch_rows = _sheet_values(session, DELIVERY_REPORT_SPREADSHEET_ID, f"'{DELIVERY_BRANCH_SHEET_NAME}'!A:F")
        cars = {}
        for row in car_rows[1:]:
            row = list(row) + [""] * max(0, 8 - len(row))
            booking = str(row[2] or "").strip().upper()
            if booking:
                cars[booking] = {"carrier": str(row[5] or "").strip(), "driver": str(row[6] or "").strip(),
                                 "plate": str(row[7] or "").strip()}
        branches = {}
        for row in branch_rows[1:]:
            row = list(row) + [""] * max(0, 6 - len(row))
            code = str(row[0] or "").strip().upper()
            if code:
                branches[code] = {"province": str(row[3] or "").strip(), "region": str(row[5] or "").strip()}
        delivery_lookup_cache.update({"expires_at": now + 600, "cars": cars, "branches": branches})
        return cars, branches


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
            request = urllib.request.Request(url, headers={"User-Agent": "Pro-Scanner-UAT/1.0"})
            with urllib.request.urlopen(request, timeout=15) as response:
                csv_text = response.read().decode("utf-8-sig")
            csv_rows = list(csv.reader(io.StringIO(csv_text)))
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
            request = urllib.request.Request(url, headers={"User-Agent": "Pro-Scanner-UAT/1.0"})
            with urllib.request.urlopen(request, timeout=15) as response:
                rows = list(csv.reader(io.StringIO(response.read().decode("utf-8-sig"))))
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
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                text = resp.read().decode("utf-8")
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
                            wave_map[wave_id] = entry
                            wave_map[f"{int(wave_id):010d}"] = entry

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

def get_sheet_meta_for_wave(wave_no: str, force: bool = False) -> dict:
    clean_wave = re.sub(r"\D", "", str(wave_no or ""))
    if not clean_wave:
        return {}
    _, wave_map = load_booking_wave_sheet_meta(force=force)
    return wave_map.get(str(int(clean_wave))) or {}

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
        query = urllib.parse.urlencode({
            "gid": WAVE_MONITORING_SHEET_GID,
            "tqx": "out:csv",
            "tq": "select A,B,I,K where A is not null and B is not null",
        })
        url = f"https://docs.google.com/spreadsheets/d/{WAVE_MONITORING_SPREADSHEET_ID}/gviz/tq?{query}"
        request = urllib.request.Request(url, headers={"User-Agent": "Pro-Scanner-UAT/1.0"})
        with urllib.request.urlopen(request, timeout=15) as response:
            rows = list(csv.reader(io.StringIO(response.read().decode("utf-8-sig"))))
        for row in rows[1:]:
            row = list(row) + [""] * max(0, 4 - len(row))
            pick_date = _parse_pick_date(row[1])
            booking = re.sub(r"\s+", "", str(row[2] or "").upper())
            branch = str(row[3] or "").strip().upper()
            if not pick_date:
                continue
            for wave in _wave_tokens(row[0]):
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
MANUAL_OVERRIDE_EMP_PREFIX = "MANUAL_OVERRIDE|"

def get_document_overrides_for_wave(wave_no: str) -> dict:
    clean_wave = re.sub(r"\D", "", str(wave_no or ""))
    if not clean_wave:
        return {}
    wave_key = str(int(clean_wave))
    with document_overrides_lock:
        if wave_key in document_overrides_overlay:
            # Intentional zero overrides must survive refresh and restart.
            return {
                branch: copy.deepcopy(value)
                for branch, value in document_overrides_overlay[wave_key].items()
            }

    overrides = {}
    try:
        rows = [row for row in read_uat_event_records("Document Overrides")
                if re.sub(r"\D", "", str(row.get("Wave_Number") or "")) == clean_wave]
        seen_branches = set()
        for row in reversed(rows):
            action = str(row.get("Action") or "").strip().upper()
            if action == "RESET_ALL" or str(row.get("Branch_Code") or "").strip().upper() == "RESET_ALL":
                break
            branch = str(row.get("Branch_Code") or "").strip().upper()
            if not branch or branch in seen_branches:
                continue
            seen_branches.add(branch)
            overrides[branch] = {
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
        with document_overrides_lock:
            document_overrides_overlay[wave_key] = copy.deepcopy(overrides)
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
    if UAT_SHEETS_ONLY:
        meta = get_sheet_meta_for_wave(wave)
        resolved_booking = str(booking or meta.get("booking") or "").strip().upper()
        return {
            **meta,
            "pick_date": get_wave_monitoring_pick_date(wave, resolved_booking),
            "booking": resolved_booking,
        }
    query = """
        SELECT MAX(Planned_Pick_Date) AS planned_pick_date,
               COALESCE(MAX(NULLIF(TRIM(Vehicle_Booking_No), '')), '') AS booking_no
        FROM `pro-analytics-db.logistics_db.wave_summary_fast`
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave
    """
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("wave", "INT64", int(wave))])
    row = next(iter(client.query(query, job_config=config).result(timeout=BQ_JOB_TIMEOUT_SECONDS)), None)
    return {"pick_date": row["planned_pick_date"] if row else None,
            "booking": str(row["booking_no"] or "").strip().upper() if row else ""}

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

def _date_serial(value):
    return (value - datetime.date(1899, 12, 30)).days if isinstance(value, datetime.date) else ""


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
        print(f"⚡ UAT test Delivery report updated | {len(batch_data)} branches")


def write_delivery_report_summaries(summaries: list):
    """Blocked in UAT: the legacy transport workbook is lookup-only."""
    if DELIVERY_REPORT_READ_ONLY:
        raise RuntimeError("legacy transport workbook is read-only in UAT")
    if not LEGACY_DELIVERY_REPORT_SYNC_ENABLED:
        raise RuntimeError("legacy Delivery report/staging sync is disabled")
    if not summaries:
        return
    session = get_sheets_session()
    cars, branches = load_delivery_lookup_maps(session)
    now_bkk = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))
    
    with delivery_report_lock:
        now = time.time()
        if delivery_report_row_cache["existing_map"] and delivery_report_row_cache["expires_at"] > now:
            existing_map = dict(delivery_report_row_cache["existing_map"])
            last_data_row = int(delivery_report_row_cache["last_data_row"] or 1)
        else:
            existing = _sheet_values(session, DELIVERY_REPORT_SPREADSHEET_ID, f"'{DELIVERY_SOURCE_SHEET_NAME}'!A:V")
            existing_map = {}
            last_data_row = 1
            for index in range(len(existing), 1, -1):
                row = list(existing[index - 1]) + [""] * max(0, 22 - len(existing[index - 1]))
                row_wave = row[2] if len(row) > 2 else ""
                row_branch = row[3] if len(row) > 3 else ""
                row_booking = str(row[18] or "").strip().upper()
                if _is_valid_wave_branch(row_wave, row_branch):
                    if index > last_data_row:
                        last_data_row = index
                    wave_digits = re.sub(r"\D", "", str(row_wave or ""))
                    branch_clean = str(row_branch or "").strip().upper()
                    wave_branch = (str(int(wave_digits)), branch_clean)
                    exact_key = (row_booking, *wave_branch)
                    existing_map.setdefault(exact_key, index)
                    existing_map.setdefault(("", *wave_branch), index)

        batch_data = []
        formula_requests = []
        current_append_row = last_data_row + 1
        meta_cache = {}

        for summary in summaries:
            wave = str(int(str(summary["wave"]).strip()))
            branch = str(summary["branch"] or "").strip().upper()
            if wave not in meta_cache:
                meta_cache[wave] = get_delivery_wave_meta(wave)
            meta = meta_cache[wave]

            pick_date = meta.get("pick_date") or now_bkk.date()
            order_date, delivery_date = delivery_business_dates(pick_date)
            booking = str(summary.get("booking") or meta.get("booking") or "").strip().upper()
            car = cars.get(booking, {})
            key = (booking, wave, branch)
            legacy_key = ("", wave, branch)

            if key in existing_map:
                target_row = existing_map[key]
            elif not summary.get("booking_split") and legacy_key in existing_map:
                target_row = existing_map[legacy_key]
            else:
                # Zero corrections update existing rows only; they never create new zero reports.
                if int(summary.get("total") or 0) <= 0:
                    print(f"Delivery zero update skipped (no existing row) | {booking}/{wave}/{branch}")
                    continue
                target_row = current_append_row
                existing_map[key] = target_row
                current_append_row += 1

            core_values = [[
                _date_serial(now_bkk.date()), now_bkk.strftime("%H:%M:%S"), wave, branch,
                clean_branch_display_name(summary.get("branch_name")), member_data_bu(summary.get("bu")), "", "",
                _history_int(summary.get("m")), _history_int(summary.get("red")), _history_int(summary.get("blue")),
                _history_int(summary.get("green")), _history_int(summary.get("black")), _history_int(summary.get("total")),
                _history_int(summary.get("pallet")), "Outbound", ""
            ]]
            car_values = [[
                booking, car.get("carrier", ""), car.get("driver", ""), car.get("plate", "")
            ]]
            date_values = [[
                _date_serial(order_date), _date_serial(pick_date), _date_serial(delivery_date)
            ]]

            batch_data.append({"range": f"'{DELIVERY_SOURCE_SHEET_NAME}'!A{target_row}:Q{target_row}", "values": core_values})
            batch_data.append({"range": f"'{DELIVERY_SOURCE_SHEET_NAME}'!S{target_row}:V{target_row}", "values": car_values})
            batch_data.append({"range": f"'{DELIVERY_SOURCE_SHEET_NAME}'!X{target_row}:Z{target_row}", "values": date_values})

            if target_row > 2:
                formula_requests.append({"copyPaste": {
                    "source": {"sheetId": DELIVERY_REPORT_SHEET_ID, "startRowIndex": target_row - 2,
                               "endRowIndex": target_row - 1, "startColumnIndex": 0, "endColumnIndex": 20},
                    "destination": {"sheetId": DELIVERY_REPORT_SHEET_ID, "startRowIndex": target_row - 1,
                                    "endRowIndex": target_row, "startColumnIndex": 0, "endColumnIndex": 20},
                    "pasteType": "PASTE_FORMULA", "pasteOrientation": "NORMAL"
                }})

        if not batch_data:
            return

        # Send 1 single values:batchUpdate request
        base = f"https://sheets.googleapis.com/v4/spreadsheets/{DELIVERY_REPORT_SPREADSHEET_ID}"
        response = session.post(f"{base}/values:batchUpdate", json={"valueInputOption": "RAW", "data": batch_data}, timeout=SHEETS_HTTP_TIMEOUT)
        response.raise_for_status()

        delivery_report_row_cache["existing_map"] = dict(existing_map)
        delivery_report_row_cache["last_data_row"] = max(last_data_row, current_append_row - 1)
        delivery_report_row_cache["expires_at"] = time.time() + SHEET_ROW_CACHE_TTL_SECONDS

        if formula_requests:
            try:
                copied = session.post(f"{base}:batchUpdate", json={"requests": formula_requests}, timeout=SHEETS_HTTP_TIMEOUT)
                copied.raise_for_status()
            except Exception as fe:
                print(f"⚠️ Formula copy warning: {fe}")

        print(f"⚡ Delivery source/report BATCH updated | {len(summaries)} branches in 1 request")

def write_delivery_report_summary(summary: dict):
    """Upsert one Wave+Branch into the source tab; Delivery report remains formula-driven."""
    write_delivery_report_summaries([summary])

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
    override = (get_document_overrides_for_wave(wave) or {}).get(branch) or {}
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


def report_summary_has_boxes(summary: dict) -> bool:
    return int(normalize_report_summary(summary).get("total") or 0) > 0


def get_durable_close_summaries(wave: str, branches) -> dict:
    """Read the exact positive totals saved atomically with CLOSE_JOB."""
    clean_branches = sorted({str(branch or "").strip().upper() for branch in branches or [] if branch})
    if not clean_branches:
        return {}
    try:
        query = """
            WITH LatestClose AS (
              SELECT
                TRIM(UPPER(CAST(Branch_Code AS STRING))) AS branch,
                CAST(Color AS STRING) AS payload,
                SAFE_CAST(Timestamp AS TIMESTAMP) AS closed_at
              FROM `pro-analytics-db.logistics_db.app_scan_transactions`
              WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave
                AND UPPER(TRIM(CAST(Scan_Type AS STRING))) = 'CLOSE_SUMMARY'
                AND TRIM(UPPER(CAST(Branch_Code AS STRING))) IN UNNEST(@branches)
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRIM(UPPER(CAST(Branch_Code AS STRING)))
                ORDER BY SAFE_CAST(Timestamp AS TIMESTAMP) DESC, CAST(LPN AS STRING) DESC
              ) = 1
            )
            SELECT c.*,
              EXISTS (
                SELECT 1
                FROM `pro-analytics-db.logistics_db.app_scan_transactions` t
                WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(t.Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave
                  AND TRIM(UPPER(CAST(t.Branch_Code AS STRING))) = c.branch
                  AND SAFE_CAST(t.Timestamp AS TIMESTAMP) > c.closed_at
                  AND (
                    UPPER(TRIM(CAST(t.Scan_Type AS STRING))) IN ('RESET_BOX', 'CANCEL_COMBINE')
                    OR STARTS_WITH(UPPER(TRIM(CAST(t.Scan_Type AS STRING))), 'CORRECTION|')
                  )
              ) AS has_post_close_correction
            FROM LatestClose c
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("wave", "INT64", int(str(wave))),
            bigquery.ArrayQueryParameter("branches", "STRING", clean_branches),
        ])
        durable = {}
        for row in client.query(query, job_config=job_config).result(timeout=BQ_JOB_TIMEOUT_SECONDS):
            branch = str(row["branch"] or "").strip().upper()
            try:
                payload = json.loads(str(row["payload"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            summary = normalize_report_summary(payload, str(wave), branch)
            if summary.get("total", 0) > 0:
                summary["_closed_at"] = row["closed_at"].isoformat() if row["closed_at"] else ""
                summary["_has_post_close_correction"] = bool(row["has_post_close_correction"])
                durable[branch] = summary
        return durable
    except Exception as exc:
        print(f"⚠️ CLOSE SUMMARY READ skipped | Wave: {wave} | {exc}")
        return {}


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
    """Queue a server-side recalculation from BigQuery after streaming rows become queryable."""
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
        # One fresh BigQuery query per Wave, even when many branches changed together.
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
        durable_summaries = get_durable_close_summaries(wave, requested)
        for branch in sorted(requested):
            branch_items = [item for item in fresh.get("lpn_list", [])
                            if str(item.get("branch") or "").strip().upper() == branch]
            # Google Sheets เป็นรายงานงานที่ปิดจบแล้วเท่านั้น ห้ามสร้างแถว 0/ยอดระหว่างทำงาน
            if not any(item.get("branch_closed_at") for item in branch_items):
                continue
            calculated = normalize_report_summary(summarize_branch_for_member_data(fresh, branch), wave, branch)
            durable = durable_summaries.get(branch)
            # CLOSE_SUMMARY คือยอดที่เห็นจริงตอนกดปิดและเก็บถาวรพร้อม CLOSE_JOB
            # ใช้เป็น fallback/กันข้อมูล BigQuery ที่เข้าช้าหรือหายบางส่วน แล้วให้ manual override ชนะท้ายสุด
            if (durable and not durable.get("_has_post_close_correction")
                    and int(durable.get("total") or 0) > int(calculated.get("total") or 0)):
                summary = {**calculated, **durable, "wave": wave, "branch": branch, "is_closed": True}
            else:
                summary = calculated
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
            # Snapshot gives users a fast update. Reconciliation follows from durable BigQuery state.
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

def load_member_history() -> dict:
    """Member Data is a completed branch summary, keyed by numeric Wave + branch."""
    now = time.time()
    with member_history_lock:
        cached = member_history_cache.get("data") or {}
        if cached and member_history_cache.get("expires_at", 0) > now:
            return cached
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
        history = {}
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=45) as response:
                rows = csv.reader(io.StringIO(response.read().decode("utf-8-sig")))
                next(rows, None)
                for row in rows:
                    row = list(row) + [""] * max(0, 16 - len(row))
                    wave_digits = re.sub(r"\D", "", row[2])
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
            with member_history_lock:
                member_history_cache["data"] = history
                member_history_cache["expires_at"] = now + MEMBER_HISTORY_CACHE_TTL_SECONDS
            print(f"✅ Member Data history loaded: {len(history)} branch summaries")
            return history
        except Exception as exc:
            print(f"⚠️ Member Data history load failed (non-critical): {exc}")
            with member_history_lock:
                member_history_cache["expires_at"] = now + 60
            return cached

def build_member_history_items(wave_no: str) -> list:
    wave = str(int(str(wave_no).strip()))
    rows = [row for (row_wave, _), row in load_member_history().items() if row_wave == wave]
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
    overrides = get_document_overrides_for_wave(wave)
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
    meta = get_sheet_meta_for_wave(wave)
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
        "scan_feature_enabled": False,
    }

DIRECT_QTY_PREFIXES = ("PP", "SP")
PACK_CASE_MAP_PATH = os.path.join(os.path.dirname(__file__), "pack_case_map.json")
pack_case_map_cache = None
pack_case_map_lock = Lock()

def is_direct_qty_lpn_value(lpn: str) -> bool:
    return str(lpn or "").strip().upper().startswith(DIRECT_QTY_PREFIXES)

def load_pack_case_map() -> dict:
    """สร้างแมป 'จำนวนชิ้นต่อลัง (CASECNT)' จาก BigQuery (แคชไว้จนกว่าจะรีสตาร์ท)
    - key แบบ 'OWNER|SKU'  -> casecnt  (ผ่าน master_picktype_native: Owner+SKU -> PACKKEY -> CASECNT)
    - key แบบ 'PRODUCT_CODE' -> casecnt  (ทางตรง: Product_Code = PACKKEY)
    calculate_direct_total_qty จะลองหา OWNER|CODE ก่อน ไม่เจอค่อยใช้ CODE ตรงๆ
    """
    global pack_case_map_cache
    if pack_case_map_cache is not None:
        return pack_case_map_cache

    with pack_case_map_lock:
        if pack_case_map_cache is not None:
            return pack_case_map_cache
        case_map = {}
        try:
            # 1) master_product_native: PACKKEY (string_field_1) -> CASECNT (string_field_7)
            product_case = {}
            prod_sql = """
                SELECT UPPER(TRIM(CAST(string_field_1 AS STRING))) AS packkey,
                       SAFE_CAST(string_field_7 AS FLOAT64) AS casecnt
                FROM `pro-analytics-db.logistics_db.master_product_native`
                WHERE string_field_1 NOT IN ('PACKKEY', 'Pack')
            """
            for r in client.query(prod_sql).result(timeout=BQ_JOB_TIMEOUT_SECONDS):
                pk = str(get_row_value(r, "packkey", "") or "").strip().upper()
                cc = to_float(get_row_value(r, "casecnt", 0))
                if pk and cc > 0:
                    product_case[pk] = cc
                    case_map[pk] = cc  # ทางตรง: Product_Code = PACKKEY

            # 2) master_picktype_native: (Owner=field_1, SKU=field_2) -> PACKKEY (field_4)
            pick_sql = """
                SELECT UPPER(TRIM(CAST(string_field_1 AS STRING))) AS owner,
                       UPPER(TRIM(CAST(string_field_2 AS STRING))) AS sku,
                       UPPER(TRIM(CAST(string_field_4 AS STRING))) AS packkey
                FROM `pro-analytics-db.logistics_db.master_picktype_native`
                WHERE string_field_1 NOT IN ('STORERKEY', 'Owner')
            """
            for r in client.query(pick_sql).result(timeout=BQ_JOB_TIMEOUT_SECONDS):
                owner = str(get_row_value(r, "owner", "") or "").strip().upper()
                sku = str(get_row_value(r, "sku", "") or "").strip().upper()
                pk = str(get_row_value(r, "packkey", "") or "").strip().upper()
                cc = product_case.get(pk)
                if owner and sku and cc:
                    case_map[f"{owner}|{sku}"] = cc

            pack_case_map_cache = case_map
            print(f"✅ Loaded case-size map from BigQuery: {len(case_map)} keys")
        except Exception as e:
            print(f"⚠️ Case-size map load failed, falling back to Total_Qty: {e}")
            pack_case_map_cache = {}
        return pack_case_map_cache

def get_row_value(row, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default

def clean_branch_display_name(value) -> str:
    """Return a short operational branch name instead of a legal-entity name."""
    name = str(value or "").strip()
    if not name or name.lower() in {"unknown", "null", "none"} or name in {"-", "ไม่ระบุ"}:
        return "Unknown"

    name = re.sub(
        r"^\s*(?:ห้างหุ้นส่วนสามัญนิติบุคคล|ห้างหุ้นส่วนจำกัด|หจก\.?|บริษัทจำกัด|บริษัท|บจก\.?)\s*",
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(
        r"\s*(?:จำกัด\s*\(มหาชน\)|จำกัด|\(มหาชน\)|มหาชน|บจก\.?|หจก\.?)\s*$",
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"\s+", " ", name).strip(" -")
    return name or "Unknown"

def is_numeric_branch_code(value) -> bool:
    branch_code = str(value or "").strip()
    return bool(branch_code) and branch_code[0].isdigit()

def _build_numeric_branch_map(rows) -> dict:
    branch_map = {}
    for row in rows or []:
        if not row or len(row) < 2:
            continue
        branch_code = str(row[0] or "").strip()
        branch_name = clean_branch_display_name(row[1])
        if is_numeric_branch_code(branch_code) and branch_name != "Unknown":
            # Master อาจมีรหัสซ้ำ ให้แถวล่าสุดในชีตเป็นค่าหลัก
            branch_map[branch_code] = branch_name
    return branch_map

def load_numeric_branch_master() -> dict:
    """Load numeric branch names from Master!B:C and cache them for 30 minutes."""
    now = time.time()
    with numeric_branch_master_lock:
        cached_data = numeric_branch_master_cache.get("data") or {}
        if cached_data and numeric_branch_master_cache.get("expires_at", 0) > now:
            return cached_data

        branch_map = {}
        try:
            sheet_name = urllib.parse.quote(NUMERIC_BRANCH_MASTER_SHEET_NAME)
            csv_url = (
                f"https://docs.google.com/spreadsheets/d/{NUMERIC_BRANCH_MASTER_SPREADSHEET_ID}/gviz/tq"
                f"?tqx=out:csv&gid={NUMERIC_BRANCH_MASTER_GID}&sheet={sheet_name}&range=B:C"
            )
            request = urllib.request.Request(csv_url, headers={"User-Agent": "Pro-LPN-Scanner/1.0"})
            with urllib.request.urlopen(request, timeout=15) as response:
                csv_text = response.read().decode("utf-8-sig")
            branch_map = _build_numeric_branch_map(csv.reader(io.StringIO(csv_text)))
        except Exception as public_error:
            print(f"Numeric branch Master public CSV unavailable: {public_error}")

        if not branch_map:
            try:
                from google.oauth2 import service_account
                from google.auth.transport.requests import Request as GoogleAuthRequest

                credentials = service_account.Credentials.from_service_account_file(
                    "bq-key.json",
                    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
                )
                credentials.refresh(GoogleAuthRequest())
                range_name = urllib.parse.quote(f"{NUMERIC_BRANCH_MASTER_SHEET_NAME}!B:C", safe="")
                api_url = (
                    f"https://sheets.googleapis.com/v4/spreadsheets/{NUMERIC_BRANCH_MASTER_SPREADSHEET_ID}"
                    f"/values/{range_name}?majorDimension=ROWS&valueRenderOption=FORMATTED_VALUE"
                )
                request = urllib.request.Request(
                    api_url,
                    headers={"Authorization": f"Bearer {credentials.token}"},
                )
                with urllib.request.urlopen(request, timeout=15) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                branch_map = _build_numeric_branch_map(payload.get("values", []))
            except Exception as auth_error:
                print(f"Numeric branch Master authenticated read unavailable: {auth_error}")

        if branch_map:
            numeric_branch_master_cache["data"] = branch_map
            numeric_branch_master_cache["expires_at"] = now + NUMERIC_BRANCH_MASTER_CACHE_TTL_SECONDS
            return branch_map

        # ถ้าชีตขัดข้องชั่วคราว ให้ใช้แคชเดิมแทนและลองโหลดใหม่เร็วขึ้น
        numeric_branch_master_cache["expires_at"] = now + 60
        return cached_data

def to_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def calculate_direct_total_qty(lpn: str, detail_rows, fallback_total) -> int:
    fallback_qty = max(1, int(math.ceil(to_float(fallback_total, 1))))
    if not is_direct_qty_lpn_value(lpn):
        return fallback_qty

    pack_case_map = load_pack_case_map()
    total_qty = 0

    for detail in detail_rows or []:
        owner = str(get_row_value(detail, "owner", "") or "").strip().upper()
        product_code = str(get_row_value(detail, "product_code", "") or "").strip().upper()
        pieces = to_float(get_row_value(detail, "total_pieces", 0))
        row_total_qty = to_float(get_row_value(detail, "row_total_qty", 0))
        case_count = pack_case_map.get(f"{owner}|{product_code}") or pack_case_map.get(product_code)

        if case_count and pieces > 0:
            total_qty += max(1, int(math.ceil(pieces / case_count)))
        elif row_total_qty > 0:
            total_qty += max(1, int(math.ceil(row_total_qty)))
        else:
            total_qty += 1

    return total_qty if total_qty > 0 else fallback_qty

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

# Valid LPNs Validation Cache to prevent slow BigQuery queries on every scan
VALID_LPNS_CACHE_TTL = 1800  # ⚡ 30 นาที (Standard Plan: RAM เพียงพอ เพิ่มจาก 10 นาที)
valid_lpns_cache = {}  # wave_no -> {"lpns": set((lpn, branch_code)), "expires_at": float}
valid_lpns_cache_lock = Lock()

# --- ULTRA-FAST WAVE AND BOOKING SEARCH CACHE ---
WAVE_CACHE_TTL = 1800  # 30 นาที cache (Standard Plan: 2GB RAM เพียงพอ)
WAVE_FORCE_REFRESH_COOLDOWN_SECONDS = 2.0
wave_cache = {}  # wave_detail_str -> {"data": dict, "expires_at": float, "fetched_at": float}
wave_cache_lock = Lock()
wave_query_locks = {}
wave_query_locks_guard = Lock()

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
    """Keep pallet color/submission visible to every handheld before BigQuery cache catches up."""
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

def fetch_wave_data_from_bq(search_wave_id: int) -> dict:
    wave_scan_str = str(search_wave_id)
    wave_detail_str = f"{search_wave_id:010d}"

    # =============================================================
    # ⚡ ข้าม QC CTEs ทั้งหมดเมื่อ QC_FEATURE_ENABLED = False
    # ลด complexity ของ query ลงมากกว่า 50% เมื่อ QC ถูก Hold
    # =============================================================
    if QC_FEATURE_ENABLED:
        qc_ctes = f"""
        QCRaw AS (
            SELECT
                TRIM(string_field_18) AS Order_Number,
                TRIM(string_field_31) AS Product_Code,
                ARRAY_AGG(
                    NULLIF(TRIM(string_field_56), '') IGNORE NULLS
                    ORDER BY string_field_66 DESC LIMIT 1
                )[SAFE_OFFSET(0)] AS Picker,
                MAX(SAFE_CAST(NULLIF(REGEXP_REPLACE(IFNULL(string_field_93, ''), r'[^0-9.-]', ''), '') AS FLOAT64)) AS Unit_Price
            FROM `pro-analytics-db.logistics_db.transaction_raw`
            WHERE SAFE_CAST(REGEXP_REPLACE(IFNULL(string_field_41, ''), r'[^0-9]', '') AS INT64) = {search_wave_id}
            GROUP BY Order_Number, Product_Code
        ),
        QCBase AS (
            SELECT
                TRIM(UPPER(d.LPN)) AS Clean_LPN,
                TRIM(UPPER(d.Branch_Code)) AS Branch_Code,
                TRIM(UPPER(d.Owner)) AS Owner,
                CASE
                    WHEN TRIM(UPPER(d.Owner)) = 'DM02' THEN 'MART'
                    WHEN TRIM(UPPER(d.Owner)) IN ('DP02', 'DG02', 'DS02', 'DO02') THEN 'PUN'
                END AS QC_Group,
                COALESCE(NULLIF(TRIM(UPPER(d.Zone)), ''), 'UNKNOWN') AS Zone,
                COALESCE(NULLIF(TRIM(UPPER(d.Product_Code)), ''), 'UNKNOWN') AS Product_Code,
                COALESCE(NULLIF(TRIM(UPPER(r.Picker)), ''), 'UNKNOWN') AS Picker,
                COALESCE(r.Unit_Price, 0) AS Unit_Price,
                GREATEST(COALESCE(d.Total_Qty, 1), 1) AS Workload
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record` AS d
            LEFT JOIN QCRaw AS r
              ON TRIM(d.Order_Number) = r.Order_Number
             AND TRIM(d.Product_Code) = r.Product_Code
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(d.Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
        ),
        QCZoneMetrics AS (
            SELECT QC_Group, Zone, SUM(Workload) AS Metric
            FROM QCBase WHERE QC_Group IS NOT NULL GROUP BY QC_Group, Zone
        ),
        QCZoneScores AS (
            SELECT *, IF(Zone = 'UNKNOWN', 0.5, CUME_DIST() OVER (PARTITION BY QC_Group ORDER BY Metric)) AS Score
            FROM QCZoneMetrics
        ),
        QCItemMetrics AS (
            SELECT QC_Group, Product_Code, SUM(Workload) AS Metric, AVG(Unit_Price) AS Unit_Price
            FROM QCBase WHERE QC_Group IS NOT NULL GROUP BY QC_Group, Product_Code
        ),
        QCItemScores AS (
            SELECT *,
                IF(Product_Code = 'UNKNOWN', 0.5, CUME_DIST() OVER (PARTITION BY QC_Group ORDER BY Metric)) AS Focus_Score,
                IF(Unit_Price <= 0, 0.5, CUME_DIST() OVER (PARTITION BY QC_Group ORDER BY Unit_Price)) AS Price_Score
            FROM QCItemMetrics
        ),
        QCPickerMetrics AS (
            SELECT QC_Group, Picker, COUNT(*) AS Metric
            FROM QCBase WHERE QC_Group IS NOT NULL GROUP BY QC_Group, Picker
        ),
        QCPickerScores AS (
            SELECT *, IF(Picker = 'UNKNOWN', 0.5, CUME_DIST() OVER (PARTITION BY QC_Group ORDER BY Metric)) AS Score
            FROM QCPickerMetrics
        ),
        QCStoreMetrics AS (
            SELECT QC_Group, Branch_Code, SUM(Workload) AS Metric
            FROM QCBase WHERE QC_Group IS NOT NULL GROUP BY QC_Group, Branch_Code
        ),
        QCStoreScores AS (
            SELECT *, CUME_DIST() OVER (PARTITION BY QC_Group ORDER BY Metric) AS Score
            FROM QCStoreMetrics
        ),
        QCLpnRisk AS (
            SELECT
                b.Clean_LPN,
                b.Branch_Code,
                b.QC_Group,
                AVG(CASE
                    WHEN b.QC_Group = 'MART' THEN (z.Score * 0.20) + (i.Focus_Score * 0.30) + (p.Score * 0.40) + (s.Score * 0.10)
                    WHEN b.QC_Group = 'PUN' THEN (i.Focus_Score + i.Price_Score + p.Score) / 3
                END) AS QC_Risk,
                NOT (b.QC_Group = 'PUN' AND REGEXP_CONTAINS(b.Clean_LPN, r'^(BP|SP)')) AS QC_Eligible
            FROM QCBase AS b
            LEFT JOIN QCZoneScores AS z USING (QC_Group, Zone)
            LEFT JOIN QCItemScores AS i USING (QC_Group, Product_Code)
            LEFT JOIN QCPickerScores AS p USING (QC_Group, Picker)
            LEFT JOIN QCStoreScores AS s USING (QC_Group, Branch_Code)
            WHERE b.QC_Group IS NOT NULL
            GROUP BY b.Clean_LPN, b.Branch_Code, b.QC_Group, QC_Eligible
        ),
        QCRanked AS (
            SELECT *,
                IF(QC_Eligible, ROW_NUMBER() OVER (
                    PARTITION BY QC_Group
                    ORDER BY IF(QC_Eligible, 0, 1), QC_Risk DESC, Clean_LPN, Branch_Code
                ), NULL) AS QC_Rank,
                COUNTIF(QC_Eligible) OVER (PARTITION BY QC_Group) AS Eligible_Count
            FROM QCLpnRisk
        ),
        QCStatus AS (
            SELECT *,
                QC_Eligible AND QC_Rank <= GREATEST(1, CAST(CEIL(Eligible_Count * IF(QC_Group = 'MART', 0.50, 0.60)) AS INT64)) AS QC_Required
            FROM QCRanked
        ),"""
        qc_select = """
            COALESCE(MAX(qc.QC_Required), FALSE) AS qc_required,
            ROUND(MAX(qc.QC_Risk), 4) AS qc_risk,
            MAX(qc.QC_Group) AS qc_source,"""
        qc_join = f"""
        LEFT JOIN QCStatus AS qc
          ON TRIM(UPPER(d.LPN)) = qc.Clean_LPN
         AND TRIM(UPPER(d.Branch_Code)) = qc.Branch_Code"""
    else:
        # 🚀 QC ถูก Hold → ข้าม QC CTEs ทั้งหมด ลด query ลง 50%+
        qc_ctes = ""
        qc_select = """
            FALSE AS qc_required,
            CAST(0.0 AS FLOAT64) AS qc_risk,
            CAST(NULL AS STRING) AS qc_source,"""
        qc_join = ""

    query = f"""
        WITH {qc_ctes}
        ScanRows AS (
            SELECT
                TRIM(CAST(Wave_Number AS STRING)) AS Wave_Number,
                SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS Scan_Wave_ID,
                TRIM(UPPER(LPN)) AS Clean_LPN,
                Qty,
                Scan_Type,
                Color,
                TRIM(UPPER(IFNULL(Branch_Code, ''))) AS Scan_Branch,
                TRIM(IFNULL(Emp_ID, '')) AS Emp_ID,
                IFNULL(Pallet_No, 0) AS Pallet_No,
                Timestamp,
                IF(Qty = 0 OR Scan_Type IN ('RESET_BOX', 'CANCEL_COMBINE') OR STARTS_WITH(Scan_Type, 'CORRECTION|'), 1, 0) AS Is_Reset
            FROM `pro-analytics-db.logistics_db.app_scan_transactions`
            -- Wave_Number อาจถูกเก็บเป็น 58903, 0000058903 หรือ WAVE-58903
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
        ),
        LatestReset AS (
            SELECT Scan_Wave_ID, Clean_LPN, Scan_Branch, MAX(Timestamp) AS Reset_Timestamp
            FROM ScanRows
            WHERE Is_Reset = 1
            GROUP BY Scan_Wave_ID, Clean_LPN, Scan_Branch
        ),
        ValidScanRows AS (
            SELECT r.*
            FROM ScanRows AS r
            LEFT JOIN LatestReset AS lr
             ON r.Scan_Wave_ID = lr.Scan_Wave_ID
             AND r.Clean_LPN = lr.Clean_LPN
             AND r.Scan_Branch = lr.Scan_Branch
            WHERE r.Is_Reset = 0
              AND IFNULL(r.Qty, 0) > 0
              AND UPPER(IFNULL(r.Scan_Type, '')) != 'CLOSE_SUMMARY'
              AND (lr.Reset_Timestamp IS NULL OR r.Timestamp > lr.Reset_Timestamp)
        ),
        PalletColorAggregatedScans AS (
            SELECT
                Scan_Wave_ID,
                Clean_LPN,
                Scan_Branch,
                Pallet_No,
                UPPER(IFNULL(Color, 'None')) AS Color_Key,
                ARRAY_AGG(Color ORDER BY Timestamp DESC LIMIT 1)[OFFSET(0)] AS Color,
                ARRAY_AGG(Scan_Type ORDER BY Timestamp DESC LIMIT 1)[OFFSET(0)] AS Scan_Type,
                ARRAY_AGG(Emp_ID ORDER BY Timestamp DESC LIMIT 1)[OFFSET(0)] AS Emp_ID,
                SUM(Qty) AS Qty,
                MAX(Timestamp) AS Max_Timestamp
            FROM ValidScanRows
            GROUP BY Scan_Wave_ID, Clean_LPN, Scan_Branch, Pallet_No, Color_Key
        ),
        ScanHistory AS (
            SELECT
                Scan_Wave_ID,
                Clean_LPN,
                Scan_Branch,
                SUM(Qty) AS Scanned_Qty,
                MAX(Pallet_No) AS Scanned_Pallet_No,
                ARRAY_AGG(Scan_Type ORDER BY Max_Timestamp DESC LIMIT 1)[OFFSET(0)] AS Scan_Type,
                ARRAY_AGG(Color ORDER BY Max_Timestamp DESC LIMIT 1)[OFFSET(0)] AS Color,
                STRING_AGG(DISTINCT NULLIF(TRIM(Emp_ID), ''), ', ' ORDER BY NULLIF(TRIM(Emp_ID), '')) AS Scanner_Emp_IDs,
                STRING_AGG(CONCAT(IFNULL(Color, 'None'), '~', CAST(Qty AS STRING), '~', IFNULL(Scan_Type, '')), '|') AS Color_Breakdown,
                STRING_AGG(CONCAT(CAST(Pallet_No AS STRING), '~', IFNULL(Color, 'None'), '~', CAST(Qty AS STRING), '~', IFNULL(Scan_Type, '')), '|' ORDER BY Pallet_No, Max_Timestamp) AS Pallet_Breakdown
            FROM PalletColorAggregatedScans
            GROUP BY Scan_Wave_ID, Clean_LPN, Scan_Branch
        ),
        PalletSummary AS (
            SELECT
                Scan_Branch,
                ARRAY_AGG(DISTINCT Pallet_No ORDER BY Pallet_No) AS Pallet_Nos
            FROM ValidScanRows
            -- นับเฉพาะพาเลทที่ยังมี LPN อยู่จริงในสถานะล่าสุด
            -- ไม่รวมเลขที่เคยจองแล้วถูกยกเลิกหรือพาเลทที่ถูกแก้จนว่าง
            WHERE IFNULL(Qty, 0) > 0 AND Pallet_No > 0 AND Scan_Branch != ''
            GROUP BY Scan_Branch
        ),
        PalletColorSummary AS (
            SELECT
                Scan_Branch,
                Pallet_No,
                ARRAY_AGG(Color IGNORE NULLS ORDER BY Timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS Pallet_Color
            FROM ScanRows
            WHERE Scan_Type = 'PALLET_START' AND Pallet_No > 0 AND Scan_Branch != ''
            GROUP BY Scan_Branch, Pallet_No
        ),
        SubmittedPalletSummary AS (
            SELECT
                Scan_Branch,
                ARRAY_AGG(DISTINCT Pallet_No ORDER BY Pallet_No) AS Submitted_Pallet_Nos
            FROM ScanRows
            WHERE Scan_Type = 'PALLET_SUBMIT' AND Pallet_No > 0 AND Scan_Branch != ''
            GROUP BY Scan_Branch
        ),
        BranchCloseSummary AS (
            SELECT
                Scan_Branch,
                MAX(Timestamp) AS Branch_Closed_At,
                ARRAY_AGG(Emp_ID ORDER BY Timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS Branch_Closed_By
            FROM ScanRows
            WHERE Scan_Type = 'CLOSE_JOB' AND Scan_Branch != ''
            GROUP BY Scan_Branch
        ),
        WaveMonitoringFiltered AS (
            SELECT
                Branch_Code,
                MAX(Branch_Name) AS Branch_Name,
                Detail_Wave
            FROM (
                SELECT
                    TRIM(Branch_Code) AS Branch_Code,
                    TRIM(Branch_Name) AS Branch_Name,
                    LPAD(CAST(SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS STRING), 10, '0') AS Detail_Wave
                FROM `pro-analytics-db.logistics_db.wave_monitoring`
                WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
            )
            GROUP BY Branch_Code, Detail_Wave
        ),
        WaveBranches AS (
            SELECT DISTINCT TRIM(UPPER(Branch_Code)) AS Branch_Code
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
              AND NULLIF(TRIM(Branch_Code), '') IS NOT NULL
        ),
        BranchNameHistory AS (
            SELECT
                TRIM(UPPER(w.Branch_Code)) AS Branch_Code,
                ARRAY_AGG(
                    NULLIF(TRIM(w.Branch_Name), '') IGNORE NULLS
                    ORDER BY w.Created_At DESC
                    LIMIT 1
                )[SAFE_OFFSET(0)] AS Branch_Name
            FROM `pro-analytics-db.logistics_db.wave_monitoring` AS w
            INNER JOIN WaveBranches AS b
              ON TRIM(UPPER(w.Branch_Code)) = b.Branch_Code
            WHERE NULLIF(TRIM(w.Branch_Name), '') IS NOT NULL
              AND LOWER(TRIM(w.Branch_Name)) != 'unknown'
            GROUP BY Branch_Code
        )
        SELECT 
            d.LPN, 
            d.Zone, 
            d.Wave_Number AS Full_Wave, 
            TRIM(d.Branch_Code) AS Branch_Code, 
            COALESCE(
                MAX(IF(
                    LOWER(TRIM(IFNULL(m.Branch_Name, ''))) IN ('', 'unknown', 'null', 'none'),
                    NULL,
                    TRIM(m.Branch_Name)
                )),
                MAX(h.Branch_Name),
                'Unknown'
            ) AS Branch_Name,
            -- PP/SP LPNs can contain multiple product rows; show the total carton target for the whole LPN.
            IF(
                REGEXP_CONTAINS(UPPER(TRIM(d.LPN)), r'^(PP|SP)'),
                SUM(IFNULL(d.Total_Qty, 1)),
                MAX(IFNULL(d.Total_Qty, 1))
            ) AS Total_Qty, 
            IF(COALESCE(MAX(s.Scanned_Qty), 0) > 0, 'Scanned', 'Pending') AS status,
            MAX(s.Scanned_Qty) AS qty,
            MAX(s.Scan_Type) AS scan_type,
            COALESCE(MAX(s.Scanned_Pallet_No), 0) AS pallet_no,
            COALESCE(MAX(TRIM(d.Owner)), 'Unknown') AS owner,
            MAX(s.Color) AS color,
            MAX(s.Scanner_Emp_IDs) AS scanner_emp_ids,
            MAX(s.Color_Breakdown) AS color_breakdown,
            MAX(s.Pallet_Breakdown) AS pallet_breakdown,
            ANY_VALUE(ps.Pallet_Nos) AS branch_pallet_nos,
            MAX(pc.Pallet_Color) AS pallet_color,
            ANY_VALUE(sps.Submitted_Pallet_Nos) AS branch_submitted_pallet_nos,
            ANY_VALUE(bcs.Branch_Closed_At) AS branch_closed_at,
            ANY_VALUE(bcs.Branch_Closed_By) AS branch_closed_by,
            {qc_select}
            ARRAY_AGG(STRUCT(
                TRIM(d.Owner) AS owner,
                TRIM(d.Product_Code) AS product_code,
                d.Total_Pieces AS total_pieces,
                d.Total_Qty AS row_total_qty
            )) AS detail_rows
        FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record` AS d
        LEFT JOIN WaveMonitoringFiltered AS m 
          ON TRIM(d.Branch_Code) = m.Branch_Code
         AND m.Detail_Wave = LPAD(
             CAST(SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(d.Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS STRING),
             10,
             '0'
         )
        LEFT JOIN BranchNameHistory AS h
          ON TRIM(UPPER(d.Branch_Code)) = h.Branch_Code
        LEFT JOIN ScanHistory AS s
         ON s.Scan_Wave_ID = {search_wave_id}
         AND TRIM(UPPER(d.LPN)) = s.Clean_LPN
         AND TRIM(UPPER(d.Branch_Code)) = s.Scan_Branch
        LEFT JOIN PalletSummary AS ps
          ON TRIM(UPPER(d.Branch_Code)) = ps.Scan_Branch
        LEFT JOIN PalletColorSummary AS pc
          ON TRIM(UPPER(d.Branch_Code)) = pc.Scan_Branch
         AND COALESCE(s.Scanned_Pallet_No, 0) = pc.Pallet_No
        LEFT JOIN SubmittedPalletSummary AS sps
          ON TRIM(UPPER(d.Branch_Code)) = sps.Scan_Branch
        LEFT JOIN BranchCloseSummary AS bcs
          ON TRIM(UPPER(d.Branch_Code)) = bcs.Scan_Branch
        {qc_join}
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(d.Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
        GROUP BY d.LPN, d.Zone, d.Branch_Code, d.Wave_Number
    """

    meta_query = f"""
        SELECT 
            COALESCE(MAX(TRIM(Vehicle_Booking_No)), '') AS booking_no,
            COALESCE(MAX(TRIM(License_Plate)), '') AS license_plate
        FROM `pro-analytics-db.logistics_db.wave_monitoring`
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
    """

    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    # 🚀 รัน meta_job และ query_job แบบ parallel เพื่อลดเวลาโดยรวม
    meta_job = client.query(meta_query, job_config=job_config)
    query_job = client.query(query, job_config=job_config)

    meta_rows = list(meta_job.result(timeout=BQ_JOB_TIMEOUT_SECONDS))
    booking_no = ""
    license_plate = ""
    if len(meta_rows) > 0:
        booking_no = meta_rows[0]["booking_no"] or ""
        license_plate = meta_rows[0]["license_plate"] or ""

    sheet_meta = get_sheet_meta_for_wave(str(search_wave_id))
    if sheet_meta:
        if sheet_meta.get("booking"):
            booking_no = sheet_meta["booking"]
        if sheet_meta.get("plate"):
            license_plate = sheet_meta["plate"]

    results = query_job.result(timeout=BQ_JOB_TIMEOUT_SECONDS)

    lpn_list = []
    zones_calc = {}
    row_count = 0
    real_wave_no = f"{search_wave_id:010d}"
    numeric_branch_map = None

    for row in results:
        row_count += 1
        real_wave_no = row["Full_Wave"]
        z = row["Zone"] if row["Zone"] else "N/A"
        raw_code = row["Branch_Code"]
        br_code_str = str(raw_code).strip() if raw_code else "Unknown"
        if is_numeric_branch_code(br_code_str):
            if numeric_branch_map is None:
                numeric_branch_map = load_numeric_branch_master()
            br_name = clean_branch_display_name(
                numeric_branch_map.get(br_code_str) or row["Branch_Name"]
            )
        else:
            br_name = clean_branch_display_name(row["Branch_Name"])
        total_qty = calculate_direct_total_qty(row["LPN"], row["detail_rows"], row["Total_Qty"])
        pallet_breakdown = []
        for raw_part in str(row["pallet_breakdown"] or "").split("|"):
            pieces = raw_part.split("~", 3)
            if len(pieces) != 4:
                continue
            try:
                part_pallet = int(pieces[0] or 0)
                part_qty = int(pieces[2] or 0)
            except (TypeError, ValueError):
                continue
            if part_qty > 0:
                pallet_breakdown.append({"pallet_no": part_pallet, "color": pieces[1], "qty": part_qty, "type": pieces[3]})

        lpn_list.append({
            "lpn": row["LPN"],
            "zone": z,
            "branch": br_code_str,
            "branch_name": br_name,
            "status": row["status"],
            "total_qty": total_qty,
            "qty": row["qty"] if row["qty"] is not None else 0,
            "scan_type": row["scan_type"],
            "owner": row["owner"] or "Unknown",
            "color": row["color"] or "None",
            "scanner_emp_ids": row["scanner_emp_ids"] or "",
            "color_breakdown": row["color_breakdown"] or "",
            "pallet_breakdown": pallet_breakdown,
            "pallet_no": row["pallet_no"] if row["pallet_no"] is not None else 0,
            "branch_pallet_nos": list(row["branch_pallet_nos"] or []),
            "pallet_color": row["pallet_color"] or "",
            "branch_submitted_pallet_nos": list(row["branch_submitted_pallet_nos"] or []),
            "branch_closed_at": (
                row["branch_closed_at"].isoformat()
                if row["branch_closed_at"] and hasattr(row["branch_closed_at"], "isoformat")
                else str(row["branch_closed_at"] or "")
            ),
            "branch_closed_by": row["branch_closed_by"] or "",
            # 🔒 QC_FEATURE_ENABLED=False → hold QC, เปลี่ยนเป็น True เมื่อเปิดใช้งาน
            "qc_required": bool(row["qc_required"]) and QC_FEATURE_ENABLED,
            "qc_status": ("ต้อง QC" if row["qc_required"] else "") if QC_FEATURE_ENABLED else "",
            "qc_risk": float(row["qc_risk"] or 0),
            "qc_source": row["qc_source"] or "",
            "wave_no": str(row["Full_Wave"]).strip()
        })

        if z not in zones_calc:
            zones_calc[z] = {"zone": z, "scanned": 0, "total": 0}
        zones_calc[z]["total"] += 1
        if row["status"] == "Scanned":
            zones_calc[z]["scanned"] += 1

    if row_count == 0:
        raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูล Wave [{search_wave_id}]")

    return {
        "wave_no": real_wave_no,
        "booking_no": booking_no,
        "license_plate": license_plate,
        "lpn_list": lpn_list,
        "zone_summary": list(zones_calc.values())
    }

def get_wave_data_internal(wave_no: str, force_refresh: bool = False) -> dict:
    if UAT_SHEETS_ONLY:
        if force_refresh:
            with member_history_lock:
                member_history_cache["expires_at"] = 0.0
            with booking_wave_sheet_lock:
                booking_wave_sheet_cache["expires_at"] = 0.0
        return build_uat_wave_data(wave_no)
    try:
        search_wave_id = int(wave_no.strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ต้องเป็นตัวเลขเท่านั้น")

    wave_detail_str = f"{search_wave_id:010d}"
    wave_clean = str(search_wave_id)
    now = time.time()

    data = None
    with wave_cache_lock:
        cached = wave_cache.get(wave_detail_str)
        if cached:
            cache_fresh = float(cached.get("expires_at") or 0) > now
            fetched_recently = now - float(cached.get("fetched_at") or 0) < WAVE_FORCE_REFRESH_COOLDOWN_SECONDS
            if (not force_refresh and cache_fresh) or (force_refresh and cache_fresh and fetched_recently):
                data = copy.deepcopy(cached["data"])

    if data is None:
        # ป้องกันหลาย Handheld ยิง Query Wave เดียวกันพร้อมกันตอน cache หมดอายุ
        with wave_query_locks_guard:
            query_lock = wave_query_locks.setdefault(wave_detail_str, Lock())
        with query_lock:
            with wave_cache_lock:
                cached = wave_cache.get(wave_detail_str)
                current_time = time.time()
                if cached:
                    cache_fresh = float(cached.get("expires_at") or 0) > current_time
                    fetched_recently = current_time - float(cached.get("fetched_at") or 0) < WAVE_FORCE_REFRESH_COOLDOWN_SECONDS
                    if (not force_refresh and cache_fresh) or (force_refresh and cache_fresh and fetched_recently):
                        data = copy.deepcopy(cached["data"])

            if data is None:
                data = fetch_wave_data_from_bq(search_wave_id)
                fetched_at = time.time()
                with wave_cache_lock:
                    wave_cache[wave_detail_str] = {
                        "data": data,
                        "expires_at": fetched_at + WAVE_CACHE_TTL,
                        "fetched_at": fetched_at,
                    }
                data = copy.deepcopy(data)

    data["document_overrides"] = get_document_overrides_for_wave(wave_clean)
    return data

active_wave_refreshes = set()
active_wave_refreshes_lock = Lock()

def background_refresh_wave(wave_no: str):
    try:
        search_wave_id = int(wave_no.strip())
        wave_detail_str = f"{search_wave_id:010d}"
        wave_clean = str(search_wave_id)
    except ValueError:
        return

    # Skip if this wave is already actively refreshing in a background task
    with active_wave_refreshes_lock:
        if wave_detail_str in active_wave_refreshes:
            return
        active_wave_refreshes.add(wave_detail_str)

    try:
        # Wait a short duration to let BigQuery ingest the stream
        time.sleep(2.5)
        get_wave_data_internal(wave_clean, force_refresh=True)
    except Exception as e:
        print(f"Background refresh error for wave {wave_clean}: {e}")
    finally:
        with active_wave_refreshes_lock:
            active_wave_refreshes.discard(wave_detail_str)

def get_valid_lpns_for_wave(wave_no: str) -> set:
    import time
    now = time.time()
    try:
        wave_clean = str(int(wave_no.strip()))
        wave_detail_str = f"{int(wave_no.strip()):010d}"
    except ValueError:
        return set()

    # Try to read from wave_cache first for near-instant validation
    with wave_cache_lock:
        cached = wave_cache.get(wave_detail_str)
        if cached and cached["expires_at"] > now:
            lpns = set()
            for item in cached["data"].get("lpn_list", []):
                lpns.add((item["lpn"].strip().upper(), item["branch"].strip().upper()))
            return lpns
    
    with valid_lpns_cache_lock:
        cache_entry = valid_lpns_cache.get(wave_clean)
        if cache_entry and cache_entry["expires_at"] > now:
            return cache_entry["lpns"]
            
    # Query BigQuery
    query = f"""
        SELECT DISTINCT TRIM(UPPER(CAST(LPN AS STRING))) AS LPN, TRIM(UPPER(CAST(Branch_Code AS STRING))) AS Branch_Code
        FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {wave_clean}
    """
    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(use_query_cache=True))
        rows = query_job.result(timeout=BQ_JOB_TIMEOUT_SECONDS)
        lpns = set()
        for row in rows:
            lpn_val = str(row["LPN"]).strip().upper() if row["LPN"] else ""
            branch_val = str(row["Branch_Code"]).strip().upper() if row["Branch_Code"] else ""
            if lpn_val:
                lpns.add((lpn_val, branch_val))
        
        if lpns:
            with valid_lpns_cache_lock:
                valid_lpns_cache[wave_clean] = {
                    "lpns": lpns,
                    "expires_at": now + VALID_LPNS_CACHE_TTL
                }
        return lpns
    except Exception as e:
        print(f"🚨 CACHE QUERY ERROR for Wave {wave_clean}: {str(e)}")
        return set()


def fetch_booking_waves_from_bq(booking_no: str) -> dict:
    if UAT_SHEETS_ONLY:
        sheet_meta = get_sheet_meta_for_booking(booking_no, force=True)
        if not sheet_meta or not sheet_meta.get("waves"):
            raise HTTPException(status_code=404, detail=f"ไม่พบ Booking [{booking_no}] ใน Sheet Booking & Wave")
        return {
            "waves": list(sheet_meta.get("waves") or []),
            "license_plate": str(sheet_meta.get("plate") or ""),
            "carrier": str(sheet_meta.get("carrier") or ""),
            "sender": str(sheet_meta.get("sender") or ""),
        }
    clean_booking = booking_no.strip().upper()
    raw_booking = booking_no.strip()
    # ✅ ใช้ query parameters แทนการต่อสตริง (อุด SQL injection ผ่านเลข Booking)
    query = """
        SELECT DISTINCT
            TRIM(Wave_Number) AS monitor_wave,
            REGEXP_REPLACE(TRIM(Wave_Number), r'^WAVE-', '') AS detail_wave,
            CAST(SAFE_CAST(REGEXP_REPLACE(TRIM(Wave_Number), r'[^0-9]', '') AS INT64) AS STRING) AS scan_wave,
            TRIM(License_Plate) AS License_Plate
        FROM `pro-analytics-db.logistics_db.wave_monitoring`
        WHERE (Vehicle_Booking_No = @clean_booking OR Vehicle_Booking_No = @raw_booking)
          AND Wave_Number IS NOT NULL
          AND Wave_Number != ''
    """
    booking_config = bigquery.QueryJobConfig(
        use_query_cache=True,
        query_parameters=[
            bigquery.ScalarQueryParameter("clean_booking", "STRING", clean_booking),
            bigquery.ScalarQueryParameter("raw_booking", "STRING", raw_booking),
        ],
    )
    query_job = client.query(query, job_config=booking_config)
    results = list(query_job.result(timeout=BQ_JOB_TIMEOUT_SECONDS))
    if not results:
        sheet_meta = get_sheet_meta_for_booking(booking_no)
        if sheet_meta and sheet_meta.get("waves"):
            return {
                "waves": sheet_meta["waves"],
                "license_plate": sheet_meta.get("plate", "")
            }
        raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูลสำหรับ Booking No. [{booking_no}]")
    
    waves = []
    license_plate = ""
    for row in results:
        detail_wave = str(row["detail_wave"]).strip()
        if detail_wave:
            waves.append(detail_wave)
        if row["License_Plate"] and not license_plate:
            license_plate = str(row["License_Plate"]).strip()
            
    return {
        "waves": waves,
        "license_plate": license_plate
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
        mapping = fetch_booking_waves_from_bq(booking_no)
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
    if UAT_SHEETS_ONLY and force_refresh:
        # Refresh the large Member Data sheet once before fan-out. Refreshing
        # independently inside every Wave thread is slower and can expose
        # different snapshots while the Sheet is still updating.
        with member_history_lock:
            member_history_cache["expires_at"] = 0.0
        with booking_wave_sheet_lock:
            booking_wave_sheet_cache["expires_at"] = 0.0
        load_member_history()
        load_booking_wave_sheet_meta(force=True)
        wave_force_refresh = False
    assignments = get_booking_branch_assignments()
    splits = get_booking_branch_splits()
    override_waves = [wave for (wave, branch), move in assignments.items()
                      if str(move.get("Assigned_Booking") or "").strip().upper() == booking_clean]
    split_waves = [wave for (wave, branch, target), split in splits.items()
                   if target == booking_clean or str(split.get("Source_Booking") or "").strip().upper() == booking_clean]
    waves = list(dict.fromkeys(
        str(int(re.sub(r"\D", "", str(wave)))) for wave in (list(mapping["waves"]) + override_waves + split_waves)
        if re.sub(r"\D", "", str(wave or ""))
    ))
    license_plate = mapping["license_plate"]
    
    lpn_list = []
    waves_included = set()
    wave_results = []
    
    # จำกัด fan-out ไม่ให้ Booking ที่มีหลาย Wave ยิง BigQuery พร้อมกันจนคิว API อั้นทั้งระบบ
    with ThreadPoolExecutor(max_workers=max(1, min(6, len(waves)))) as executor:
        futures = {executor.submit(get_wave_data_internal, wave, wave_force_refresh): wave for wave in waves}
        for future in futures:
            wave = futures[future]
            try:
                wave_data = future.result()
                wave_data_overlaid = merge_member_history(apply_local_overlay(wave, wave_data), wave)
                wave_results.append(wave_data_overlaid)
            except Exception as e:
                print(f"🚨 Error fetching wave {wave} in booking {booking_no}: {e}")
                raise
                
    for wave_data_overlaid in wave_results:
        wave_key = str(int(str(wave_data_overlaid["wave_no"]).strip()))
        native_booking = str(wave_data_overlaid.get("booking_no") or "").strip().upper()
        items_by_branch = {}
        for item in wave_data_overlaid.get("lpn_list", []):
            items_by_branch.setdefault(str(item.get("branch") or "").strip().upper(), []).append(item)
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

                if booking_clean == native_booking:
                    visible_totals = {field: max(0, int(base_summary.get(field) or 0) - outgoing[field]) for field in fields}
                    for item in branch_items:
                        item = copy.deepcopy(item)
                        item["booking_split_summary"] = visible_totals
                        item["booking_split_outgoing"] = True
                        lpn_list.append(item)
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
            else:
                for item in branch_items:
                    lpn_list.append(item)
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
            combined_overrides.update(get_document_overrides_for_wave(clean_w))

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
            return copy.deepcopy(booking_assignments_cache["data"])
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
            return copy.deepcopy(data)
        except Exception as exc:
            print(f"BOOKING OVERRIDE READ ERROR: {exc}")
            return copy.deepcopy(booking_assignments_cache["data"])

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
            return copy.deepcopy(booking_splits_cache["data"])
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
            return copy.deepcopy(data)
        except Exception as exc:
            print(f"BOOKING SPLIT READ ERROR: {exc}")
            return copy.deepcopy(booking_splits_cache["data"])

# ==================== ROUTES & APIs ====================

@app.get("/")
async def read_root():
    return {
        "status": "ok",
        "message": "Scanner API UAT is running",
        "environment": APP_ENV,
        "data_source": "google_sheets" if UAT_SHEETS_ONLY else "bigquery",
        "legacy_transport_workbook": "read_only",
        "scan_feature_enabled": SCAN_FEATURE_ENABLED,
    }

# ✅ Health Check Endpoint: ตอบสนองเร็ว <5ms สำหรับ keep-alive heartbeat
@app.get("/api/health")
async def health_check(response: Response):
    # ⚡ Cache-Control: s-maxage=5 ทำให้ CDN/Render ตอบ health check ได้ทันที ไม่ต้อง round-trip ถึง Python
    response.headers["Cache-Control"] = "no-store"
    response.headers["Connection"] = "keep-alive"
    # ตรวจสอบสถานะ Google credentials
    creds_source = "none"
    creds_ok = False
    creds_error = None
    try:
        if os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip():
            creds_source = "GOOGLE_SERVICE_ACCOUNT_JSON"
        elif os.path.exists(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")):
            creds_source = f"file:{os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}"
        else:
            creds_source = "default_adc"
        get_sheets_session()
        creds_ok = True
    except Exception as e:
        creds_error = str(e)[:200]
    return {
        "status": "ok", "version": APP_VERSION, "timestamp": time.time(),
        "environment": APP_ENV,
        "data_source": "google_sheets" if UAT_SHEETS_ONLY else "bigquery",
        "legacy_transport_workbook": "read_only",
        "scan_feature_enabled": SCAN_FEATURE_ENABLED,
        "google_credentials": {
            "source": creds_source,
            "ok": creds_ok,
            "error": creds_error,
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

def sync_document_summary_reports(summaries: list):
    if UAT_SHEETS_ONLY:
        # Manual document totals must update both UAT sources before the API
        # reports success. The legacy transport workbook remains read-only.
        test_summaries = [normalize_report_summary(summary) for summary in summaries or []]
        test_summaries = [summary for summary in test_summaries
                          if not summary.get("is_hidden") and
                          (summary.get("total", 0) > 0 or summary.get("allow_zero_update"))]
        member_summaries = [summary for summary in test_summaries if not summary.get("booking_split")]
        failures = []
        try:
            write_member_history_summaries(member_summaries)
        except Exception as exc:
            failures.append(f"Member Data: {exc}")
        try:
            write_uat_report_test_summaries(test_summaries)
        except Exception as exc:
            failures.append(f"UAT Delivery report: {exc}")
        if failures:
            raise RuntimeError(" | ".join(failures))
        return
    # Automatic zero snapshots must not create report rows.
    # Intentional zero corrections may pass through to clear an existing row only.
    summaries = [normalize_report_summary(summary) for summary in summaries or []]
    summaries = [summary for summary in summaries
                 if summary.get("total", 0) > 0 or summary.get("allow_zero_update")]
    if not summaries:
        return
    failures = []
    # Member Data ไม่มีคอลัมน์ Booking จึงเก็บยอดรวมเดิม 1 แถวต่อ Wave+Branch
    # ส่วน Delivery report รองรับแยก Booking และรับยอดแบ่งได้
    member_summaries = [summary for summary in summaries if not summary.get("booking_split")]
    try:
        write_member_history_summaries(member_summaries)
    except Exception as exc:
        print(f"🚨 Member Data batch write error: {exc}")
        failed = []
        for s in member_summaries:
            try:
                write_member_history_summary(s)
            except Exception as single_exc:
                failed.append(f"{s.get('wave')}/{s.get('branch')}: {single_exc}")
        if failed:
            failures.append("Member Data: " + "; ".join(failed))
    if LEGACY_DELIVERY_REPORT_SYNC_ENABLED:
        try:
            write_delivery_report_summaries(summaries)
        except Exception as exc:
            print(f"🚨 Delivery report batch write error: {exc}")
            failed = []
            for s in summaries:
                try:
                    write_delivery_report_summary(s)
                except Exception as single_exc:
                    failed.append(f"{s.get('wave')}/{s.get('branch')}: {single_exc}")
            if failed:
                failures.append("Delivery report: " + "; ".join(failed))
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

    if data.persist_overrides:
        # The frontend sends dirty branches only. Persist zero so old totals cannot return later.
        persistent_items = normalized
        for item in persistent_items:
            item["allow_zero_update"] = True
        # Apply the edit to the live web overlay first. A temporary Google auth
        # outage must never make the UI snap back to the calculated old total.
        with document_overrides_lock:
            for item in persistent_items:
                wave_ov = document_overrides_overlay.setdefault(item["wave"], {})
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
    # UAT mirror ใช้ยอดล่าสุดที่หน้าเอกสารแสดง เพื่อทดสอบยอดก่อนตัด staging tab;
    # production ยังคงส่งเฉพาะสาขาที่ปิดจบแล้วตามกติกาเดิม.
    report_summaries = (normalized if UAT_SHEETS_ONLY
                        else [item for item in normalized if item.get("is_closed")])
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
                document_overrides_overlay[w] = {}
        queue_wave_totals_reconciliation(waves_to_clear, delay_seconds=0.5)

    return {"status": "success", "cleared_waves": waves_to_clear}

@app.get("/api/document-overrides")
def get_document_overrides_endpoint(wave_no: str):
    clean_w = re.sub(r"\D", "", str(wave_no or ""))
    if not clean_w:
        return {"overrides": {}}
    return {"wave_no": clean_w, "overrides": get_document_overrides_for_wave(clean_w)}

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
        return merge_member_history(overlaid_data, wave_no)
    except HTTPException:
        raise
    except Exception as e:
        print(f"🚨 CHECK WAVE ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 [API 1.5] โหลดข้อมูล Booking
def ensure_booking_source_complete(booking_no: str, booking_data: dict):
    """Reject progressive Member Data snapshots until every planned branch exists."""
    if not UAT_SHEETS_ONLY:
        return
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
        actual.update(get_document_overrides_for_wave(wave).keys())
        absent = sorted(expected - actual)
        if absent:
            missing.append({"wave": wave, "missing": absent, "expected": len(expected), "actual": len(actual)})
    if missing:
        details = "; ".join(
            f"Wave {item['wave']} ขาด {len(item['missing'])} สาขา ({','.join(item['missing'][:5])})"
            for item in missing
        )
        raise HTTPException(status_code=409, detail=f"ข้อมูล Booking [{booking}] ยังเข้าไม่ครบ: {details}")


@app.get("/api/check-booking")
def check_booking(booking_no: str, force: bool = False):
    try:
        try:
            booking_data = get_booking_data_internal(booking_no, force_refresh=force)
            ensure_booking_source_complete(booking_no, booking_data)
            return booking_data
        except HTTPException as first_error:
            if not UAT_SHEETS_ONLY or first_error.status_code not in (404, 409):
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
    return {"status": "success", "message": "ย้ายสาขาเรียบร้อย", "previous_booking": previous, "target_booking": target, "preview": preview}

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
    for booking in (source, target):
        booking_view = get_booking_data_internal(booking, force_refresh=True)
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
        if UAT_SHEETS_ONLY or is_closed:
            summary.update({"booking": booking, "booking_split": True, "is_closed": is_closed})
            split_report_summaries.append(summary)
    queue_report_summary_snapshots(split_report_summaries, delay_seconds=0.0)
    return {"status": "success", "message": "แบ่งยอดเข้าสอง Booking เรียบร้อย",
            "source_booking": source, "target_booking": target, "allocated": requested}

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

    cache_key = (tuple(wave_ids), branch)
    with pallet_allocation_lock:
        cached_max = int(pallet_counter_cache.get(cache_key, 0) or 0)
        if cached_max > 0:
            next_no = cached_max + 1
        else:
            max_query = """
                SELECT COALESCE(MAX(SAFE_CAST(Pallet_No AS INT64)), 0) AS max_no
                FROM `pro-analytics-db.logistics_db.app_scan_transactions`
                WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) IN UNNEST(@wave_ids)
                  AND TRIM(UPPER(IFNULL(Branch_Code, ''))) = @branch
                  AND SAFE_CAST(Pallet_No AS INT64) > 0
            """
            max_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("wave_ids", "INT64", wave_ids),
                bigquery.ScalarQueryParameter("branch", "STRING", branch),
            ])
            row = next(iter(client.query(max_query, job_config=max_config).result(timeout=BQ_JOB_TIMEOUT_SECONDS)))
            next_no = int(row["max_no"] or 0) + 1
        marker_lpn = f"PALLET_{branch}_{next_no}"
        marker_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        markers = [{
            "Wave_Number": str(wave_id),
            "LPN": marker_lpn,
            "Scan_Type": "PALLET_START",
            "Color": color,
            "Qty": 0,
            "Timestamp": marker_time,
            "Branch_Code": branch,
            "Branch_Name": branch_name,
            "Emp_ID": emp_id,
            "Pallet_No": next_no,
        } for wave_id in wave_ids]
        errors = client.insert_rows_json(
            client.dataset("logistics_db").table("app_scan_transactions"),
            markers,
            row_ids=[f"pallet:{wave_id}:{branch}:{next_no}" for wave_id in wave_ids],
        )
        if errors:
            raise HTTPException(status_code=500, detail=f"จองเลขพาเลทไม่สำเร็จ: {errors}")
        pallet_counter_cache[cache_key] = next_no

    record_shared_pallet_state(wave_ids, branch, next_no, color=color, submitted=False)

    return {
        "status": "success",
        "pallet_no": next_no,
        "color": color,
        "allocated_by": emp_id,
    }

@app.post("/api/submit-pallet")
def submit_pallet(data: PalletSubmitData):
    """Share a completed pallet with every handheld while keeping an auditable marker in BigQuery."""
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

    marker_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    markers = [{
        "Wave_Number": str(wave_id),
        "LPN": f"PALLET_SUBMIT_{branch}_{pallet_no}",
        "Scan_Type": "PALLET_SUBMIT",
        "Color": color or "None",
        "Qty": 0,
        "Timestamp": marker_time,
        "Branch_Code": branch,
        "Branch_Name": branch_name,
        "Emp_ID": emp_id,
        "Pallet_No": pallet_no,
    } for wave_id in wave_ids]
    errors = client.insert_rows_json(
        client.dataset("logistics_db").table("app_scan_transactions"),
        markers,
        row_ids=[f"pallet-submit:{wave_id}:{branch}:{pallet_no}" for wave_id in wave_ids],
    )
    if errors:
        raise HTTPException(status_code=500, detail=f"ส่งสถานะพาเลทไม่สำเร็จ: {errors}")
    record_shared_pallet_state(wave_ids, branch, pallet_no, color=color, submitted=True)
    # รายงาน Google Sheet ใช้เฉพาะยอดตอนปิดสาขา จึงไม่ต้อง query BigQuery ซ้ำตอนส่งทุกพาเลท
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
    valid_pairs = get_valid_lpns_for_wave(wave_clean)
    if valid_pairs and (lpn, branch) not in valid_pairs:
        raise HTTPException(status_code=400, detail=f"ไม่พบ LPN [{lpn}] ใน Wave/Branch นี้")

    audit_color = f"AUDIT:{correction_id}"
    duplicate_query = """
        SELECT COUNT(*) AS found
        FROM `pro-analytics-db.logistics_db.app_scan_transactions`
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave
          AND TRIM(UPPER(LPN)) = @lpn
          AND Color = @audit_color
    """
    duplicate_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("wave", "INT64", int(wave_clean)),
        bigquery.ScalarQueryParameter("lpn", "STRING", lpn),
        bigquery.ScalarQueryParameter("audit_color", "STRING", audit_color),
    ])
    duplicate_row = next(iter(client.query(duplicate_query, job_config=duplicate_config).result(timeout=BQ_JOB_TIMEOUT_SECONDS)))
    if int(duplicate_row["found"] or 0) > 0:
        queue_branch_totals_reconciliation([(wave_clean, branch)])
        return {"status": "success", "correction_id": correction_id, "duplicate": True, "report_sync": "queued"}

    fresh = apply_local_overlay(wave_clean, get_wave_data_internal(wave_clean, force_refresh=True))
    current = next((item for item in fresh.get("lpn_list", [])
                    if str(item.get("lpn", "")).strip().upper() == lpn
                    and str(item.get("branch", "")).strip().upper() == branch), None)
    if not current:
        raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูลปัจจุบันของ LPN [{lpn}]")

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
    now = datetime.datetime.now(datetime.timezone.utc)
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
        "corrected_at": now.isoformat(),
    }
    audit_type = "CORRECTION|" + encode_correction_audit(audit_payload)
    insert_query = """
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (Wave_Number, LPN, Scan_Type, Color, Qty, Timestamp, Branch_Code, Branch_Name, Emp_ID, Pallet_No)
        SELECT @wave_str, @lpn, @audit_type, @audit_color, 0, @audit_time, @branch, @branch_name, @emp_id, @old_pallet
        UNION ALL
        SELECT @wave_str, @lpn, @new_type, @new_color, @new_qty, @new_time, @branch, @branch_name, @emp_id, @new_pallet
        FROM UNNEST([1]) AS guard_row
        WHERE @new_qty > 0
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("wave_str", "STRING", wave_clean),
        bigquery.ScalarQueryParameter("lpn", "STRING", lpn),
        bigquery.ScalarQueryParameter("audit_type", "STRING", audit_type),
        bigquery.ScalarQueryParameter("audit_color", "STRING", audit_color),
        bigquery.ScalarQueryParameter("audit_time", "TIMESTAMP", now),
        bigquery.ScalarQueryParameter("branch", "STRING", branch),
        bigquery.ScalarQueryParameter("branch_name", "STRING", (data.branch_name or "").strip()),
        bigquery.ScalarQueryParameter("emp_id", "STRING", emp_id),
        bigquery.ScalarQueryParameter("old_pallet", "INT64", int(old_snapshot["pallet_no"] or 0)),
        bigquery.ScalarQueryParameter("new_type", "STRING", scan_type),
        bigquery.ScalarQueryParameter("new_color", "STRING", color),
        bigquery.ScalarQueryParameter("new_qty", "INT64", new_qty),
        bigquery.ScalarQueryParameter("new_time", "TIMESTAMP", now + datetime.timedelta(milliseconds=1)),
        bigquery.ScalarQueryParameter("new_pallet", "INT64", pallet_no),
    ])
    client.query(insert_query, job_config=config).result(timeout=BQ_JOB_TIMEOUT_SECONDS)

    record_local_scan(wave_clean, lpn, branch, 0, audit_type, audit_color, 0)
    if new_qty > 0:
        record_local_scan(wave_clean, lpn, branch, new_qty, scan_type, color, pallet_no)
    queue_branch_totals_reconciliation([(wave_clean, branch)])
    # ไม่ refresh BigQuery ซ้ำทุกครั้ง: local overlay อัปเดตทุกเครื่องได้ทันทีอยู่แล้ว
    return {"status": "success", "correction_id": correction_id, "audit": audit_payload, "report_sync": "queued"}

@app.get("/api/corrections")
def get_corrections(waves: str, branch: str, lpn: Optional[str] = None):
    wave_ids = sorted({int(part.strip()) for part in str(waves or "").split(",") if part.strip().isdigit()})
    branch_clean = (branch or "").strip().upper()
    lpn_clean = (lpn or "").strip().upper()
    if not wave_ids or not branch_clean:
        raise HTTPException(status_code=400, detail="ข้อมูล Wave หรือสาขาไม่ครบ")
    query = """
        SELECT Scan_Type, Timestamp
        FROM `pro-analytics-db.logistics_db.app_scan_transactions`
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) IN UNNEST(@wave_ids)
          AND TRIM(UPPER(IFNULL(Branch_Code, ''))) = @branch
          AND STARTS_WITH(Scan_Type, 'CORRECTION|')
          AND (@lpn = '' OR TRIM(UPPER(LPN)) = @lpn)
        ORDER BY Timestamp DESC
        LIMIT 200
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("wave_ids", "INT64", wave_ids),
        bigquery.ScalarQueryParameter("branch", "STRING", branch_clean),
        bigquery.ScalarQueryParameter("lpn", "STRING", lpn_clean),
    ])
    history = []
    for row in client.query(query, job_config=config).result(timeout=BQ_JOB_TIMEOUT_SECONDS):
        decoded = decode_correction_audit(row["Scan_Type"])
        if decoded:
            history.append(decoded)
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
    base_pallet_breakdown = []
    transaction_id = (data.transaction_id or "").strip()
    if transaction_already_processed(transaction_id):
        return {"status": "success", "message": "Already saved", "duplicate": True}
    try:
        pallet_no_val = int(data.pallet_no or 0)
    except (ValueError, TypeError):
        pallet_no_val = 0

    # Check cache first
    valid_pairs = get_valid_lpns_for_wave(wave_clean)

    if valid_pairs:
        if (lpn_val.upper(), branch_val.upper()) not in valid_pairs:
            print(f"🚫 REJECTED | LPN: {lpn_val} ไม่พบใน Wave {wave_clean} / Branch {branch_val}")
            raise HTTPException(
                status_code=400,
                detail=f"ไม่พบ LPN [{data.lpn}] ใน Wave {wave_clean} สาขา {data.branch_code}"
            )
    else:
        # Fallback to direct query only if cache is empty
        # ✅ ใช้ query parameters แทนการต่อสตริง
        check_query = """
            SELECT COUNT(*) AS found
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave_id
              AND TRIM(UPPER(CAST(LPN AS STRING))) = @lpn
              AND TRIM(UPPER(CAST(Branch_Code AS STRING))) = @branch
        """
        check_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("wave_id", "INT64", int(wave_clean)),
            bigquery.ScalarQueryParameter("lpn", "STRING", lpn_val.upper()),
            bigquery.ScalarQueryParameter("branch", "STRING", branch_val.upper()),
        ])
        try:
            check_result = client.query(check_query, job_config=check_config).result()
            found = next(iter(check_result))["found"]
            if found == 0:
                print(f"🚫 REJECTED (DB Fallback) | LPN: {lpn_val} ไม่พบใน Wave {wave_clean} / Branch {branch_val}")
                raise HTTPException(
                    status_code=400,
                    detail=f"ไม่พบ LPN [{data.lpn}] ใน Wave {wave_clean} สาขา {data.branch_code}"
                )
        except HTTPException:
            raise
        except Exception as e:
            print(f"🚨 CHECK FALLBACK ERROR | LPN: {lpn_val} | Error: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # PP/SP ใช้ยอดสะสม: ตรวจว่าระหว่างนั้นไม่มีเครื่องอื่นแก้ยอดเดียวกัน
    # ถ้าค่าเริ่มต้นไม่ตรง ให้ผู้ใช้โหลดค่าล่าสุดแทนการเขียนทับข้อมูลของอีกเครื่อง
    if data.expected_previous_qty is not None and type_val not in ("RESET_BOX", "CANCEL_COMBINE"):
        current_data = apply_local_overlay(wave_clean, get_wave_data_internal(wave_clean))
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

    print(f"📦 SCAN | Wave: {wave_clean} | LPN: {lpn_val} | Branch: {branch_val} | Emp: {emp_val}")

    # Write to BigQuery using insert_rows_json (Streaming API is near-instant, <100ms)
    import datetime
    table_id = "pro-analytics-db.logistics_db.app_scan_transactions"
    row_to_insert = {
        "Wave_Number": wave_clean,
        "LPN": lpn_val,
        "Scan_Type": type_val,
        "Color": color_val,
        "Qty": data.qty,
        "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "Branch_Code": branch_val,
        "Branch_Name": branch_name_val,
        "Emp_ID": emp_val,
        "Pallet_No": pallet_no_val
    }

    try:
        insert_kwargs = {"row_ids": [transaction_id]} if transaction_id else {}
        errors = client.insert_rows_json(client.dataset("logistics_db").table("app_scan_transactions"), [row_to_insert], **insert_kwargs)
        if errors:
            print(f"🚨 INSERT ROWS JSON ERROR | LPN: {lpn_val} | Errors: {errors}")
            raise Exception(f"BigQuery streaming errors: {errors}")
        mark_transaction_processed(transaction_id)
        record_local_scan(wave_clean, lpn_val, branch_val, data.qty, type_val, color_val, pallet_no_val, base_pallet_breakdown)
        print(f"✅ SAVED | LPN: {lpn_val}")
        return {"status": "success", "message": "Saved", "report_sync": "not_needed"}
    except Exception as e:
        # ห้าม fallback ด้วย SQL INSERT เพราะ response หลุดหลัง BigQuery รับแล้วจะทำให้ยอดซ้ำ
        # Frontend จะ retry ด้วย transaction_id/insertId เดิม จึง dedupe ได้อย่างปลอดภัย
        print(f"🚨 INSERT RETRY REQUIRED | LPN: {lpn_val} | Error: {str(e)}")
        raise HTTPException(status_code=503, detail="Server ยังไม่ยืนยันการบันทึก ระบบจะส่งรายการเดิมซ้ำให้อัตโนมัติ")


# 🚀 [API 2.5] บันทึกข้อมูลสแกนเป็นชุด (Batch)
@app.post("/api/scan-batch")
def process_scan_batch(data: ScanBatchData, background_tasks: BackgroundTasks):
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    if not data.scans:
        return {"status": "success", "message": "No scans to process", "processed_count": 0}

    import datetime
    table_ref = client.dataset("logistics_db").table("app_scan_transactions")
    rows_to_insert = []
    accepted_scans = []
    row_ids = []
    failed_scans = []
    processed_transaction_ids = []
    for item in data.scans:
        item_transaction_id = (item.transaction_id or "").strip()
        if transaction_already_processed(item_transaction_id):
            if item_transaction_id:
                processed_transaction_ids.append(item_transaction_id)
            continue
        try:
            wave_clean = str(int(item.wave_no))
        except ValueError:
            failed_scans.append({"lpn": item.lpn, "transaction_id": item_transaction_id, "reason": "รหัส Wave ไม่ถูกต้อง"})
            continue

        lpn_val = (item.lpn or "").strip()
        branch_val = (item.branch_code or "").strip()
        branch_name_val = (item.branch_name or "").strip()
        emp_val = (item.emp_id or "").strip()
        type_val = (item.type or "").strip()
        color_val = (item.color or "").strip()
        try:
            pallet_no_val = int(item.pallet_no or 0)
        except (ValueError, TypeError):
            pallet_no_val = 0

        # Check cache first
        valid_pairs = get_valid_lpns_for_wave(wave_clean)
        
        # If cache has items, perform the check
        if valid_pairs:
            if (lpn_val.upper(), branch_val.upper()) not in valid_pairs:
                print(f"🚫 BATCH REJECTED | LPN: {lpn_val} ไม่พบใน Wave {wave_clean} / Branch {branch_val}")
                failed_scans.append({"lpn": item.lpn, "transaction_id": item_transaction_id, "reason": "ไม่พบ LPN ใน Wave/Branch"})
                continue
        else:
            # Fallback to direct query if cache is empty
            # ✅ ใช้ query parameters แทนการต่อสตริง
            check_query = """
                SELECT COUNT(*) AS found
                FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
                WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave_id
                  AND TRIM(UPPER(CAST(LPN AS STRING))) = @lpn
                  AND TRIM(UPPER(CAST(Branch_Code AS STRING))) = @branch
            """
            check_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("wave_id", "INT64", int(wave_clean)),
                bigquery.ScalarQueryParameter("lpn", "STRING", lpn_val.upper()),
                bigquery.ScalarQueryParameter("branch", "STRING", branch_val.upper()),
            ])
            try:
                check_result = client.query(check_query, job_config=check_config).result()
                found = next(iter(check_result))["found"]
                if found == 0:
                    failed_scans.append({"lpn": item.lpn, "transaction_id": item_transaction_id, "reason": "ไม่พบ LPN ใน Wave/Branch (Fallback)"})
                    continue
            except Exception as e:
                failed_scans.append({"lpn": item.lpn, "transaction_id": item_transaction_id, "reason": f"Check error: {str(e)}"})
                continue

        rows_to_insert.append({
            "Wave_Number": wave_clean,
            "LPN": lpn_val,
            "Scan_Type": type_val,
            "Color": color_val,
            "Qty": item.qty,
            "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "Branch_Code": branch_val,
            "Branch_Name": branch_name_val,
            "Emp_ID": emp_val,
            "Pallet_No": pallet_no_val
        })
        accepted_scans.append((wave_clean, lpn_val, branch_val, item.qty, type_val, color_val, pallet_no_val, item_transaction_id))
        row_ids.append(item_transaction_id or str(uuid.uuid4()))

    # Trigger background refreshes
    # ไม่สร้าง BigQuery query เพิ่มตามจำนวน Wave หลัง batch; overlay ถูกบันทึกไว้แล้ว

    if not rows_to_insert:
        return {
            "status": "success" if processed_transaction_ids and not failed_scans else "failed",
            "message": "Already saved" if processed_transaction_ids else "ไม่มีข้อมูลสแกนที่ผ่านการตรวจสอบ",
            "processed_count": 0,
            "failed_count": len(failed_scans),
            "processed_transaction_ids": processed_transaction_ids,
            "errors": failed_scans,
            "report_sync": "not_needed",
        }

    try:
        errors = client.insert_rows_json(table_ref, rows_to_insert, row_ids=row_ids)
        if errors:
            print(f"🚨 BATCH INSERT ROWS JSON ERROR | Errors: {errors}")
            # Streaming API อาจรับสำเร็จเพียงบางแถว ห้าม fallback ทั้ง batch เพราะยอดจะซ้ำ
            failed_indexes = {int(error.get("index")) for error in errors if error.get("index") is not None}
            if not failed_indexes:
                failed_indexes = set(range(len(rows_to_insert)))
            streamed_scans = [scan for index, scan in enumerate(accepted_scans) if index not in failed_indexes]
            for scan in streamed_scans:
                record_local_scan(*scan[:7])
                mark_transaction_processed(scan[7])
                if scan[7]:
                    processed_transaction_ids.append(scan[7])
            error_by_index = {int(error.get("index")): error for error in errors if error.get("index") is not None}
            for index in sorted(failed_indexes):
                scan = accepted_scans[index]
                failed_scans.append({
                    "lpn": scan[1],
                    "transaction_id": scan[7],
                    "reason": f"BigQuery ยังไม่รับรายการ: {error_by_index.get(index, {})}",
                })
            return {
                "status": "partial_success" if streamed_scans else "failed",
                "processed_count": len(streamed_scans),
                "failed_count": len(failed_scans),
                "processed_transaction_ids": processed_transaction_ids,
                "errors": failed_scans,
                "report_sync": "not_needed",
            }
        for scan in accepted_scans:
            record_local_scan(*scan[:7])
            mark_transaction_processed(scan[7])
            if scan[7]:
                processed_transaction_ids.append(scan[7])
        print(f"✅ BATCH SAVED | Processed: {len(rows_to_insert)} | Failed: {len(failed_scans)}")
        return {
            "status": "success", 
            "message": "Saved", 
            "processed_count": len(rows_to_insert), 
            "failed_count": len(failed_scans),
            "processed_transaction_ids": processed_transaction_ids,
            "errors": failed_scans,
            "report_sync": "not_needed",
        }
    except Exception as e:
        # ไม่ยิง SQL ซ้ำทั้งชุดเมื่อไม่รู้ว่า BigQuery รับไปแล้วหรือยัง
        # ให้ Handheld retry ด้วย transaction_id เดิมเพื่อใช้ insertId dedupe
        print(f"🚨 BATCH RETRY REQUIRED | Error: {str(e)}")
        raise HTTPException(status_code=503, detail="Server ยังไม่ยืนยันรายการชุดนี้ ระบบจะส่งซ้ำด้วยรหัสเดิมอัตโนมัติ")


def run_close_job_queries_in_background(wave_clean: str, branch: str, insert_zero_query: str, frontend_summary: dict = None):
    # CLOSE_JOB marker ถูก streaming แบบ durable ก่อนตอบ API แล้ว ส่วน AUTO_NOT_FOUND ทำเบื้องหลัง
    # และ retry ได้ เพราะ Qty=0 จึง idempotent และไม่ทำให้ยอดกล่องเพิ่ม
    for attempt in range(1, 6):
        try:
            client.query(insert_zero_query).result(timeout=BQ_JOB_TIMEOUT_SECONDS)
            queue_branch_totals_reconciliation([(wave_clean, branch)], delay_seconds=0.25)
            print(f"✅ BACKGROUND CLOSE JOB COMPLETE | Wave: {wave_clean} | Branch: {branch}")
            return
        except Exception as e:
            print(f"🚨 BACKGROUND CLOSE JOB RETRY {attempt}/5 | Wave: {wave_clean} | Branch: {branch} | Error: {str(e)}")
            if attempt < 5:
                time.sleep(min(15, attempt * 2))
    # Marker ยังอยู่ใน BigQuery จึงให้ startup recovery/การเปิดงานครั้งถัดไป reconcile Sheet ได้เสมอ
    queue_branch_totals_reconciliation([(wave_clean, branch)], delay_seconds=0.25)


# 🚀 [API 5] ปิดจบงานสาขา
@app.post("/api/close-job")
def close_job(data: CloseJobData, background_tasks: BackgroundTasks):
    if not SCAN_FEATURE_ENABLED:
        scan_hold_error()
    try:
        wave_clean = str(int(data.wave.strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ไม่ถูกต้อง")

    def esc(val):
        return str(val or "").replace("\\", "\\\\").replace("'", "\\'")

    branch = data.branch.strip().upper()
    branch_sql = esc(branch)
    emp_id = esc((data.emp_id or "").strip())
    completed_at = esc((data.completed_at or "").strip())

    frontend_summary = None
    if data.summary:
        frontend_summary = normalize_report_summary(data.summary.dict(), wave_clean, branch)
        frontend_summary["is_closed"] = True
        if int(frontend_summary.get("label_count") or 0) > 0 and frontend_summary.get("total", 0) <= 0:
            raise HTTPException(
                status_code=409,
                detail="ยังปิดสาขาไม่ได้: พบรายการ LPN แต่ยอดรวมเป็น 0 กรุณารีเฟรชข้อมูลและตรวจคิวส่งก่อนกดปิดอีกครั้ง",
            )

    insert_zero_query = f"""
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`)
        WITH Expected AS (
            SELECT TRIM(UPPER(CAST(LPN AS STRING))) AS LPN
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {wave_clean}
              AND TRIM(UPPER(CAST(Branch_Code AS STRING))) = '{branch_sql}'
        ),
        Scanned AS (
            SELECT TRIM(UPPER(CAST(LPN AS STRING))) AS LPN
            FROM `pro-analytics-db.logistics_db.app_scan_transactions`
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {wave_clean}
        )
        SELECT '{wave_clean}', e.LPN, 'AUTO_NOT_FOUND', 'None', 0, CURRENT_TIMESTAMP()
        FROM Expected e
        LEFT JOIN Scanned s ON e.LPN = s.LPN
        WHERE s.LPN IS NULL
    """

    marker_timestamp = completed_at or (datetime.datetime.now(datetime.timezone.utc).isoformat())
    marker_row = {
        "Wave_Number": wave_clean,
        "LPN": f"BRANCH_{branch}",
        "Scan_Type": "CLOSE_JOB",
        "Color": "None",
        "Qty": 0,
        "Timestamp": marker_timestamp,
        "Branch_Code": branch,
        "Emp_ID": emp_id,
        "Pallet_No": 0,
    }
    marker_rows = [marker_row]
    marker_row_ids = [f"close-{wave_clean}-{branch}-{marker_timestamp}"]
    if frontend_summary and frontend_summary.get("total", 0) > 0:
        marker_rows.append({
            "Wave_Number": wave_clean,
            "LPN": f"BRANCH_SUMMARY_{branch}",
            "Scan_Type": "CLOSE_SUMMARY",
            "Color": json.dumps(frontend_summary, ensure_ascii=False, separators=(",", ":")),
            "Qty": int(frontend_summary["total"]),
            "Timestamp": marker_timestamp,
            "Branch_Code": branch,
            "Branch_Name": frontend_summary.get("branch_name") or branch,
            "Emp_ID": emp_id,
            "Pallet_No": int(frontend_summary.get("pallet") or 0),
        })
        marker_row_ids.append(f"close-summary-{wave_clean}-{branch}-{marker_timestamp}")
    marker_errors = client.insert_rows_json(
        client.dataset("logistics_db").table("app_scan_transactions"),
        marker_rows,
        row_ids=marker_row_ids,
    )
    if marker_errors:
        raise HTTPException(status_code=503, detail=f"บันทึกสถานะปิดสาขายังไม่สำเร็จ: {marker_errors}")

    record_shared_branch_closed(wave_clean, branch, completed_at, emp_id)

    if frontend_summary:
        # Fast snapshot from the exact numbers visible on screen; server reconciliation follows.
        queue_report_summary_snapshots([frontend_summary], delay_seconds=0.0)

    # ตอบกลับทันที: งาน BigQuery ทั้งหมดทำเบื้องหลัง ไม่ query ซ้ำใน request ปิดสาขา
    background_tasks.add_task(
        run_close_job_queries_in_background,
        wave_clean,
        branch,
        insert_zero_query,
        frontend_summary
    )

    return {
        "status": "success",
        "message": f"ปิดจบงานสาขา {branch} และบันทึกยอด 0 ให้กล่องที่ค้างสำเร็จ! (ระบบกำลังบันทึกเบื้องหลัง)",
        "completed_at": data.completed_at
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


def query_pending_waves_from_bigquery():
    if UAT_SHEETS_ONLY:
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
    query = """
        WITH Waves AS (
            SELECT DISTINCT
                SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS Wave_ID
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
            WHERE Wave_Number IS NOT NULL
              AND TRIM(CAST(Wave_Number AS STRING)) != ''
        )
        SELECT LPAD(CAST(Wave_ID AS STRING), 10, '0') AS Wave_Number
        FROM Waves
        WHERE Wave_ID IS NOT NULL
        ORDER BY Wave_ID DESC
        LIMIT 50
    """
    query_job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(use_query_cache=True)
    )
    results = query_job.result(timeout=BQ_JOB_TIMEOUT_SECONDS)
    waves = [{"wave_no": str(row["Wave_Number"]).strip()} for row in results]
    return {"success": True, "waves": waves, "cached": False}


def _startup_warm_cache():
    """⚡ Standard Plan: ดึงรายการ Wave ล่วงหน้าตอน startup พร้อม preload Wave cache"""
    try:
        data = query_pending_waves_from_bigquery()
        with pending_waves_cache_lock:
            pending_waves_cache["data"] = data
            pending_waves_cache["expires_at"] = time.time() + PENDING_WAVES_CACHE_TTL_SECONDS
        print("✅ Startup cache warm-up complete (pending waves)")

        # ⚡ Standard Plan: 2GB RAM → preload Wave แรกๆ ให้พร้อมเลย
        waves_to_preload = (data.get("waves") or [])[:3]
        if waves_to_preload:
            import threading
            def _preload_waves():
                for w in waves_to_preload:
                    try:
                        wno = str(w.get("wave_no") or "").strip()
                        if wno:
                            get_wave_data_internal(wno, force_refresh=False)
                            print(f"⚡ Preloaded wave {wno} into cache")
                    except Exception as we:
                        print(f"⚠️ Wave preload skipped {w}: {we}")
            threading.Thread(target=_preload_waves, daemon=True).start()
    except Exception as e:
        print(f"⚠️ Startup cache warm-up failed (non-critical): {e}")

    # เตรียม metadata Booking ล่วงหน้า เพื่อให้การค้นหา Booking ครั้งแรกไม่ต้องรอ DDL + 2 queries
    try:
        get_booking_branch_assignments()
        get_booking_branch_splits()
    except Exception as exc:
        print(f"⚠️ Booking metadata warm-up skipped: {exc}")

    # โหลดประวัติกล่องจาก Member Data ไว้ล่วงหน้า ไม่ให้การค้นหา Wave แรกต้องรอ Google Sheet
    load_member_history()

    if UAT_SHEETS_ONLY:
        try:
            load_booking_wave_sheet_meta(force=True)
            ensure_uat_event_sheets()
            print("✅ UAT Google Sheets data source ready")
        except Exception as exc:
            # ไม่ทำให้ Service ล้มตอนเริ่ม หากเพิ่งแชร์ Sheet หรือ API สะดุดชั่วคราว
            print(f"⚠️ UAT Sheet setup skipped at startup: {exc}")


def recover_recent_report_syncs():
    """Recover only closed/corrected branches; open scans must not trigger Sheet/BigQuery rebuilds."""
    if UAT_SHEETS_ONLY:
        return
    try:
        query = """
            SELECT DISTINCT
                CAST(SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS STRING) AS wave,
                TRIM(UPPER(CAST(Branch_Code AS STRING))) AS branch
            FROM `pro-analytics-db.logistics_db.app_scan_transactions`
            WHERE SAFE_CAST(Timestamp AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
              AND SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) IS NOT NULL
              AND NULLIF(TRIM(CAST(Branch_Code AS STRING)), '') IS NOT NULL
              AND (
                    UPPER(TRIM(CAST(Scan_Type AS STRING))) IN ('CLOSE_JOB', 'CLOSE_SUMMARY')
                    OR STARTS_WITH(UPPER(TRIM(CAST(Scan_Type AS STRING))), 'CORRECTION|')
                  )
            LIMIT 500
        """
        rows = list(client.query(query).result(timeout=BQ_JOB_TIMEOUT_SECONDS))
        pairs = {(str(row["wave"] or ""), str(row["branch"] or "").strip().upper()) for row in rows}
        pairs = {(wave, branch) for wave, branch in pairs if wave and branch}
        if pairs:
            queue_branch_totals_reconciliation(pairs, delay_seconds=1.0)
            print(f"♻️ REPORT SYNC RECOVERY | queued={len(pairs)} recent Wave+Branch pairs")
    except Exception as exc:
        print(f"⚠️ REPORT SYNC RECOVERY skipped: {exc}")

@app.on_event("startup")
async def startup_event():
    """⚡ Standard Plan: Pre-warm cache + preload Wave cache ตอน server เริ่มทำงาน"""
    ensure_report_sync_worker_started()
    threading.Thread(target=_startup_warm_cache, daemon=True).start()
    threading.Thread(target=recover_recent_report_syncs, daemon=True, name="report-sync-recovery").start()

def run_pending_waves_refresh_in_background():
    global is_refreshing_pending_waves
    try:
        data = query_pending_waves_from_bigquery()
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
        data = query_pending_waves_from_bigquery()
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


# 🔍 [DEBUG] ตรวจการคำนวณจำนวนกล่องของ PP/SP รายตัว (read-only)
# ใช้หาสาเหตุ "SP นับไม่ตรง": ดูว่าแต่ละบรรทัดสินค้าใน LPN เจอใน pack_case_map ไหม,
# case_count เท่าไหร่, total_pieces เท่าไหร่, และใช้กติกา (A/B/C) ตัวไหนคำนวณ
@app.get("/api/debug-carton")
def debug_carton(wave_no: str, lpn: str):
    try:
        search_wave_id = int(wave_no.strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ต้องเป็นตัวเลขเท่านั้น")

    lpn_target = lpn.strip().upper()
    wave_detail_str = f"{search_wave_id:010d}"

    query = """
        SELECT
            TRIM(CAST(d.Owner AS STRING)) AS owner,
            TRIM(CAST(d.Product_Code AS STRING)) AS product_code,
            d.Total_Pieces AS total_pieces,
            d.Total_Qty AS row_total_qty
        FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record` AS d
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(d.Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = @wave_id
          AND TRIM(UPPER(CAST(d.LPN AS STRING))) = @lpn
    """
    config = bigquery.QueryJobConfig(
        use_query_cache=True,
        query_parameters=[
            bigquery.ScalarQueryParameter("wave_id", "INT64", search_wave_id),
            bigquery.ScalarQueryParameter("lpn", "STRING", lpn_target),
        ],
    )

    try:
        rows = list(client.query(query, job_config=config).result(timeout=BQ_JOB_TIMEOUT_SECONDS))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not rows:
        raise HTTPException(status_code=404, detail=f"ไม่พบ LPN [{lpn_target}] ใน Wave [{search_wave_id}]")

    pack_case_map = load_pack_case_map()
    breakdown = []
    total_qty = 0
    for r in rows:
        owner = str(get_row_value(r, "owner", "") or "").strip().upper()
        product_code = str(get_row_value(r, "product_code", "") or "").strip().upper()
        pieces = to_float(get_row_value(r, "total_pieces", 0))
        row_total_qty = to_float(get_row_value(r, "row_total_qty", 0))
        key = f"{owner}|{product_code}"
        case_count = pack_case_map.get(key) or pack_case_map.get(product_code)

        if case_count and pieces > 0:
            cartons = max(1, int(math.ceil(pieces / case_count)))
            rule = "A: ceil(total_pieces / case_count)"
        elif row_total_qty > 0:
            cartons = max(1, int(math.ceil(row_total_qty)))
            rule = "B: fallback ceil(row_total_qty)"
        else:
            cartons = 1
            rule = "C: default 1"
        total_qty += cartons

        breakdown.append({
            "owner": owner,
            "product_code": product_code,
            "pack_case_map_key": key,
            "in_pack_case_map": case_count is not None,
            "case_count": case_count,
            "total_pieces": pieces,
            "row_total_qty": row_total_qty,
            "cartons_counted": cartons,
            "rule_used": rule,
        })

    return {
        "wave_no": wave_detail_str,
        "lpn": lpn_target,
        "is_direct_qty_prefix": is_direct_qty_lpn_value(lpn_target),
        "detail_row_count": len(rows),
        "computed_total_qty": total_qty if total_qty > 0 else 1,
        "rows": breakdown,
    }
