from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from typing import List, Optional       # ✅ แก้ไข #1: เพิ่ม Optional
from google.cloud import bigquery
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
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# ✅ GZip Compression: ลด payload size สำหรับ response ขนาดใหญ่ (Wave data)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ✅ จำกัด CORS: อนุญาตเฉพาะหน้าเว็บบน GitHub Pages (*.github.io) + localhost สำหรับทดสอบ
#    ถ้าใช้โดเมนอื่น (custom domain) ให้เพิ่ม origin นั้นใน ALLOWED_ORIGINS ด้วย
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
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

client = bigquery.Client(
    project=os.environ.get("GOOGLE_CLOUD_PROJECT", "pro-analytics-db")
)

# ✅ BigQuery Job Timeout: ป้องกัน query ค้างนานโดยไม่จำกัด
BQ_JOB_TIMEOUT_SECONDS = 30

NUMERIC_BRANCH_MASTER_SPREADSHEET_ID = "1zI5YAq0JvlM-WsaCfDVYVZgiCn5pWx_HVJjQMiTFwoI"
NUMERIC_BRANCH_MASTER_SHEET_NAME = "Master"
NUMERIC_BRANCH_MASTER_GID = "606346592"
NUMERIC_BRANCH_MASTER_CACHE_TTL_SECONDS = 30 * 60
numeric_branch_master_cache = {"expires_at": 0.0, "data": {}}
numeric_branch_master_lock = Lock()

# ไฟล์ Control Outbound ที่ใช้งานจริง (Member Data เป็นแท็บแรก)
MEMBER_HISTORY_SPREADSHEET_ID = "1MO3lu1GssPZZvaruwQ5trUB045dzh4HUHdH35mbyOtc"
MEMBER_HISTORY_GID = "0"
MEMBER_HISTORY_CACHE_TTL_SECONDS = 10 * 60
member_history_cache = {"expires_at": 0.0, "data": {}}
member_history_lock = Lock()

DELIVERY_REPORT_SPREADSHEET_ID = "1_giWrKy5bi8cpmdM-2ui1_vG9jQ7kEhf4yxMvHpj-XY"
DELIVERY_REPORT_SHEET_NAME = "Delivery report"
DELIVERY_CAR_SHEET_NAME = "Data Booking&Car"
DELIVERY_BRANCH_SHEET_NAME = "Sheet3"
delivery_report_lock = Lock()
delivery_lookup_cache = {"expires_at": 0.0, "cars": {}, "branches": {}}

def get_sheets_session():
    credentials = client._credentials
    if hasattr(credentials, "with_scopes"):
        credentials = credentials.with_scopes(["https://www.googleapis.com/auth/spreadsheets"])
    return AuthorizedSession(credentials)

def member_data_bu(owner) -> str:
    """แปลงรหัส BU จากข้อมูล Wave ให้เป็นชื่อที่หน้างานใช้ใน Member Data."""
    code = str(owner or "").strip().upper()
    return {
        "DP02": "PUNTHAI",
        "DM02": "MAX MART",
        "MAXMART": "MAX MART",
        "MAX MART": "MAX MART",
    }.get(code, code or "Unknown")

def write_member_history_summary(summary: dict):
    """Upsert one completed Wave+Branch row in Member Data."""
    session = get_sheets_session()
    sheet_range = urllib.parse.quote("Member Data!A:P", safe="")
    lookup_range = urllib.parse.quote("Member Data!A:D", safe="")
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{MEMBER_HISTORY_SPREADSHEET_ID}/values"
    # อ่านแค่ A:D เพราะคอลัมน์ Outbound ถูกลงสูตรยาวถึงท้ายชีต
    # ถ้าอ่าน A:P จะเข้าใจผิดว่าแถวสูตรว่างคือข้อมูลจริง แล้ว append ไปไกลมาก
    read_res = session.get(f"{base}/{lookup_range}", timeout=45)
    read_res.raise_for_status()
    values = read_res.json().get("values") or []
    target_wave = str(int(summary["wave"]))
    target_branch = str(summary["branch"]).strip().upper()
    row_no = 0
    last_data_row = 1
    blank_run = 0
    misplaced_rows = []
    for index, row in enumerate(values[1:], start=2):
        row = list(row) + [""] * max(0, 4 - len(row))
        has_core_data = bool(str(row[0]).strip() or str(row[2]).strip() or str(row[3]).strip())
        if not has_core_data:
            blank_run += 1
            continue
        # ช่องว่างยาวเป็นแถวที่เตรียมรูปแบบไว้ ไม่ใช่ข้อมูลจริง
        if blank_run > 50:
            if str(row[3]).strip().upper() == target_branch and re.sub(r"\D", "", str(row[2] or "")) == target_wave:
                misplaced_rows.append(index)
            continue
        blank_run = 0
        last_data_row = index
        wave_digits = re.sub(r"\D", "", str(row[2] or ""))
        if wave_digits and str(int(wave_digits)) == target_wave and str(row[3]).strip().upper() == target_branch:
            row_no = index
            break
    now_bkk = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))
    row_values = [[
        now_bkk.strftime("%-d/%-m/%Y") if os.name != "nt" else f"{now_bkk.day}/{now_bkk.month}/{now_bkk.year}",
        now_bkk.strftime("%H:%M"), target_wave, target_branch, summary["branch_name"], summary["bu"],
        summary["label_count"], "", summary["m"], summary["red"], summary["blue"],
        summary["green"], summary["black"], summary["total"], summary["pallet"], " Outbound"
    ]]
    if row_no:
        target_range = urllib.parse.quote(f"Member Data!A{row_no}:P{row_no}", safe="")
        response = session.put(f"{base}/{target_range}?valueInputOption=USER_ENTERED", json={"values": row_values}, timeout=45)
    else:
        next_row = last_data_row + 1
        target_range = urllib.parse.quote(f"Member Data!A{next_row}:P{next_row}", safe="")
        response = session.put(f"{base}/{target_range}?valueInputOption=USER_ENTERED", json={"values": row_values}, timeout=45)
    response.raise_for_status()
    for bad_row in misplaced_rows:
        clear_range = urllib.parse.quote(f"Member Data!A{bad_row}:P{bad_row}", safe="")
        cleared = session.post(f"{base}/{clear_range}:clear", json={}, timeout=30)
        cleared.raise_for_status()
    with member_history_lock:
        member_history_cache["expires_at"] = 0
    print(f"✅ Member Data updated | Wave {target_wave} | Branch {target_branch} | {summary['total']} boxes")

def write_member_history_summaries(summaries: list):
    """Upsert document totals. Used after a supervisor confirms edited print values."""
    for summary in summaries:
        write_member_history_summary(summary)

def _sheet_values(session, spreadsheet_id: str, a1_range: str) -> list:
    encoded = urllib.parse.quote(a1_range, safe="")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded}"
    response = session.get(url, timeout=60)
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

def get_delivery_wave_meta(wave: str) -> dict:
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

def write_delivery_report_summary(summary: dict):
    """Upsert one Wave+Branch summary into Delivery report A:T."""
    wave = str(int(str(summary["wave"]).strip()))
    branch = str(summary["branch"] or "").strip().upper()
    meta = get_delivery_wave_meta(wave)
    pick_date = meta.get("pick_date")
    if not pick_date:
        raise ValueError(f"Planned_Pick_Date not found for Wave {wave}")
    order_date, delivery_date = delivery_business_dates(pick_date)
    session = get_sheets_session()
    cars, branches = load_delivery_lookup_maps(session)
    booking = str(summary.get("booking") or meta.get("booking") or "").strip().upper()
    car = cars.get(booking, {})
    branch_master = branches.get(branch, {})

    with delivery_report_lock:
        existing = _sheet_values(session, DELIVERY_REPORT_SPREADSHEET_ID, f"'{DELIVERY_REPORT_SHEET_NAME}'!A:K")
        target_row = 0
        last_data_row = 1
        max_sequence = 0
        for index, row in enumerate(existing[1:], start=2):
            row = list(row) + [""] * max(0, 11 - len(row))
            try: max_sequence = max(max_sequence, int(float(str(row[0] or 0))))
            except (TypeError, ValueError): pass
            row_wave = re.sub(r"\D", "", str(row[9] or ""))
            row_branch = str(row[10] or "").strip().upper()
            if str(row[7] or "").strip() or row_wave or row_branch:
                last_data_row = index
            if row_wave and str(int(row_wave)) == wave and row_branch == branch:
                target_row = index
                break
        if not target_row:
            target_row = last_data_row + 1
        sequence = max_sequence + 1 if target_row > last_data_row else _history_int(existing[target_row - 1][0])
        if sequence <= 0:
            sequence = max_sequence + 1
        row_values = [[
            sequence, _date_serial(order_date), _date_serial(pick_date), _date_serial(delivery_date),
            car.get("carrier", ""), car.get("driver", ""), car.get("plate", ""), booking,
            member_data_bu(summary.get("bu")), wave, branch, clean_branch_display_name(summary.get("branch_name")),
            branch_master.get("province", ""), branch_master.get("region", ""),
            _history_int(summary.get("m")), _history_int(summary.get("red")), _history_int(summary.get("blue")),
            _history_int(summary.get("green")), _history_int(summary.get("black")), _history_int(summary.get("total"))
        ]]
        encoded = urllib.parse.quote(f"'{DELIVERY_REPORT_SHEET_NAME}'!A{target_row}:T{target_row}", safe="")
        base = f"https://sheets.googleapis.com/v4/spreadsheets/{DELIVERY_REPORT_SPREADSHEET_ID}/values"
        response = session.put(f"{base}/{encoded}?valueInputOption=RAW", json={"values": row_values}, timeout=60)
        response.raise_for_status()
    print(f"✅ Delivery report updated | Wave {wave} | Branch {branch} | Row {target_row}")

def summarize_branch_for_member_data(wave_data: dict, branch: str) -> dict:
    items = [item for item in wave_data.get("lpn_list", []) if str(item.get("branch") or "").strip().upper() == branch]
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
    pallet_nos = {int(no) for item in items for no in (item.get("branch_pallet_nos") or []) if int(no) > 0}
    if not pallet_nos: pallet_nos = {int(item.get("pallet_no") or 0) for item in items if int(item.get("pallet_no") or 0) > 0}
    wave_key = str(int(str(wave_data.get("wave_no") or 0)))
    assignment = get_booking_branch_assignments().get((wave_key, branch))
    current_booking = str((assignment or {}).get("Assigned_Booking") or wave_data.get("booking_no") or "").strip().upper()
    return {"wave": wave_key, "booking": current_booking, "branch": branch,
            "branch_name": first.get("branch_name") or branch, "bu": member_data_bu(first.get("owner")),
            "label_count": len({str(item.get("lpn") or "") for item in items if item.get("lpn")}),
            **totals, "total": sum(totals.values()), "pallet": len(pallet_nos)}

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
VALID_LPNS_CACHE_TTL = 600  # 10 minutes cache
valid_lpns_cache = {}  # wave_no -> {"lpns": set((lpn, branch_code)), "expires_at": float}
valid_lpns_cache_lock = Lock()

# --- ULTRA-FAST WAVE AND BOOKING SEARCH CACHE ---
WAVE_CACHE_TTL = 600  # 10 minutes cache
wave_cache = {}  # wave_detail_str -> {"data": dict, "expires_at": float}
wave_cache_lock = Lock()
wave_query_locks = {}
wave_query_locks_guard = Lock()

BOOKING_WAVES_CACHE_TTL = 600  # 10 minutes cache
booking_waves_cache = {}  # booking_clean -> {"mapping": dict, "expires_at": float}
booking_waves_cache_lock = Lock()

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

    query = f"""
        WITH QCRaw AS (
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
        ),
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
                IF(Qty = 0 OR Scan_Type IN ('RESET_BOX', 'CANCEL_COMBINE'), 1, 0) AS Is_Reset
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
              AND (lr.Reset_Timestamp IS NULL OR r.Timestamp > lr.Reset_Timestamp)
        ),
        LatestColorScan AS (
            SELECT * EXCEPT(rn)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Scan_Wave_ID, Clean_LPN, Scan_Branch, Pallet_No, UPPER(IFNULL(Color, 'None'))
                        ORDER BY Timestamp DESC, Qty DESC
                    ) AS rn
                FROM ValidScanRows
            )
            WHERE rn = 1
        ),
        ScanHistory AS (
            SELECT
                Scan_Wave_ID,
                Clean_LPN,
                Scan_Branch,
                SUM(Qty) AS Scanned_Qty,
                MAX(Pallet_No) AS Scanned_Pallet_No,
                ARRAY_AGG(Scan_Type ORDER BY Timestamp DESC LIMIT 1)[OFFSET(0)] AS Scan_Type,
                ARRAY_AGG(Color ORDER BY Timestamp DESC LIMIT 1)[OFFSET(0)] AS Color,
                STRING_AGG(CONCAT(IFNULL(Color, 'None'), '~', CAST(Qty AS STRING), '~', IFNULL(Scan_Type, '')), '|') AS Color_Breakdown,
                STRING_AGG(CONCAT(CAST(Pallet_No AS STRING), '~', IFNULL(Color, 'None'), '~', CAST(Qty AS STRING), '~', IFNULL(Scan_Type, '')), '|' ORDER BY Pallet_No, Timestamp) AS Pallet_Breakdown
            FROM LatestColorScan
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
            MAX(s.Color_Breakdown) AS color_breakdown,
            MAX(s.Pallet_Breakdown) AS pallet_breakdown,
            ANY_VALUE(ps.Pallet_Nos) AS branch_pallet_nos,
            MAX(pc.Pallet_Color) AS pallet_color,
            ANY_VALUE(sps.Submitted_Pallet_Nos) AS branch_submitted_pallet_nos,
            ANY_VALUE(bcs.Branch_Closed_At) AS branch_closed_at,
            ANY_VALUE(bcs.Branch_Closed_By) AS branch_closed_by,
            COALESCE(MAX(qc.QC_Required), FALSE) AS qc_required,
            ROUND(MAX(qc.QC_Risk), 4) AS qc_risk,
            MAX(qc.QC_Group) AS qc_source,
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
        LEFT JOIN QCStatus AS qc
          ON TRIM(UPPER(d.LPN)) = qc.Clean_LPN
         AND TRIM(UPPER(d.Branch_Code)) = qc.Branch_Code
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
    meta_job = client.query(meta_query, job_config=job_config)
    query_job = client.query(query, job_config=job_config)

    meta_rows = list(meta_job.result(timeout=BQ_JOB_TIMEOUT_SECONDS))
    booking_no = ""
    license_plate = ""
    if len(meta_rows) > 0:
        booking_no = meta_rows[0]["booking_no"] or ""
        license_plate = meta_rows[0]["license_plate"] or ""

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
            "color_breakdown": row["color_breakdown"] or "",
            "pallet_breakdown": pallet_breakdown,
            "pallet_no": row["pallet_no"] if row["pallet_no"] is not None else 0,
            "branch_pallet_nos": list(row["branch_pallet_nos"] or []),
            "pallet_color": row["pallet_color"] or "",
            "branch_submitted_pallet_nos": list(row["branch_submitted_pallet_nos"] or []),
            "branch_closed_at": row["branch_closed_at"].isoformat() if row["branch_closed_at"] else "",
            "branch_closed_by": row["branch_closed_by"] or "",
            "qc_required": bool(row["qc_required"]),
            "qc_status": "ต้อง QC" if row["qc_required"] else "",
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
    try:
        search_wave_id = int(wave_no.strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ต้องเป็นตัวเลขเท่านั้น")

    wave_detail_str = f"{search_wave_id:010d}"
    now = time.time()

    if not force_refresh:
        with wave_cache_lock:
            cached = wave_cache.get(wave_detail_str)
            if cached and cached["expires_at"] > now:
                return cached["data"]

    # ป้องกันหลาย Handheld ยิง Query Wave เดียวกันพร้อมกันตอน cache หมดอายุ
    with wave_query_locks_guard:
        query_lock = wave_query_locks.setdefault(wave_detail_str, Lock())
    with query_lock:
        if not force_refresh:
            with wave_cache_lock:
                cached = wave_cache.get(wave_detail_str)
                if cached and cached["expires_at"] > time.time():
                    return cached["data"]

        data = fetch_wave_data_from_bq(search_wave_id)
        with wave_cache_lock:
            wave_cache[wave_detail_str] = {
                "data": data,
                "expires_at": time.time() + WAVE_CACHE_TTL
            }
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
    if not force_refresh:
        with booking_waves_cache_lock:
            cached = booking_waves_cache.get(clean_booking)
            if cached and cached["expires_at"] > now:
                return cached["mapping"]
                
    mapping = fetch_booking_waves_from_bq(booking_no)
    with booking_waves_cache_lock:
        booking_waves_cache[clean_booking] = {
            "mapping": mapping,
            "expires_at": now + BOOKING_WAVES_CACHE_TTL
        }
    return mapping

def get_booking_data_internal(booking_no: str, force_refresh: bool = False) -> dict:
    mapping = get_booking_waves_mapping(booking_no, force_refresh)
    booking_clean = booking_no.strip().upper()
    assignments = get_booking_branch_assignments()
    override_waves = [wave for (wave, branch), move in assignments.items()
                      if str(move.get("Assigned_Booking") or "").strip().upper() == booking_clean]
    waves = list(dict.fromkeys(list(mapping["waves"]) + override_waves))
    license_plate = mapping["license_plate"]
    
    lpn_list = []
    waves_included = set()
    wave_results = []
    
    # Fetch all waves in parallel using ThreadPoolExecutor to prevent slow sequential BigQuery queries
    with ThreadPoolExecutor(max_workers=max(1, len(waves))) as executor:
        futures = {executor.submit(get_wave_data_internal, wave, force_refresh): wave for wave in waves}
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
        for item in wave_data_overlaid.get("lpn_list", []):
            assignment = assignments.get((wave_key, str(item.get("branch") or "").strip().upper()))
            assigned_booking = str((assignment or {}).get("Assigned_Booking") or "").strip().upper()
            native_booking = str(wave_data_overlaid.get("booking_no") or "").strip().upper()
            effective_booking = assigned_booking or native_booking
            if effective_booking == booking_clean:
                if assignment:
                    item["booking_override"] = {
                        "previous_booking": assignment.get("Previous_Booking") or native_booking,
                        "assigned_booking": assignment.get("Assigned_Booking") or booking_clean,
                        "reason": assignment.get("Reason") or "",
                        "emp_id": assignment.get("Emp_ID") or "",
                        "created_at": assignment.get("Created_At").isoformat() if assignment.get("Created_At") else ""
                    }
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
            
    return {
        "booking_no": booking_no,
        "license_plate": license_plate,
        "waves": list(waves_included),
        "lpn_list": lpn_list,
        "zone_summary": list(zones_calc.values())
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

class CloseJobData(BaseModel):
    wave: str
    branch: str
    emp_id: Optional[str] = None
    completed_at: Optional[str] = None

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

class DocumentSummaryBatchData(BaseModel):
    summaries: List[DocumentSummaryData]
    emp_id: Optional[str] = None

def ensure_booking_override_table():
    client.query("""
        CREATE TABLE IF NOT EXISTS `pro-analytics-db.logistics_db.booking_branch_overrides` (
            Event_ID STRING, Wave_Number STRING, Branch_Code STRING,
            Previous_Booking STRING, Assigned_Booking STRING,
            Reason STRING, Note STRING, Emp_ID STRING, Created_At TIMESTAMP
        )
    """).result(timeout=BQ_JOB_TIMEOUT_SECONDS)

def get_booking_branch_assignments() -> dict:
    try:
        ensure_booking_override_table()
        rows = client.query("""
            SELECT Wave_Number, Branch_Code, Previous_Booking, Assigned_Booking,
                   Reason, Note, Emp_ID, Created_At
            FROM `pro-analytics-db.logistics_db.booking_branch_overrides`
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY TRIM(Wave_Number), UPPER(TRIM(Branch_Code))
                ORDER BY Created_At DESC, Event_ID DESC
            ) = 1
        """).result(timeout=BQ_JOB_TIMEOUT_SECONDS)
        return {(str(row["Wave_Number"]).strip(), str(row["Branch_Code"]).strip().upper()): dict(row.items()) for row in rows}
    except Exception as exc:
        print(f"BOOKING OVERRIDE READ ERROR: {exc}")
        return {}

# ==================== ROUTES & APIs ====================

@app.get("/")
async def read_root():
    return {"status": "ok", "message": "Scanner API is running perfectly!"}

# ✅ Health Check Endpoint: ตอบสนองเร็ว <5ms สำหรับ keep-alive heartbeat
@app.get("/api/health")
async def health_check():
    return {"status": "ok", "version": "1.7.1", "timestamp": time.time()}

@app.post("/api/document-summary")
def save_document_summary(data: DocumentSummaryBatchData):
    if not data.summaries or len(data.summaries) > 100:
        raise HTTPException(status_code=400, detail="summary count must be 1-100")
    normalized = []
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
        normalized.append({"wave": wave, "booking": str(item.booking or "").strip().upper(), "branch": branch,
                           "branch_name": str(item.branch_name or branch).strip(),
                           "bu": member_data_bu(item.bu), **values})
    try:
        write_member_history_summaries(normalized)
        for summary in normalized:
            write_delivery_report_summary(summary)
    except Exception as exc:
        print(f"🚨 DOCUMENT SUMMARY WRITE ERROR: {exc}")
        raise HTTPException(status_code=503, detail="Member Data write failed")
    return {"status": "success", "updated": len(normalized)}

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

# 🚀 [API 1] โหลดข้อมูล Wave
# 🚀 [API 1] โหลดข้อมูล Wave
@app.get("/api/check-wave")
def check_wave(wave_no: str, force: bool = False):
    try:
        # Load wave data (either from cache or by querying BigQuery if not cached)
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
@app.get("/api/check-booking")
def check_booking(booking_no: str, force: bool = False):
    try:
        booking_data = get_booking_data_internal(booking_no, force_refresh=force)
        return booking_data
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
    return {
        "status": "success", "wave_no": wave_clean, "branch_code": branch,
        "branch_name": items[0].get("branch_name") or "Unknown",
        "current_booking": current_booking, "lpn_total": len(items),
        "lpn_scanned": len(scanned), "box_qty": sum(int(item.get("qty") or 0) for item in scanned),
        "pallet_count": len({int(item.get("pallet_no") or 0) for item in scanned if int(item.get("pallet_no") or 0) > 0}),
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
    ensure_booking_override_table()
    query = """
        INSERT INTO `pro-analytics-db.logistics_db.booking_branch_overrides`
        (Event_ID, Wave_Number, Branch_Code, Previous_Booking, Assigned_Booking, Reason, Note, Emp_ID, Created_At)
        VALUES (@event_id, @wave, @branch, @previous, @target, @reason, @note, @emp_id, CURRENT_TIMESTAMP())
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("event_id", "STRING", str(uuid.uuid4())),
        bigquery.ScalarQueryParameter("wave", "STRING", wave_clean),
        bigquery.ScalarQueryParameter("branch", "STRING", branch),
        bigquery.ScalarQueryParameter("previous", "STRING", previous),
        bigquery.ScalarQueryParameter("target", "STRING", target),
        bigquery.ScalarQueryParameter("reason", "STRING", reason),
        bigquery.ScalarQueryParameter("note", "STRING", str(data.note or "").strip()),
        bigquery.ScalarQueryParameter("emp_id", "STRING", emp_id),
    ])
    client.query(query, job_config=config).result(timeout=BQ_JOB_TIMEOUT_SECONDS)
    with booking_waves_cache_lock:
        booking_waves_cache.pop(previous, None)
        booking_waves_cache.pop(target, None)
    return {"status": "success", "message": "ย้ายสาขาเรียบร้อย", "previous_booking": previous, "target_booking": target, "preview": preview}

@app.post("/api/start-pallet")
def start_pallet(data: PalletStartData):
    """Allocate one branch-wide pallet number so multiple handhelds cannot reuse the same number."""
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
    return {"status": "success", "pallet_no": pallet_no, "submitted_by": emp_id}

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
        return {"status": "success", "correction_id": correction_id, "duplicate": True}

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
    # ไม่ refresh BigQuery ซ้ำทุกครั้ง: local overlay อัปเดตทุกเครื่องได้ทันทีอยู่แล้ว
    return {"status": "success", "correction_id": correction_id, "audit": audit_payload}

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
        return {"status": "success", "message": "Saved"}
    except Exception as e:
        print(f"🚨 INSERT FALLBACK TO QUERY | LPN: {lpn_val} | Error: {str(e)}")
        # Fallback to standard query insert if streaming insert fails for any reason
        def esc(val):
            return (val or "").replace("\\", "\\\\").replace("'", "\\'")
        esc_lpn = esc(lpn_val)
        esc_branch = esc(branch_val)
        esc_branch_name = esc(branch_name_val)
        esc_emp = esc(emp_val)
        esc_type = esc(type_val)
        esc_color = esc(color_val)
        
        insert_query = f"""
            INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
            (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`, `Branch_Code`, `Branch_Name`, `Emp_ID`, `Pallet_No`)
            VALUES
            ('{wave_clean}', '{esc_lpn}', '{esc_type}', '{esc_color}', {data.qty},
             CURRENT_TIMESTAMP(), '{esc_branch}', '{esc_branch_name}', '{esc_emp}', {pallet_no_val})
        """
        try:
            client.query(insert_query).result()
            mark_transaction_processed(transaction_id)
            record_local_scan(wave_clean, lpn_val, branch_val, data.qty, type_val, color_val, pallet_no_val, base_pallet_breakdown)
            print(f"✅ SAVED (Fallback Query) | LPN: {lpn_val}")
            return {"status": "success", "message": "Saved"}
        except Exception as query_err:
            print(f"🚨 DOUBLE INSERT ERROR | LPN: {lpn_val} | Error: {str(query_err)}")
            raise HTTPException(status_code=500, detail=str(query_err))


# 🚀 [API 2.5] บันทึกข้อมูลสแกนเป็นชุด (Batch)
@app.post("/api/scan-batch")
def process_scan_batch(data: ScanBatchData, background_tasks: BackgroundTasks):
    if not data.scans:
        return {"status": "success", "message": "No scans to process", "processed_count": 0}

    import datetime
    table_ref = client.dataset("logistics_db").table("app_scan_transactions")
    rows_to_insert = []
    accepted_scans = []
    row_ids = []
    failed_scans = []
    for item in data.scans:
        if transaction_already_processed(item.transaction_id or ""):
            continue
        try:
            wave_clean = str(int(item.wave_no))
        except ValueError:
            failed_scans.append({"lpn": item.lpn, "reason": "รหัส Wave ไม่ถูกต้อง"})
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
                failed_scans.append({"lpn": item.lpn, "reason": f"ไม่พบ LPN ใน Wave/Branch"})
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
                    failed_scans.append({"lpn": item.lpn, "reason": f"ไม่พบ LPN ใน Wave/Branch (Fallback)"})
                    continue
            except Exception as e:
                failed_scans.append({"lpn": item.lpn, "reason": f"Check error: {str(e)}"})
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
        accepted_scans.append((wave_clean, lpn_val, branch_val, item.qty, type_val, color_val, pallet_no_val, (item.transaction_id or "").strip()))
        row_ids.append((item.transaction_id or "").strip() or str(uuid.uuid4()))

    # Trigger background refreshes
    # ไม่สร้าง BigQuery query เพิ่มตามจำนวน Wave หลัง batch; overlay ถูกบันทึกไว้แล้ว

    if not rows_to_insert:
        raise HTTPException(
            status_code=400,
            detail={"message": "ไม่มีข้อมูลสแกนที่ผ่านการตรวจสอบ", "errors": failed_scans}
        )

    try:
        errors = client.insert_rows_json(table_ref, rows_to_insert, row_ids=row_ids)
        if errors:
            print(f"🚨 BATCH INSERT ROWS JSON ERROR | Errors: {errors}")
            raise Exception(f"BigQuery streaming errors: {errors}")
        for scan in accepted_scans:
            record_local_scan(*scan[:7])
            mark_transaction_processed(scan[7])
        
        print(f"✅ BATCH SAVED | Processed: {len(rows_to_insert)} | Failed: {len(failed_scans)}")
        return {
            "status": "success", 
            "message": "Saved", 
            "processed_count": len(rows_to_insert), 
            "failed_count": len(failed_scans),
            "errors": failed_scans
        }
    except Exception as e:
        print(f"🚨 BATCH INSERT FALLBACK | Error: {str(e)}")
        # If streaming batch fails, fall back to inserting rows sequentially via query
        success_count = 0
        for row, scan in zip(rows_to_insert, accepted_scans):
            def esc(val):
                return (val or "").replace("\\", "\\\\").replace("'", "\\'")
            insert_query = f"""
                INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
                (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`, `Branch_Code`, `Branch_Name`, `Emp_ID`, `Pallet_No`)
                VALUES
                ('{row["Wave_Number"]}', '{esc(row["LPN"])}', '{esc(row["Scan_Type"])}', '{esc(row["Color"])}', {row["Qty"]},
                 CURRENT_TIMESTAMP(), '{esc(row["Branch_Code"])}', '{esc(row["Branch_Name"])}', '{esc(row["Emp_ID"])}', {int(row.get("Pallet_No", 0) or 0)})
            """
            try:
                client.query(insert_query).result()
                record_local_scan(*scan[:7])
                mark_transaction_processed(scan[7])
                success_count += 1
            except Exception as query_err:
                print(f"🚨 BATCH FALLBACK SINGLE INSERT ERROR | LPN: {row['LPN']} | Error: {str(query_err)}")
        
        return {
            "status": "partial_success" if success_count > 0 else "failed",
            "processed_count": success_count,
            "failed_count": len(rows_to_insert) - success_count + len(failed_scans),
            "errors": failed_scans
        }


def run_close_job_queries_in_background(wave_clean: str, branch: str, insert_zero_query: str, insert_close_marker: str):
    try:
        # Run BQ inserts
        client.query(insert_zero_query).result()
        client.query(insert_close_marker).result()
        # After BQ queries finish, refresh cache
        refreshed = get_wave_data_internal(wave_clean, force_refresh=True)
        try:
            summary = summarize_branch_for_member_data(refreshed, branch)
            write_member_history_summary(summary)
            write_delivery_report_summary(summary)
        except Exception as sheet_error:
            print(f"⚠️ REPORT SHEET WRITE ERROR | Wave: {wave_clean} | Branch: {branch} | {sheet_error}")
        print(f"✅ BACKGROUND CLOSE JOB COMPLETE | Wave: {wave_clean} | Branch: {branch}")
    except Exception as e:
        print(f"🚨 BACKGROUND CLOSE JOB ERROR | Wave: {wave_clean} | Branch: {branch} | Error: {str(e)}")


# 🚀 [API 5] ปิดจบงานสาขา
@app.post("/api/close-job")
def close_job(data: CloseJobData, background_tasks: BackgroundTasks):
    try:
        wave_clean = str(int(data.wave.strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ไม่ถูกต้อง")

    def esc(val):
        return str(val or "").replace("\\", "\\\\").replace("'", "\\'")

    branch = esc(data.branch.strip().upper())
    emp_id = esc((data.emp_id or "").strip())
    completed_at = esc((data.completed_at or "").strip())
    timestamp_expr = "CURRENT_TIMESTAMP()"
    if completed_at:
        timestamp_expr = f"COALESCE(SAFE_CAST('{completed_at}' AS TIMESTAMP), CURRENT_TIMESTAMP())"

    insert_zero_query = f"""
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`)
        WITH Expected AS (
            SELECT TRIM(UPPER(CAST(LPN AS STRING))) AS LPN
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {wave_clean}
              AND TRIM(UPPER(CAST(Branch_Code AS STRING))) = '{branch}'
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

    insert_close_marker = f"""
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`, `Branch_Code`, `Emp_ID`)
        VALUES ('{wave_clean}', 'BRANCH_{branch}', 'CLOSE_JOB', 'None', 0, {timestamp_expr}, '{branch}', '{emp_id}')
    """

    record_shared_branch_closed(wave_clean, branch, completed_at, emp_id)

    # ตอบกลับทันที: งาน BigQuery ทั้งหมดทำเบื้องหลัง ไม่ query ซ้ำใน request ปิดสาขา
    background_tasks.add_task(
        run_close_job_queries_in_background,
        wave_clean,
        branch,
        insert_zero_query,
        insert_close_marker
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
    """ดึงรายการ Wave ล่วงหน้าตอน startup เพื่อให้พร้อมใช้งานทันที"""
    try:
        data = query_pending_waves_from_bigquery()
        with pending_waves_cache_lock:
            pending_waves_cache["data"] = data
            pending_waves_cache["expires_at"] = time.time() + PENDING_WAVES_CACHE_TTL_SECONDS
        print("✅ Startup cache warm-up complete")
    except Exception as e:
        print(f"⚠️ Startup cache warm-up failed (non-critical): {e}")

    # โหลดประวัติกล่องจาก Member Data ไว้ล่วงหน้า ไม่ให้การค้นหา Wave แรกต้องรอ Google Sheet
    load_member_history()

@app.on_event("startup")
async def startup_event():
    """Pre-warm cache ตอน server เริ่มทำงาน เพื่อให้ response เร็วตั้งแต่ request แรก"""
    import threading
    threading.Thread(target=_startup_warm_cache, daemon=True).start()

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
