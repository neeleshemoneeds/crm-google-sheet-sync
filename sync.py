import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

REQUEST_TIMEOUT = 90
PAGE_LIMIT = 200
MAX_PAGES = 50
MAX_RETRIES = 3

# ============ MANUAL DATE RANGE ============
MANUAL_START_DATE = "2026-01-01"

# ================ SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# =========== GOOGLE SHEET AUTH ============
creds = Credentials.from_service_account_info(
    SERVICE_ACCOUNT_JSON,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

# ============ DATE FILTER =================
lead_date_after = MANUAL_START_DATE
lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ============ EXISTING DATA ============
existing_data = sheet.get_all_records()

existing_leads = {}
existing_status = {}
existing_stage = {}

for idx, row in enumerate(existing_data, start=2):
    lid = str(row.get("lead_id") or row.get("id") or "")
    if lid:
        existing_leads[lid] = idx
        existing_status[lid] = row.get("lead_status", "")
        existing_stage[lid] = str(row.get("stage_id", ""))

headers = sheet.row_values(1)

STATUS_COL = headers.index("lead_status") + 1
STAGE_COL = headers.index("stage_id") + 1
LAST_UPDATED_COL = headers.index("last_updated") + 1
STATUS_LOG_COL = headers.index("status_log") + 1

print("🚀 Sync started")
print("From:", lead_date_after, "To:", lead_date_before)

page = 0
total_new = 0
total_updated = 0

# ============ MAIN LOOP ===================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT
    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_date_before": lead_date_before,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            print(f"⏳ Timeout page {page+1}, retry {attempt}/{MAX_RETRIES}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

    data = response.json().get("lead_data", [])
    if not data:
        break

    new_rows = []
    updates = []

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id") or "")
        if not lead_id:
            continue

        lead_status = item.get("lead_status", "")
        stage_id = str(item.get("stage_id", ""))
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ===== EXISTING LEAD =====
        if lead_id in existing_leads:
            row_no = existing_leads[lead_id]

            old_status = existing_status.get(lead_id, "")
            old_stage = existing_stage.get(lead_id, "")

            status_changed = lead_status != old_status
            stage_changed = stage_id != old_stage

            if status_changed:
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_no, STATUS_COL),
                    "values": [[lead_status]]
                })
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_no, LAST_UPDATED_COL),
                    "values": [[now_time]]
                })
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_no, STATUS_LOG_COL),
                    "values": [[f"{old_status} → {lead_status}"]]
                })
                total_updated += 1

            if stage_changed:
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_no, STAGE_COL),
                    "values": [[stage_id]]
                })

        # ===== NEW LEAD =====
        else:
            row = [""] * len(headers)
            for i, h in enumerate(headers):
                if h == "lead_id":
                    row[i] = lead_id
                elif h == "lead_status":
                    row[i] = lead_status
                elif h == "stage_id":
                    row[i] = stage_id
                elif h == "last_updated":
                    row[i] = now_time
                elif h == "status_log":
                    row[i] = "NEW"
                else:
                    v = item.get(h, "")
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    row[i] = v

            new_rows.append(row)
            total_new += 1

    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")

    if updates:
        sheet.batch_update(updates)

    page += 1
    time.sleep(1)

print("🎉 DONE")
print("🆕 New Leads:", total_new)
print("🔁 Status Updated:", total_updated)
