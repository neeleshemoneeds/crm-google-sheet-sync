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
MANUAL_START_DATE = "2025-10-01"

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
    lid = row.get("lead_id") or row.get("id")
    if lid:
        existing_leads[str(lid)] = idx
        existing_status[str(lid)] = row.get("lead_status", "")
        existing_stage[str(lid)] = str(row.get("stage_id", ""))

headers = sheet.row_values(1)
header_index = {h: i + 1 for i, h in enumerate(headers)}

STATUS_COL = header_index.get("lead_status")
STAGE_COL = header_index.get("stage_id")
LAST_UPDATED_COL = header_index.get("last_updated")

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
        "lead_offset": offset,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

    data = response.json().get("lead_data", [])
    if not data:
        break

    new_rows = []
    updates = []

    now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id"))
        if not lead_id:
            continue

        lead_status = item.get("lead_status", "")
        stage_id = str(item.get("stage_id", ""))

        # -------- EXISTING LEAD --------
        if lead_id in existing_leads:
            row_num = existing_leads[lead_id]

            if lead_status != existing_status.get(lead_id) or stage_id != existing_stage.get(lead_id):
                if STATUS_COL:
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_num, STATUS_COL),
                        "values": [[lead_status]]
                    })
                if STAGE_COL:
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_num, STAGE_COL),
                        "values": [[stage_id]]
                    })
                if LAST_UPDATED_COL:
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_num, LAST_UPDATED_COL),
                        "values": [[now_time]]
                    })

                total_updated += 1

        # -------- NEW LEAD --------
        else:
            row = []
            for h in headers:
                if h == "last_updated":
                    row.append(now_time)
                else:
                    v = item.get(h, "")
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    row.append(v)

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
print("🔁 Updated Leads:", total_updated)
