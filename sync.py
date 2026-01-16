import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

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

print("🚀 Sync started")
print("From:", lead_date_after, "To:", lead_date_before)

# ============ READ EXISTING SHEET ============
headers = sheet.row_values(1)
header_index = {h: i + 1 for i, h in enumerate(headers)}

LEAD_ID_COL = header_index["lead_id"]
STATUS_COL = header_index["lead_status"]
STAGE_COL = header_index["stage_id"]
LAST_UPDATED_COL = header_index["last_updated"]

existing_rows = sheet.get_all_values()[1:]
lead_row_map = {}

for i, row in enumerate(existing_rows, start=2):
    if len(row) >= LEAD_ID_COL and row[LEAD_ID_COL - 1]:
        lead_row_map[row[LEAD_ID_COL - 1]] = i

total_new = 0
total_updated = 0
page = 0

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

    # -------- API CALL WITH RETRY --------
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            print(f"⏳ Timeout page {page+1}, retry {attempt}")
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

        # ---------- EXISTING LEAD ----------
        if lead_id in lead_row_map:
            r = lead_row_map[lead_id]

            updates.append({
                "range": gspread.utils.rowcol_to_a1(r, STATUS_COL),
                "values": [[lead_status]]
            })
            updates.append({
                "range": gspread.utils.rowcol_to_a1(r, STAGE_COL),
                "values": [[stage_id]]
            })
            updates.append({
                "range": gspread.utils.rowcol_to_a1(r, LAST_UPDATED_COL),
                "values": [[now_time]]
            })

            total_updated += 1

        # ---------- NEW LEAD ----------
        else:
            row = [""] * len(headers)
            for k, v in item.items():
                if k in header_index:
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    row[header_index[k] - 1] = v

            row[LAST_UPDATED_COL - 1] = now_time
            new_rows.append(row)
            total_new += 1

    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")

    if updates:
        sheet.batch_update(updates)

    page += 1
    time.sleep(1)

print("🎉 DONE")
print("🆕 New leads added:", total_new)
print("🔁 Leads updated (status + stage):", total_updated)
