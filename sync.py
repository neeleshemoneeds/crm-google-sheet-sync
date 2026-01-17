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

# ✅ FIXED START DATE (TILL TODAY)
LEAD_DATE_AFTER = "2025-01-10"

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

# =========== HEADERS ======================
headers = sheet.row_values(1)
if not headers:
    headers = [
        "lead_id",
        "lead_name",
        "lead_phone",
        "lead_email",
        "lead_source",
        "lead_stage",
        "treatment",
        "lead_created_at",
        "nextcallback_at",
        "comments",
        "last_updated"
    ]
    sheet.append_row(headers)

# =========== EXISTING DATA =================
existing_rows = sheet.get_all_records(expected_headers=headers)
existing_map = {}

for idx, row in enumerate(existing_rows, start=2):
    lid = str(row.get("lead_id", "")).strip()
    if lid:
        existing_map[lid] = idx

print("🚀 CRM → Google Sheet Sync Started")

page = 0
new_count = 0
update_count = 0

# =========== MAIN LOOP (MANDATORY) =========
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,

        # ✅ START DATE ONLY (TILL DATE AUTO)
        "lead_date_after": LEAD_DATE_AFTER,

        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    if not data:
        break

    now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ✅ PAGE LEVEL DEDUP (latest wins)
    dedup = {}
    for item in data:
        lid = str(item.get("lead_id") or item.get("id"))
        if lid:
            dedup[lid] = item

    new_rows = []
    updates = []

    for lead_id, item in dedup.items():
        row_data = []
        for h in headers:
            if h == "last_updated":
                row_data.append(now_time)
            else:
                v = item.get(h, "")
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                row_data.append(v)

        # 🔁 UPDATE
        if lead_id in existing_map:
            row_num = existing_map[lead_id]
            if row_num <= sheet.row_count:
                updates.append({
                    "range": f"A{row_num}:K{row_num}",
                    "values": [row_data]
                })
                update_count += 1

        # 🆕 INSERT
        else:
            new_rows.append(row_data)

    # ========= SAFE APPEND =========
    if new_rows:
        sheet.add_rows(len(new_rows) + 10)
        start_row = sheet.row_count + 1
        sheet.append_rows(new_rows, value_input_option="RAW")

        for i, r in enumerate(new_rows):
            existing_map[str(r[0])] = start_row + i
            new_count += 1

    if updates:
        sheet.batch_update(updates)

    page += 1
    time.sleep(1)

print("✅ SYNC COMPLETED")
print("🆕 New Leads:", new_count)
print("♻️ Updated Leads:", update_count)
