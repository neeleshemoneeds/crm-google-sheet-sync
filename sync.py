import os
import json
import time
import requests
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

REQUEST_TIMEOUT = 90
PAGE_LIMIT = 200
MAX_PAGES = 50

LEAD_DATE_AFTER = "2025-11-01"

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

# =========== HEADERS (ONLY A–K) ===========
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

TOTAL_COLS = len(headers)           # 11
END_COL = chr(ord('A') + TOTAL_COLS - 1)  # K

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

# =========== MAIN LOOP =====================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,
        "lead_date_after": LEAD_DATE_AFTER,
        "stage_id": ",".join(str(i) for i in range(1, 134))
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    if not data:
        break

    now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dedup = {}
    for item in data:
        lid = str(item.get("lead_id") or item.get("id", "")).strip()
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

        # 🔒 ONLY A–K (Column L untouched forever)
        row_data = row_data[:TOTAL_COLS]

        # 🔁 UPDATE (A–K only)
        if lead_id in existing_map:
            row_num = existing_map[lead_id]
            updates.append({
                "range": f"A{row_num}:{END_COL}{row_num}",
                "values": [row_data]
            })
            update_count += 1

        # 🆕 INSERT (A–K only)
        else:
            new_rows.append(row_data)

    # ========= SAFE APPEND =========
    if new_rows:
        sheet.add_rows(len(new_rows) + 5)
        last_row = len(sheet.get_all_values())
        sheet.append_rows(new_rows, value_input_option="RAW")

        for i, r in enumerate(new_rows):
            existing_map[str(r[0])] = last_row + 1 + i
            new_count += 1

    # ========= SAFE UPDATE =========
    if updates:
        sheet.batch_update(updates)

    page += 1
    time.sleep(1)

print("✅ SYNC COMPLETED")
print("🆕 New Leads:", new_count)
print("♻️ Updated Leads:", update_count)
