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

PAGE_LIMIT = 200
MAX_PAGES = 50
REQUEST_TIMEOUT = 60

HISTORIC_START_DATE = "2026-01-01"

STAGE_IDS = "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"

# ================= SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# ================= GOOGLE AUTH =================
creds = Credentials.from_service_account_info(
    SERVICE_ACCOUNT_JSON,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

# ================= HEADERS =================
HEADERS = [
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

if not sheet.row_values(1):
    sheet.append_row(HEADERS)

# ================= EXISTING DATA =================
existing_rows = sheet.get_all_records(expected_headers=HEADERS)
existing_map = {}
last_created_at = None

for idx, row in enumerate(existing_rows, start=2):
    lid = str(row.get("lead_id", "")).strip()
    if lid:
        existing_map[lid] = idx

    dt = row.get("lead_created_at")
    if dt:
        try:
            parsed = datetime.strptime(dt[:19], "%Y-%m-%d %H:%M:%S")
            if not last_created_at or parsed > last_created_at:
                last_created_at = parsed
        except:
            pass

# ================= DATE LOGIC =================
if last_created_at:
    lead_date_after = last_created_at.strftime("%Y-%m-%d %H:%M:%S")
    print("🔁 Incremental sync from:", lead_date_after)
else:
    lead_date_after = HISTORIC_START_DATE
    print("📦 First run – full historic fetch")

lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("⏱ To:", lead_date_before)

# ================= MAIN LOOP =================
page = 0
new_count = 0
update_count = 0

while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_date_before": lead_date_before,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,
        "stage_id": STAGE_IDS
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    if not data:
        break

    now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_rows = []
    updates = []

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id"))
        if not lead_id:
            continue

        row = []
        for h in HEADERS:
            if h == "last_updated":
                row.append(now_time)
            else:
                v = item.get(h, "")
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                row.append(v)

        if lead_id in existing_map:
            row_num = existing_map[lead_id]
            updates.append({
                "range": f"A{row_num}:K{row_num}",
                "values": [row]
            })
            update_count += 1
        else:
            new_rows.append(row)
            new_count += 1

    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")

    if updates:
        sheet.batch_update(updates)

    page += 1
    time.sleep(1)

print("✅ SYNC COMPLETED")
print("🆕 New Leads:", new_count)
print("♻️ Updated Leads:", update_count)
