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

PAGE_LIMIT = 200
MAX_PAGES = 50
REQUEST_TIMEOUT = 60

# 🔥 START DATE (LOCAL FILTER)
START_DATE = datetime.strptime("2024-01-01", "%Y-%m-%d")
CURRENT_YEAR = datetime.now().year

# ================= SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# ================= HEADERS =================
HEADERS = [
    "Lead ID", "Name", "Phone", "Email", "Source", "Stage",
    "Treatment", "Created Date", "Updated Date", "Next Callback",
    "Comments", "Status Log", "Last Sync Time"
]

# =========== GOOGLE AUTH ============
creds = Credentials.from_service_account_info(
    SERVICE_ACCOUNT_JSON,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

# =========== INIT SHEET ============
existing = sheet.get_all_values()

if not existing:
    sheet.append_row(HEADERS)
    lead_row_map = {}
else:
    lead_row_map = {
        row[0]: idx + 2
        for idx, row in enumerate(existing[1:])
        if row and row[0]
    }

print("🚀 Smart Sync Started")

def safe(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

new_rows = []
update_map = {}

page = 0

# ================= MAIN LOOP =================
while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "stage_id": "15"
    }

    res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()

    leads = res.json().get("lead_data", [])
    if not leads:
        break

    for item in leads:
        lead_id = safe(item.get("lead_id"))
        if not lead_id:
            continue

        created_raw = item.get("lead_created_at")
        if not created_raw:
            continue

        try:
            created_dt = datetime.strptime(created_raw[:10], "%Y-%m-%d")
        except:
            continue

        # ❌ HARD BLOCK FUTURE YEAR (CRM BUG FIX)
        if created_dt.year > CURRENT_YEAR:
            continue

        # ❌ START DATE FILTER
        if created_dt < START_DATE:
            continue

        row = [
            lead_id,
            safe(item.get("lead_name")),
            safe(item.get("lead_phone")),
            safe(item.get("lead_email")),
            safe(item.get("lead_source")),
            safe(item.get("lead_stage")),
            safe(item.get("treatment")),
            safe(item.get("lead_created_at")),
            safe(item.get("lead_updated_at")),
            safe(item.get("nextcallback_at")),
            safe(item.get("comments")),
            safe(item.get("statuslog")),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

        if lead_id in lead_row_map:
            update_map[lead_row_map[lead_id]] = row
        else:
            new_rows.append(row)

    page += 1
    time.sleep(1)

# ================= WRITE TO SHEET =================

# ✅ APPEND NEW LEADS
if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

# ✅ BULK UPDATE EXISTING LEADS
if update_map:
    min_row = min(update_map.keys())
    max_row = max(update_map.keys())

    bulk_data = []
    for r in range(min_row, max_row + 1):
        bulk_data.append(update_map.get(r, [""] * len(HEADERS)))

    sheet.update(
        range_name=f"A{min_row}:M{max_row}",
        values=bulk_data
    )

print("✅ SYNC COMPLETE")
print(f"🆕 New leads added: {len(new_rows)}")
print(f"🔁 Leads updated: {len(update_map)}")
