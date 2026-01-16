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

# 🔹 CHANGE START DATE HERE
START_DATE = "2024-01-01"   # YYYY-MM-DD

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

# =========== INIT SHEET ===================
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

# ============ SAFE VALUE ==================
def safe(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

# ============ DATE RANGE ==================
today = datetime.now().strftime("%Y-%m-%d")

# ============ MAIN LOOP ===================
new_rows = []
update_rows = []

page = 0

while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,
        "stage_id": "15",
        "from_date": START_DATE,
        "to_date": today
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    if not data:
        break

    for item in data:
        lead_id = safe(item.get("lead_id"))
        if not lead_id:
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
            update_rows.append((lead_row_map[lead_id], row))
        else:
            new_rows.append(row)

    page += 1
    time.sleep(1)

# ============ BULK WRITE ==================
# ✅ NEW LEADS (ONE CALL)
if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

# ✅ UPDATES (LIMITED CALLS)
for row_no, row_data in update_rows:
    sheet.update(
        range_name=f"A{row_no}:M{row_no}",
        values=[row_data]
    )
    time.sleep(0.3)   # quota safe

print("✅ SYNC COMPLETE")
print(f"🆕 New leads added: {len(new_rows)}")
print(f"🔁 Leads updated: {len(update_rows)}")
