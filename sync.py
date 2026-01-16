import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

REQUEST_TIMEOUT = 60
PAGE_LIMIT = 200
MAX_PAGES = 50

# ================= SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# ================= HEADERS =================
HEADERS = [
    "Lead ID",
    "Name",
    "Phone",
    "Email",
    "Source",
    "Stage",
    "Treatment",
    "Created Date",
    "Updated Date",
    "Next Callback",
    "Comments",
    "Status Log",
    "Last Sync Time"
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

total_new = 0
total_updated = 0
page = 0

# ============ SAFE CONVERTER ==============
def safe_value(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

# ============ MAIN LOOP ===================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,
        "stage_id": "15"   # confirmed working stage
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    if not data:
        break

    for item in data:
        lead_id = safe_value(item.get("lead_id"))
        if not lead_id:
            continue

        row_data = [
            lead_id,
            safe_value(item.get("lead_name")),
            safe_value(item.get("lead_phone")),
            safe_value(item.get("lead_email")),
            safe_value(item.get("lead_source")),
            safe_value(item.get("lead_stage")),
            safe_value(item.get("treatment")),
            safe_value(item.get("lead_created_at")),
            safe_value(item.get("lead_updated_at")),
            safe_value(item.get("nextcallback_at")),
            safe_value(item.get("comments")),     # ✅ LIST → STRING
            safe_value(item.get("statuslog")),    # ✅ LIST → STRING
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

        if lead_id in lead_row_map:
            row_no = lead_row_map[lead_id]
            sheet.update(f"A{row_no}:M{row_no}", [row_data])
            total_updated += 1
        else:
            sheet.append_row(row_data)
            lead_row_map[lead_id] = sheet.row_count
            total_new += 1

    page += 1
    time.sleep(1)

print("✅ SYNC COMPLETE")
print(f"🆕 New leads added: {total_new}")
print(f"🔁 Leads updated: {total_updated}")
