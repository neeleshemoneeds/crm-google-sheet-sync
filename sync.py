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

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 200
MAX_PAGES = 50

# ================ MANUAL START DATE =================
# 🔴 YAHAN APNI DATE DAALE (YYYY-MM-DD)
START_DATE = "2025-12-16"

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

print("🚀 Smart Sync started")

# ================= DATE RANGE =================
lead_date_after = START_DATE
lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("From:", lead_date_after)
print("To  :", lead_date_before)

# ================= HEADERS =================
HEADERS = [
    "Lead ID",
    "Assigned Date",
    "Assigned To",
    "Date",
    "City",
    "Phone",
    "Name",
    "Treatment",
    "Update Date",
    "Source",
    "Stage",
    "Keyword",
    "Last Comment",
    "Next Call-back Date",
    "Last Sync Time"
]

# ================= READ EXISTING DATA =================
existing = sheet.get_all_values()
lead_map = {}   # lead_id -> (row_index, update_date)

if existing:
    header = existing[0]
    id_idx = header.index("Lead ID")
    upd_idx = header.index("Update Date")

    for i, row in enumerate(existing[1:], start=2):
        if len(row) > upd_idx:
            lead_map[row[id_idx]] = (i, row[upd_idx])
else:
    sheet.append_row(HEADERS)

sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
page = 0
total_updated = 0
total_new = 0

# ================= MAIN LOOP =================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_date_before": lead_date_before,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset
    }

    print(f"➡️ Page {page+1}")

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    print("API returned:", len(data))

    if not data:
        break

    new_rows = []

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id"))
        update_date = item.get("updated_at", "")

        row_data = [
            lead_id,
            item.get("assigned_date", ""),
            item.get("assigned_to", ""),
            item.get("lead_date", ""),
            item.get("city", ""),
            item.get("phone", ""),
            item.get("name", ""),
            item.get("treatment", ""),
            update_date,
            item.get("source", ""),
            item.get("stage", ""),
            item.get("keyword", ""),
            item.get("last_comment", ""),
            item.get("next_callback_date", ""),
            sync_time
        ]

        # 🧠 SMART LOGIC
        if lead_id in lead_map:
            row_num, old_update = lead_map[lead_id]
            if old_update != update_date:
                sheet.update(f"A{row_num}:O{row_num}", [row_data])
                total_updated += 1
        else:
            new_rows.append(row_data)
            total_new += 1

    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")

    page += 1
    time.sleep(1)

print("🎉 DONE")
print("🆕 New leads added:", total_new)
print("♻️ Leads updated   :", total_updated)
print("🕒 Last Sync Time :", sync_time)
