import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "CRM_Leads"

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 200
MAX_PAGES = 50

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

if sheet.row_values(1) != HEADERS:
    sheet.clear()
    sheet.append_row(HEADERS)

# =========== EXISTING DATA =================
rows = sheet.get_all_values()[1:]
sheet_map = {}

for i, r in enumerate(rows, start=2):
    if r and r[0]:
        sheet_map[r[0]] = i

print("🚀 CRM → Google Sheet Sync Started")

crm_leads = {}
page = 0

while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)

    if response.status_code == 401:
        raise Exception("❌ CRM TOKEN INVALID OR EXPIRED")

    response.raise_for_status()

    data = response.json().get("lead_data", [])
    if not data:
        break

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id"))
        if not lead_id:
            continue

        crm_leads[lead_id] = [
            lead_id,
            item.get("lead_name", ""),
            item.get("lead_phone", ""),
            item.get("lead_email", ""),
            item.get("lead_source", ""),
            item.get("lead_stage", ""),
            item.get("treatment", ""),
            item.get("lead_created_at", ""),
            item.get("nextcallback_at", ""),
            item.get("comments", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

    page += 1
    time.sleep(1)

print(f"📦 CRM Active Leads: {len(crm_leads)}")

# =========== INSERT / UPDATE ===============
new_count = 0
update_count = 0

for lead_id, row in crm_leads.items():
    if lead_id in sheet_map:
        sheet.update(f"A{sheet_map[lead_id]}:K{sheet_map[lead_id]}", [row])
        update_count += 1
    else:
        sheet.append_row(row)
        new_count += 1

# =========== DELETE REMOVED LEADS ==========
deleted = 0
to_delete = sorted(set(sheet_map) - set(crm_leads), reverse=True)

for lid in to_delete:
    sheet.delete_rows(sheet_map[lid])
    deleted += 1

print("✅ SYNC COMPLETED")
print("🆕 New:", new_count)
print("♻️ Updated:", update_count)
print("🗑️ Deleted:", deleted)
