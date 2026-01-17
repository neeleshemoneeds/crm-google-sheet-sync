import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

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

# =========== ENSURE HEADERS ===============
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

existing_headers = sheet.row_values(1)
if existing_headers != HEADERS:
    sheet.clear()
    sheet.append_row(HEADERS)

# =========== READ EXISTING DATA ===========
existing_rows = sheet.get_all_values()[1:]  # skip header

sheet_data = {}
for idx, row in enumerate(existing_rows, start=2):
    if len(row) > 0 and row[0]:
        sheet_data[row[0]] = idx  # lead_id -> row number

# =========== FETCH CRM DATA ===============
print("🚀 CRM → Google Sheet Sync Started")

headers = {
    "Authorization": f"Bearer {CRM_API_TOKEN}",
    "Content-Type": "application/json"
}

crm_leads = {}
page = 1

while page <= MAX_PAGES:
    payload = {
        "page": page,
        "limit": PAGE_LIMIT
    }

    r = requests.post(API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = r.json()
    leads = data.get("data", [])

    if not leads:
        break

    for lead in leads:
        lead_id = str(lead.get("id"))
        crm_leads[lead_id] = [
            lead_id,
            lead.get("name", ""),
            lead.get("phone", ""),
            lead.get("email", ""),
            lead.get("source", ""),
            lead.get("stage", ""),
            lead.get("treatment", ""),
            lead.get("created_at", ""),
            lead.get("next_callback", ""),
            lead.get("comments", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

    page += 1

print(f"📦 CRM Active Leads: {len(crm_leads)}")

# =========== APPEND / UPDATE ==============
new_count = 0
update_count = 0

for lead_id, row_data in crm_leads.items():
    if lead_id in sheet_data:
        sheet.update(f"A{sheet_data[lead_id]}:K{sheet_data[lead_id]}", [row_data])
        update_count += 1
    else:
        sheet.append_row(row_data)
        new_count += 1

# =========== DELETE REMOVED CRM LEADS ======
deleted = 0
sheet_lead_ids = set(sheet_data.keys())
crm_lead_ids = set(crm_leads.keys())

to_delete = sorted(sheet_lead_ids - crm_lead_ids, reverse=True)

for lead_id in to_delete:
    sheet.delete_rows(sheet_data[lead_id])
    deleted += 1

# =========== SUMMARY ======================
print("✅ SYNC COMPLETE")
print(f"🆕 New Leads Added: {new_count}")
print(f"♻️ Leads Updated: {update_count}")
print(f"🗑️ Deleted Leads Removed: {deleted}")
