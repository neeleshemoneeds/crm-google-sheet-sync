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
MAX_PAGES = 100
REQUEST_TIMEOUT = 90

START_DATE = "2026-01-01"

STAGE_ID = "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100"

# ================= SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# ================= GOOGLE SHEET =================
creds = Credentials.from_service_account_info(
    SERVICE_ACCOUNT_JSON,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

print("🚀 CRM → Google Sheet Sync Started")

# ================= HEADER MAP =================
HEADERS = sheet.row_values(1)

FIELD_MAP = {
    "lead_id": ["lead_id", "id"],
    "lead_name": ["name"],
    "lead_phone": ["phone"],
    "lead_email": ["email"],
    "lead_source": ["source"],
    "lead_stage": ["stage_name", "stage_id"],
    "treatment": ["treatment"],
    "lead_created_at": ["lead_date", "created_at"],
    "nextcallback_at": ["next_callback_date"],
    "comments": ["last_comment"],
    "last_updated": ["updated_at"]
}

existing_ids = set()
rows = sheet.get_all_values()[1:]
lead_id_index = HEADERS.index("lead_id")

for r in rows:
    if len(r) > lead_id_index and r[lead_id_index]:
        existing_ids.add(r[lead_id_index])

new_rows = []

page = 0
while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "stage_id": STAGE_ID,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "lead_date_after": START_DATE,
        "lead_date_before": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()

    data = res.json().get("lead_data", [])
    if not data:
        break

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id"))
        if not lead_id or lead_id in existing_ids:
            continue

        row = []
        for h in HEADERS:
            val = ""
            for api_key in FIELD_MAP.get(h, []):
                if api_key in item and item[api_key] not in (None, ""):
                    val = item[api_key]
                    break

            if isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)

            row.append(val)

        new_rows.append(row)

    page += 1
    time.sleep(0.5)

if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

print("🎉 SYNC COMPLETE")
print("🆕 New Leads Added:", len(new_rows))
