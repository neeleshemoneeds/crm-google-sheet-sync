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

REQUEST_TIMEOUT = 60
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

print("🚀 Sync started")

headers = sheet.row_values(1)
existing_ids = set()

existing_rows = sheet.get_all_values()[1:]
for row in existing_rows:
    if row and row[0]:
        existing_ids.add(str(row[0]))

page = 0
new_rows = 0

# ================= MAIN LOOP =================
while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    print(f"➡️ Page {page+1} | Leads: {len(data)}")

    if not data:
        break

    rows_to_add = []

    for item in data:
        lead_id = str(item.get("lead_id", "")).strip()
        if not lead_id or lead_id in existing_ids:
            continue

        row = []
        for h in headers:
            val = item.get(h, "")
            if isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)
            row.append(val)

        rows_to_add.append(row)
        existing_ids.add(lead_id)
        new_rows += 1

    if rows_to_add:
        sheet.append_rows(rows_to_add, value_input_option="RAW")

    page += 1
    time.sleep(1)

print("🎉 DONE")
print("🆕 New rows added:", new_rows)
