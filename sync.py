import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
PAGE_LIMIT = 200        # safe batch size
REQUEST_TIMEOUT = 25    # seconds
SHEET_TAB = "Leads"

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

sheet.clear()

# ================= DATE FILTER =================
lead_date_after = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ================= FETCH LOOP =================
offset = 0
headers_written = False
total_rows = 0

while True:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_offset": offset,
        "lead_limit": PAGE_LIMIT,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    response = requests.post(
        API_URL,
        data=payload,
        timeout=REQUEST_TIMEOUT
    )

    response.raise_for_status()
    data = response.json().get("lead_data", [])

    if not data:
        break

    if not headers_written:
        headers = list(data[0].keys())
        sheet.append_row(headers)
        headers_written = True

    rows = []
    for item in data:
        row = []
        for h in headers:
            v = item.get(h, "")
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            row.append(v)
        rows.append(row)

    sheet.append_rows(rows, value_input_option="RAW")

    total_rows += len(rows)
    offset += PAGE_LIMIT

    print(f"Fetched {total_rows} leads so far...")
    time.sleep(1)

print(f"✅ DONE. Total leads synced: {total_rows}")
