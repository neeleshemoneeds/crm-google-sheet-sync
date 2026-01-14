import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"
REQUEST_TIMEOUT = 25
PAGE_LIMIT = 200
MAX_PAGES = 50   # safety stop

CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

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

lead_date_after = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")


seen_ids = set()
headers_written = False
total = 0
page = 0

while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_limit": PAGE_LIMIT,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json().get("lead_data", [])
    print("API returned leads:", len(data))


    if not data:
        break

    new_rows = []

    for item in data:
        lead_id = item.get("lead_id") or item.get("id")
        if not lead_id:
            continue

        if lead_id in seen_ids:
            continue

        seen_ids.add(lead_id)

        if not headers_written:
            headers = list(item.keys())
            sheet.append_row(headers)
            headers_written = True

        row = []
        for h in headers:
            v = item.get(h, "")
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            row.append(v)

        new_rows.append(row)

    if not new_rows:
        print("No new leads found. Stopping.")
        break

    sheet.append_rows(new_rows, value_input_option="RAW")
    total += len(new_rows)

    print(f"Page {page+1}: added {len(new_rows)} leads (total {total})")

    page += 1
    time.sleep(1)

print(f"✅ DONE. Unique leads synced: {total}")
