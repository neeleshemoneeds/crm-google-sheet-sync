import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

REQUEST_TIMEOUT = 90
PAGE_LIMIT = 200
MAX_PAGES = 50

START_DATE = datetime.strptime("2026-01-01", "%Y-%m-%d")

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

headers = sheet.row_values(1)
header_index = {h: i for i, h in enumerate(headers)}

print("🚀 Sync started")

existing = sheet.get_all_values()[1:]
existing_map = {}
for i, row in enumerate(existing, start=2):
    if row and row[0]:
        existing_map[row[0]] = i

page = 0
new_rows = []
crm_ids = set()

while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()

    leads = res.json().get("lead_data", [])
    if not leads:
        break

    for item in leads:
        created = item.get("lead_created_at", "")
        if not created:
            continue

        created_dt = datetime.strptime(created[:10], "%Y-%m-%d")
        if created_dt < START_DATE:
            continue

        lead_id = str(item.get("lead_id"))
        crm_ids.add(lead_id)

        row = []
        for h in headers:
            v = item.get(h, "")
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            row.append(v)

        if lead_id not in existing_map:
            new_rows.append(row)

    page += 1
    time.sleep(1)

if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

print("✅ DONE")
print("🆕 Rows added:", len(new_rows))
