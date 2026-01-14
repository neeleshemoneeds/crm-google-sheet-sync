# ===============================
# CRM → Google Sheet Auto Sync
# ===============================

import os
import json
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===============================
# ENV VARIABLES (GitHub Secrets)
# ===============================

CRM_API_TOKEN = os.environ.get("CRM_API_TOKEN")
SHEET_ID = os.environ.get("SHEET_ID")
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")

if not CRM_API_TOKEN:
    raise Exception("❌ CRM_API_TOKEN missing")

if not SHEET_ID:
    raise Exception("❌ SHEET_ID missing")

if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ SERVICE_ACCOUNT_JSON missing")

# ===============================
# GOOGLE SHEET CONNECT
# ===============================

creds_dict = json.loads(SERVICE_ACCOUNT_JSON)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

spreadsheet = client.open_by_key(SHEET_ID)

try:
    sheet = spreadsheet.worksheet("Leads")
except:
    sheet = spreadsheet.add_worksheet(title="Leads", rows="1000", cols="50")

sheet.clear()

# ===============================
# CRM API SETUP
# ===============================

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"

STAGE_IDS = (
    "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,"
    "43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,"
    "66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,"
    "88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,"
    "107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,"
    "123,124,125,126,127,128,129,130,131,132,133"
)

# ===============================
# REQUEST SESSION (RETRY + TIMEOUT)
# ===============================

session = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["POST"]
)

session.mount("https://", HTTPAdapter(max_retries=retries))

# ===============================
# FETCH DATA WITH PAGINATION
# ===============================

offset = 0
limit = 200
headers_written = False
total_rows = 0

while True:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": "2025-01-01",   # ⬅️ जरूरत हो तो बदलेंगे
        "stage_id": STAGE_IDS,
        "lead_offset": offset,
        "limit": limit
    }

    print(f"➡️ Fetching leads offset={offset}")

    response = session.post(API_URL, data=payload, timeout=180)

    data = response.json()

    leads = data.get("lead_data", [])

    if not leads:
        print("✅ No more leads found")
        break

    # Write headers once
    if not headers_written:
        headers = [
            k for k in leads[0].keys()
            if k not in ("comments", "statuslog")
        ]
        sheet.append_row(headers)
        headers_written = True

    rows = []
    for lead in leads:
        row = []
        for h in headers:
            val = lead.get(h, "")
            if isinstance(val, (dict, list)):
                val = json.dumps(val)
            row.append(val)
        rows.append(row)

    sheet.append_rows(rows)

    fetched = len(rows)
    total_rows += fetched
    offset += fetched

    print(f"✅ Fetched {fetched} leads (Total: {total_rows})")

    time.sleep(2)  # API ko rest

print("🎉 SYNC COMPLETED SUCCESSFULLY")
