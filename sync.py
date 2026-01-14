import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# ===============================
# ENV VARIABLES (GitHub Secrets)
# ===============================
SHEET_ID = os.environ.get("SHEET_ID")
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
CRM_API_TOKEN = os.environ.get("CRM_API_TOKEN")

if not SHEET_ID or not SERVICE_ACCOUNT_JSON or not CRM_API_TOKEN:
    raise Exception("❌ Missing GitHub Secrets")

# ===============================
# GOOGLE SHEETS AUTH
# ===============================
service_account_info = json.loads(SERVICE_ACCOUNT_JSON)

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    service_account_info, scopes=scopes
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_key(SHEET_ID)
sheet = spreadsheet.worksheet("Leads")

# ===============================
# CLEAR OLD DATA (keep header)
# ===============================
sheet.clear()

# ===============================
# CRM API CONFIG
# ===============================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"

START_DATE = "2024-01-01"   # aap chaaho to change kar sakte ho
OFFSET = 0
ALL_ROWS = []
HEADERS = None
TOTAL_FETCHED = 0

# ===============================
# FETCH DATA WITH PAGINATION
# ===============================
while True:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": START_DATE,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133",
        "lead_offset": OFFSET
    }

    response = requests.post(API_URL, data=payload, timeout=60)
    data = response.json()

    leads = data.get("lead_data", [])

    if not leads:
        break

    if HEADERS is None:
        HEADERS = [k for k in leads[0].keys()
                   if k not in ("comments", "statuslog")]
        ALL_ROWS.append(HEADERS)

    for item in leads:
        row = []
        for h in HEADERS:
            v = item.get(h, "")
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            row.append(v)
        ALL_ROWS.append(row)

    OFFSET += len(leads)
    TOTAL_FETCHED += len(leads)

# ===============================
# WRITE TO SHEET
# ===============================
if ALL_ROWS:
    sheet.update("A1", ALL_ROWS)

print(f"✅ DONE — Total leads fetched: {TOTAL_FETCHED}")
