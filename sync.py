import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
import time

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "CRM_Leads"

PAGE_LIMIT = 200
MAX_PAGES = 100
REQUEST_TIMEOUT = 90

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

print("🗑️ Deleted CRM Leads Cleanup Started")

# ================= STEP 1: FETCH ALL CRM LEAD IDS =================
crm_lead_ids = set()
page = 0

while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()

    data = res.json().get("lead_data", [])
    if not data:
        break

    for lead in data:
        lid = lead.get("lead_id") or lead.get("id")
        if lid:
            crm_lead_ids.add(str(lid))

    page += 1
    time.sleep(1)

print(f"📦 CRM Active Leads: {len(crm_lead_ids)}")

# ================= STEP 2: READ SHEET LEAD IDS =================
sheet_rows = sheet.get_all_values()

if not sheet_rows or len(sheet_rows) < 2:
    print("⚠️ Sheet empty, nothing to delete")
    exit()

header = sheet_rows[0]
lead_id_col = header.index("lead_id")

rows_to_delete = []

for idx, row in enumerate(sheet_rows[1:], start=2):
    if len(row) <= lead_id_col:
        continue

    sheet_lead_id = row[lead_id_col].strip()
    if sheet_lead_id and sheet_lead_id not in crm_lead_ids:
        rows_to_delete.append(idx)

# ================= STEP 3: DELETE ROWS =================
for row_num in reversed(rows_to_delete):
    sheet.delete_rows(row_num)

print("🧹 Deleted Leads Removed:", len(rows_to_delete))
print("✅ Cleanup Completed Successfully")
