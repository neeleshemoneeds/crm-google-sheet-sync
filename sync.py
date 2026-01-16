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

MANUAL_START_DATE = "2026-01-01"

# ⚠️ REQUIRED BY CRM (DO NOT REMOVE)
STAGE_ID = "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"

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

print("🚀 CRM → Google Sheet Sync Started")

# ================= DATE RANGE =================
lead_date_after = MANUAL_START_DATE
lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ================= READ HEADERS =================
headers = sheet.row_values(1)
header_index = {h: i for i, h in enumerate(headers)}

LEAD_ID_COL = header_index.get("lead_id")

existing_rows = sheet.get_all_values()[1:]
existing_map = {}

for i, row in enumerate(existing_rows, start=2):
    if LEAD_ID_COL is not None and len(row) > LEAD_ID_COL:
        lid = row[LEAD_ID_COL]
        if lid:
            existing_map[lid] = i

crm_ids = set()
page = 0
total_new = 0
total_updated = 0

# ================= MAIN LOOP =================
while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "stage_id": STAGE_ID,          # 🔥 REQUIRED
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "lead_date_after": lead_date_after,
        "lead_date_before": lead_date_before
    }

    res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()

    data = res.json().get("lead_data", [])
    if not data:
        break

    new_rows = []
    updates = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in data:
        lead_id = str(item.get("lead_id") or item.get("id"))
        if not lead_id:
            continue

        crm_ids.add(lead_id)

        if lead_id in existing_map:
            total_updated += 1
            continue

        row = []
        for h in headers:
            if h == "last_updated":
                row.append(now)
            else:
                v = item.get(h, "")
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                row.append(v)

        new_rows.append(row)
        total_new += 1

    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")

    page += 1
    time.sleep(1)

# ================= REMOVE DELETED CRM LEADS =================
to_delete = []
for lid, row_num in existing_map.items():
    if lid not in crm_ids:
        to_delete.append(row_num)

for r in sorted(to_delete, reverse=True):
    sheet.delete_rows(r)

print("🎉 SYNC COMPLETE")
print("🆕 New Leads:", total_new)
print("🔁 Updated Leads:", total_updated)
print("🧹 Deleted Leads Removed:", len(to_delete))
