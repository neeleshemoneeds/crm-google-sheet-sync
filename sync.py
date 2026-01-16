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
REQUEST_TIMEOUT = 60

START_DATE = "2026-01-01"

STAGE_IDS = [
    "1","2","15","18","19","20","21","22","24","25","29","30","32","33","34",
    "35","36","37","38","39","40","41","42","43","44","46","47","48","49","50"
]

# ================= SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# ================= SHEET AUTH =================
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

# ================= HEADERS =================
headers = sheet.row_values(1)
if not headers:
    raise Exception("❌ Sheet me headers nahi hain")

lead_id_col = headers.index("lead_id")

existing_rows = sheet.get_all_values()[1:]
existing_map = {}
for i, row in enumerate(existing_rows, start=2):
    if len(row) > lead_id_col and row[lead_id_col]:
        existing_map[row[lead_id_col]] = i

crm_ids = set()
new_rows = []

# ================= DATE RANGE =================
lead_date_after = START_DATE
lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ================= MAIN FETCH =================
for stage_id in STAGE_IDS:
    print(f"📥 Fetching stage_id = {stage_id}")
    page = 0

    while page < MAX_PAGES:
        payload = {
            "token": CRM_API_TOKEN,
            "stage_id": stage_id,
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

        for item in data:
            lead_id = str(item.get("lead_id") or item.get("id"))
            if not lead_id:
                continue

            crm_ids.add(lead_id)

            if lead_id in existing_map:
                continue  # already exists

            row = []
            for h in headers:
                v = item.get(h, "")
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                row.append(v)

            new_rows.append(row)

        page += 1
        time.sleep(0.5)

# ================= APPEND NEW =================
if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

# ================= REMOVE DELETED =================
delete_rows = []
for lid, row_num in existing_map.items():
    if lid not in crm_ids:
        delete_rows.append(row_num)

for r in sorted(delete_rows, reverse=True):
    sheet.delete_rows(r)

print("🎉 SYNC COMPLETE")
print("🆕 New Leads Added:", len(new_rows))
print("🧹 Deleted Leads Removed:", len(delete_rows))
