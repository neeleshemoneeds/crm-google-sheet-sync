import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

REQUEST_TIMEOUT = 30
PAGE_LIMIT = 200
MAX_PAGES = 50   # safety stop (200 x 50 = 10,000 leads max)

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

# ⚠️ Har run me sheet fresh hogi
sheet.clear()

sheet.append_row(HEADERS)
headers_written = True

# ============ DATE FILTER =================
lead_date_after = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

# ============ CUSTOM HEADERS =================
HEADERS = [
    "Lead ID",
    "Assigned Date",
    "Assigned To",
    "Date",
    "City",
    "Phone",
    "Name",
    "Treatment",
    "Update Date",
    "Source",
    "Stage",
    "Keyword",
    "Last Comment",
    "Next Call-back Date"
]


# ============ INTERNAL STATE ==============
seen_ids = set()
headers_written = False
headers = []
total = 0
page = 0

print("🚀 Sync started")
print("Lead date after:", lead_date_after)

# ============ MAIN LOOP ===================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,   # ✅ VERY IMPORTANT (pagination)
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    print(f"➡️ Page {page+1} | offset {offset}")

    response = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json().get("lead_data", [])
    print("API returned leads:", len(data))

    if not data:
        print("❌ No more data from API. Stopping.")
        break

    new_rows = []

    for item in data:
        lead_id = item.get("lead_id") or item.get("id")
        if not lead_id:
            continue

        if lead_id in seen_ids:
            continue

        seen_ids.add(lead_id)

        

        row = [
    item.get("lead_id") or item.get("id", ""),
    item.get("assigned_date", ""),
    item.get("assigned_to", ""),
    item.get("lead_date", ""),
    item.get("city", ""),
    item.get("phone", ""),
    item.get("name", ""),
    item.get("treatment", ""),
    item.get("updated_at", ""),
    item.get("source", ""),
    item.get("stage", ""),
    item.get("keyword", ""),
    item.get("last_comment", ""),
    item.get("next_callback_date", "")
]


        new_rows.append(row)

    if not new_rows:
        print("⚠️ No new unique leads found. Stopping.")
        break

    sheet.append_rows(new_rows, value_input_option="RAW")
    total += len(new_rows)

    print(f"✅ Added {len(new_rows)} leads | Total: {total}")

    page += 1
    time.sleep(1)   # CRM safe delay

print(f"🎉 DONE. Total unique leads synced: {total}")

