import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from requests.exceptions import ReadTimeout, RequestException

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

REQUEST_TIMEOUT = 90   # 🔥 increased timeout
PAGE_LIMIT = 200
MAX_PAGES = 50
MAX_RETRIES = 3

# 🔴 START DATE (YAHAN CHANGE KAR SAKTE HO)
START_DATE = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

# ================= SECRETS =================
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
print("Lead date after:", START_DATE)

# ================= HEADERS =================
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

sheet.clear()
sheet.append_row(HEADERS)

seen_ids = set()
page = 0
total = 0

# ================= MAIN LOOP =================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT
    attempt = 1

    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": START_DATE,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    print(f"➡️ Page {page + 1} | offset {offset}")

    while attempt <= MAX_RETRIES:
        try:
            response = requests.post(
                API_URL,
                data=payload,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            break
        except ReadTimeout:
            print(f"⏳ Timeout (attempt {attempt}/{MAX_RETRIES})")
        except RequestException as e:
            print(f"❌ API error: {e}")

        attempt += 1
        time.sleep(5)

    if attempt > MAX_RETRIES:
        print("🚨 Page skipped due to repeated timeout")
        page += 1
        continue

    data = response.json().get("lead_data", [])
    print("API returned leads:", len(data))

    if not data:
        print("❌ No more data")
        break

    rows = []

    for item in data:
        lead_id = item.get("lead_id") or item.get("id")
        if not lead_id or lead_id in seen_ids:
            continue

        seen_ids.add(lead_id)

        row = [
                item.get("lead_id") or item.get("id", ""),          # Lead ID
                item.get("assigned_date", ""),                      # Assigned Date
                item.get("assigned_user", ""),                      # Assigned To
                item.get("lead_date", ""),                          # Date
                item.get("city_name", ""),                          # City ✅
                item.get("mobile", "") or item.get("phone", ""),    # Phone ✅
                item.get("name", ""),                               # Name
                item.get("treatment_name", ""),                     # Treatment ✅
                item.get("updated_at", ""),                         # Update Date
                item.get("source_name", ""),                        # Source ✅
                item.get("stage_name", ""),                         # Stage ✅
                item.get("keyword", ""),                            # Keyword
                item.get("last_comment", ""),                       # Last Comment
                item.get("next_callback_date", "")                  # Next Call-back Date
            ]


    if rows:
        sheet.append_rows(rows, value_input_option="RAW")
        total += len(rows)
        print(f"✅ Added {len(rows)} | Total {total}")

    page += 1
    time.sleep(2)

print(f"🎉 DONE. Total leads synced: {total}")
