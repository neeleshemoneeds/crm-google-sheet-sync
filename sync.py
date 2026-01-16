import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# ================= CONFIG =================
API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
SHEET_TAB = "Leads"

PAGE_LIMIT = 200
MAX_PAGES = 50
REQUEST_TIMEOUT = 60

# 🔥 START DATE (MANUAL)
START_DATE = "2024-01-01"   # YYYY-MM-DD
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")

# ================= STAGES =================
STAGE_IDS = [
    1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,
    40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,
    59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,
    77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,
    94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,
    109,110,111,112,113,114,115,116,117,118,119,120,121,
    122,123,124,125,126,127,128,129,130,131,132,133
]

# ================= SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

# ================= HEADERS =================
BASE_HEADERS = [
    "Lead ID", "Name", "Phone", "Email", "Source",
    "Treatment", "Created Date", "Updated Date",
    "Next Callback", "Comments", "Status Log", "Last Sync Time"
]

STAGE_HEADERS = [f"Stage_{sid}" for sid in STAGE_IDS]

HEADERS = BASE_HEADERS + STAGE_HEADERS

# =========== GOOGLE AUTH ============
creds = Credentials.from_service_account_info(
    SERVICE_ACCOUNT_JSON,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

# =========== INIT SHEET ============
existing = sheet.get_all_values()

if not existing:
    sheet.append_row(HEADERS)
    lead_row_map = {}
else:
    lead_row_map = {
        row[0]: idx + 2
        for idx, row in enumerate(existing[1:])
        if row and row[0]
    }

print("🚀 Smart Sync Started")

# ================= HELPERS =================
def safe(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

new_rows = []
update_map = {}

page = 0

# ================= FETCH LOOP =================
while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "from_date": START_DATE,
        "to_date": TODAY_DATE
    }

    res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()

    leads = res.json().get("lead_data", [])
    if not leads:
        break

    for item in leads:
        lead_id = safe(item.get("lead_id"))
        if not lead_id:
            continue

        lead_stage = safe(item.get("lead_stage"))

        # 🧠 Stage column mapping
        stage_cols = []
        for sid in STAGE_IDS:
            if lead_stage == str(sid):
                stage_cols.append("YES")
            else:
                stage_cols.append("")

        row = [
            lead_id,
            safe(item.get("lead_name")),
            safe(item.get("lead_phone")),
            safe(item.get("lead_email")),
            safe(item.get("lead_source")),
            safe(item.get("treatment")),
            safe(item.get("lead_created_at")),
            safe(item.get("lead_updated_at")),
            safe(item.get("nextcallback_at")),
            safe(item.get("comments")),
            safe(item.get("statuslog")),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ] + stage_cols

        if lead_id in lead_row_map:
            update_map[lead_row_map[lead_id]] = row
        else:
            new_rows.append(row)

    page += 1
    time.sleep(1)

# ================= WRITE TO SHEET =================

# ✅ APPEND NEW LEADS (SINGLE CALL)
if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

# ✅ BULK UPDATE EXISTING LEADS (SINGLE CALL)
if update_map:
    min_row = min(update_map.keys())
    max_row = max(update_map.keys())

    bulk = []
    for r in range(min_row, max_row + 1):
        bulk.append(update_map.get(r, [""] * len(HEADERS)))

    sheet.update(
        range_name=f"A{min_row}:{chr(64+len(HEADERS))}{max_row}",
        values=bulk
    )

print("✅ SYNC COMPLETE")
print(f"🆕 New leads added: {len(new_rows)}")
print(f"🔁 Leads updated: {len(update_map)}")
