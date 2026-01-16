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
REQUEST_TIMEOUT = 90
MAX_RETRIES = 3

# 🔥 MANUAL START DATE (YYYY-MM-DD)
MANUAL_START_DATE = "2026-01-01"

# ================ SECRETS =================
CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])

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

# ============ DATE RANGE =================
lead_date_after = MANUAL_START_DATE
lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("🚀 Smart Sync Started")
print("From:", lead_date_after, "To:", lead_date_before)

# ============ READ EXISTING SHEET ============
existing_rows = sheet.get_all_records()
headers = sheet.row_values(1)

lead_row_map = {}      # lead_id -> row number
lead_status_map = {}   # lead_id -> status
lead_stage_map = {}    # lead_id -> stage
sheet_lead_ids = set()

for idx, row in enumerate(existing_rows, start=2):
    lead_id = str(row.get("lead_id") or row.get("id") or "").strip()
    if lead_id:
        lead_row_map[lead_id] = idx        # 🔥 last duplicate wins
        lead_status_map[lead_id] = row.get("lead_status", "")
        lead_stage_map[lead_id] = str(row.get("stage_id", ""))
        sheet_lead_ids.add(lead_id)

header_index = {h: i + 1 for i, h in enumerate(headers)}

STATUS_COL = header_index.get("lead_status")
STAGE_COL = header_index.get("stage_id")
UPDATED_COL = header_index.get("last_updated")

# ============ HELPERS =================
def safe(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

# ============ MAIN LOOP =================
page = 0
crm_lead_ids = set()
new_rows = []
updates = []
now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

while page < MAX_PAGES:
    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_date_before": lead_date_before,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": page * PAGE_LIMIT,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
            res.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5)

    data = res.json().get("lead_data", [])
    if not data:
        break

    for item in data:
        lead_id = safe(item.get("lead_id") or item.get("id"))
        if not lead_id:
            continue

        crm_lead_ids.add(lead_id)

        lead_status = safe(item.get("lead_status"))
        stage_id = safe(item.get("stage_id"))

        # ---------- EXISTING LEAD ----------
        if lead_id in lead_row_map:
            old_status = lead_status_map.get(lead_id)
            old_stage = lead_stage_map.get(lead_id)

            if lead_status != old_status or stage_id != old_stage:
                row_num = lead_row_map[lead_id]

                if STATUS_COL:
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_num, STATUS_COL),
                        "values": [[lead_status]]
                    })
                if STAGE_COL:
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_num, STAGE_COL),
                        "values": [[stage_id]]
                    })
                if UPDATED_COL:
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_num, UPDATED_COL),
                        "values": [[now_time]]
                    })

        # ---------- NEW LEAD ----------
        else:
            row = []
            for h in headers:
                if h == "last_updated":
                    row.append(now_time)
                else:
                    row.append(safe(item.get(h)))
            new_rows.append(row)

    page += 1
    time.sleep(1)

# ============ WRITE NEW + UPDATES ============
if new_rows:
    sheet.append_rows(new_rows, value_input_option="RAW")

if updates:
    sheet.batch_update(updates)

# ============ DELETE REMOVED CRM LEADS ============
rows_to_delete = []

for lead_id, row_num in lead_row_map.items():
    if lead_id not in crm_lead_ids:
        rows_to_delete.append(row_num)

# delete bottom → top (safe)
for r in sorted(rows_to_delete, reverse=True):
    sheet.delete_rows(r)

print("🎉 SYNC COMPLETE")
print("🆕 New Leads:", len(new_rows))
print("🔁 Updated Leads:", len(updates))
print("🧹 Deleted Leads:", len(rows_to_delete))
print("🕒 Last Sync:", now_time)
