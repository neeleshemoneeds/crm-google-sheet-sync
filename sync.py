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

REQUEST_TIMEOUT = 90
PAGE_LIMIT = 200
MAX_PAGES = 50
MAX_RETRIES = 3

MANUAL_START_DATE = "2026-01-01"

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

print("🚀 Smart Sync Started")

# =====================================================
# 🧹 AUTO DUPLICATE LEAD_ID MERGER (SAFE)
# =====================================================
def merge_duplicate_leads(sheet):
    rows = sheet.get_all_values()
    if len(rows) <= 1:
        return

    headers = rows[0]
    data = rows[1:]

    if "lead_id" not in headers:
        return

    lead_id_idx = headers.index("lead_id")
    last_updated_idx = headers.index("last_updated") if "last_updated" in headers else None

    seen = {}
    delete_rows = []

    for i, row in enumerate(data, start=2):
        if len(row) <= lead_id_idx:
            continue

        lid = row[lead_id_idx]
        if not lid:
            continue

        if lid not in seen:
            seen[lid] = (row, i)
        else:
            old_row, old_i = seen[lid]

            if last_updated_idx is not None:
                old_t = old_row[last_updated_idx] if len(old_row) > last_updated_idx else ""
                new_t = row[last_updated_idx] if len(row) > last_updated_idx else ""

                if new_t > old_t:
                    delete_rows.append(old_i)
                    seen[lid] = (row, i)
                else:
                    delete_rows.append(i)
            else:
                delete_rows.append(i)

    for r in sorted(set(delete_rows), reverse=True):
        sheet.delete_rows(r)

    if delete_rows:
        print(f"🧹 Duplicate lead_id merged: {len(delete_rows)}")

merge_duplicate_leads(sheet)

# ================= DATE FILTER =================
lead_date_after = MANUAL_START_DATE
lead_date_before = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("From:", lead_date_after, "To:", lead_date_before)

# ================= READ EXISTING =================
headers = sheet.row_values(1)
header_index = {h: i + 1 for i, h in enumerate(headers)}

LEAD_ID_COL = header_index.get("lead_id")
STATUS_COL = header_index.get("lead_status")
STAGE_COL = header_index.get("stage_id")
LAST_UPDATED_COL = header_index.get("last_updated")

existing_rows = sheet.get_all_values()[1:]

existing_map = {}
for i, row in enumerate(existing_rows, start=2):
    if len(row) >= LEAD_ID_COL:
        lid = row[LEAD_ID_COL - 1]
        if lid:
            existing_map[lid] = i

existing_ids = set(existing_map.keys())
crm_ids = set()

page = 0
total_new = 0
total_updated = 0

# ================= MAIN LOOP =================
while page < MAX_PAGES:
    offset = page * PAGE_LIMIT

    payload = {
        "token": CRM_API_TOKEN,
        "lead_date_after": lead_date_after,
        "lead_date_before": lead_date_before,
        "lead_limit": PAGE_LIMIT,
        "lead_offset": offset,
        "stage_id": "1,2,15,18,19,20,21,22,24,25,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133"
    }

    for attempt in range(MAX_RETRIES):
        try:
            res = requests.post(API_URL, data=payload, timeout=REQUEST_TIMEOUT)
            res.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            time.sleep(5)

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

        lead_status = item.get("lead_status", "")
        stage_id = str(item.get("stage_id", ""))

        if lead_id in existing_map:
            row_num = existing_map[lead_id]

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
            if LAST_UPDATED_COL:
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_num, LAST_UPDATED_COL),
                    "values": [[now]]
                })

            total_updated += 1

        else:
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

    if updates:
        sheet.batch_update(updates)

    page += 1
    time.sleep(1)

# ================= REMOVE DELETED CRM LEADS =================
to_delete = []
for lid, row_num in existing_map.items():
    if lid not in crm_ids:
        to_delete.append(row_num)

for r in sorted(to_delete, reverse=True):
    sheet.delete_rows(r)

print("🎉 DONE")
print("🆕 New Leads:", total_new)
print("🔁 Updated Leads:", total_updated)
print("🧹 Deleted CRM Leads Removed:", len(to_delete))
