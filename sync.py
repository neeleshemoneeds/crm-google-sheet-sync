import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIG =================
SHEET_TAB = "Leads"

# ================ SECRETS =================
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

# =========== HEADERS (ONLY A–K) ===========
headers = sheet.row_values(1)

if not headers:
    headers = [
        "lead_id",
        "lead_name",
        "lead_phone",
        "lead_email",
        "lead_source",
        "lead_stage",
        "treatment",
        "lead_created_at",
        "nextcallback_at",
        "comments",
        "last_updated"
    ]
    sheet.append_row(headers)

TOTAL_COLS = len(headers)  # 11
END_COL = chr(ord('A') + TOTAL_COLS - 1)  # K

# =========== EXISTING DATA =================
existing_rows = sheet.get_all_records(expected_headers=headers)
existing_map = {}

for idx, row in enumerate(existing_rows, start=2):
    lid = str(row.get("lead_id", "")).strip()
    if lid:
        existing_map[lid] = idx

print("✅ Google Sheet Connected Successfully")
print("📄 Sheet Name:", SHEET_TAB)
print("📊 Total Existing Records:", len(existing_rows))
