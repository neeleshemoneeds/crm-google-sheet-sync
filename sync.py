import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# ------------------ CONFIG ------------------
API_URL = os.environ["CRM_API_URL"]
API_TOKEN = os.environ["CRM_API_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
SHEET_TAB = "Leads"

# ------------------ GOOGLE SHEET LOGIN ------------------
service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
client = gspread.authorize(creds)

sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

# Clear old data (except header)
sheet.clear()

# ------------------ CRM FETCH ------------------
all_leads = []
offset = 0
limit = 31  # CRM page size

while True:
    payload = {
        "token": API_TOKEN,
        "lead_offset": offset
    }

    response = requests.post(API_URL, data=payload, timeout=60)
    data = response.json().get("data", [])

    print(f"Fetched {len(data)} leads at offset {offset}")

    if not data:
        break

    all_leads.extend(data)
    offset += limit

print("Total leads fetched:", len(all_leads))

# ------------------ WRITE TO SHEET ------------------
if not all_leads:
    print("No data received from CRM")
    exit()

headers = list(all_leads[0].keys())
rows = [headers]

for lead in all_leads:
    rows.append([str(lead.get(h, "")) for h in headers])

sheet.update("A1", rows)

print("✅ Sheet updated successfully")
