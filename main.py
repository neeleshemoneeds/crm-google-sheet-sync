import os
import json
import gspread
from google.oauth2.service_account import Credentials

print("Script started")

# Secrets se data lena
service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
sheet_id = os.environ["SHEET_ID"]

# Google auth scopes
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Credentials
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=scope
)

client = gspread.authorize(creds)

# Sheet open karo
sheet = client.open_by_key(sheet_id).worksheet("Leads")


# Test data likho
sheet.clear()
sheet.append_row(["Status", "Message"])
sheet.append_row(["OK", "GitHub successfully connected"])

print("Sheet updated successfully")

