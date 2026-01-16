import os
import requests
from pprint import pprint

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"

TOKEN = os.environ.get("CRM_API_TOKEN")

if not TOKEN:
    raise Exception("❌ CRM_API_TOKEN missing")

payload = {
    "token": TOKEN,
    "limit": 1,
    "offset": 0
}

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

print("🔍 Fetching sample lead (FINAL FORMAT)...")

response = requests.post(
    API_URL,
    data=payload,
    headers=headers,
    timeout=60
)

print("Status:", response.status_code)
print("Raw response:\n", response.text)

response.raise_for_status()

data = response.json()

lead_data = data.get("lead_data")

if not lead_data:
    raise Exception("❌ lead_data missing in response")

sample = lead_data[0]

print("\n✅ ACTUAL KEYS FROM CRM:\n")
for k in sample.keys():
    print("-", k)
