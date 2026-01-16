import os
import requests
from pprint import pprint

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"

CRM_API_TOKEN = os.environ.get("CRM_API_TOKEN")

if not CRM_API_TOKEN:
    raise Exception("❌ CRM_API_TOKEN env missing")

# ✅ Headers (IMPORTANT)
headers = {
    "Authorization": f"Bearer {CRM_API_TOKEN}",
    "Accept": "application/json"
}

payload = {
    "lead_limit": 1,
    "lead_offset": 0
}

print("🔍 Fetching sample lead with HEADER auth...")

response = requests.post(
    API_URL,
    headers=headers,
    data=payload,
    timeout=60
)

print("Status code:", response.status_code)

if response.status_code != 200:
    print("❌ RAW RESPONSE:")
    print(response.text)
    raise Exception("API failed")

data = response.json()

print("\n📦 FULL API RESPONSE:\n")
pprint(data)

lead_data = data.get("lead_data", [])

if not lead_data:
    print("❌ No lead_data found")
    exit()

print("\n🧩 SAMPLE LEAD KE ACTUAL KEYS:\n")

sample = lead_data[0]

for k, v in sample.items():
    print(f"{k:25} -> {type(v).__name__}")
