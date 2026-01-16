import os
import json
import requests
from pprint import pprint

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"

CRM_API_TOKEN = os.environ["CRM_API_TOKEN"]

payload = {
    "token": CRM_API_TOKEN,
    "lead_limit": 1,
    "lead_offset": 0
}

print("🔍 Fetching sample lead...")

response = requests.post(API_URL, data=payload, timeout=30)
response.raise_for_status()

data = response.json()

print("\n📦 FULL API RESPONSE:\n")
pprint(data)

lead_data = data.get("lead_data", [])

if not lead_data:
    print("❌ No lead_data found")
    exit()

print("\n🧩 SAMPLE LEAD KE KEYS:\n")

sample = lead_data[0]

for k, v in sample.items():
    print(f"{k:25} -> {type(v).__name__}")
