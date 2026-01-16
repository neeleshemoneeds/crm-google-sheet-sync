import os
import requests
import json

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
TOKEN = os.environ["CRM_API_TOKEN"]

payload = {
    "token": TOKEN,
    "lead_limit": 5,
    "lead_offset": 0
}

res = requests.post(API_URL, data=payload, timeout=30)

print("STATUS CODE:", res.status_code)
print("RAW RESPONSE ↓↓↓")
print(json.dumps(res.json(), indent=2))
