import os
import requests

API_URL = "https://emoneeds.icg-crm.in/api/leads/getleads"
TOKEN = os.environ.get("CRM_API_TOKEN")

if not TOKEN:
    raise Exception("❌ CRM_API_TOKEN missing")

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

FOUND = False

for stage_id in range(1, 15):  # try stages 1–14
    print(f"\n🔍 Trying stage_id = {stage_id}")

    payload = {
        "token": TOKEN,
        "stage_id": stage_id,
        "limit": 1,
        "offset": 0
    }

    response = requests.post(API_URL, data=payload, headers=headers)
    print("Status:", response.status_code)

    if response.status_code != 200:
        continue

    data = response.json()
    leads = data.get("lead_data", [])

    if leads:
        print(f"\n✅ LEAD FOUND in stage {stage_id}\n")
        sample = leads[0]

        print("📌 ACTUAL KEYS:\n")
        for k in sample.keys():
            print("-", k)

        FOUND = True
        break
    else:
        print("❌ No leads in this stage")

if not FOUND:
    raise Exception("❌ No leads found in any stage")
