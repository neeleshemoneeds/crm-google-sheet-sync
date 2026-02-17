import os
import json
import psycopg2
import pandas as pd
import gspread
import numpy as np
from google.oauth2.service_account import Credentials

# ---------- PGSQL CONNECTION ----------
conn = psycopg2.connect(
    host=os.environ["PG_HOST"],
    database=os.environ["PG_DB"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    port=os.environ.get("PG_PORT", 5432)
)

# ---------- SQL QUERY (🔥 LAST 12 MONTH ROLLING + CURRENT MTD) ----------
query = """
SELECT 
pr.patient_id,
pr.age,
pr.district_id,
pr.gender_name,
pa.hosp_name,
pr.mobile_number,
pr.patient_name,
pr.is_nvf_facility,
pr.lead_source,
pr.marketing_person_name,
pa.assigned_to_name,
pa.assigned_to_role_name,
pa.appointment_date,
pa.appointment_time_slot

FROM public.patient_registration pr

LEFT JOIN public.patient_appointment pa
ON pr.patient_id = pa.patient_id

WHERE 
pa.appointment_time_slot <> ''
AND pa.appointment_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
AND pa.appointment_date::date <= CURRENT_DATE;


"""

df = pd.read_sql(query, conn)
conn.close()

print("📊 Rows fetched from PostgreSQL:", len(df))

# ---------- 🔥 GOOGLE SHEET SAFE CLEANING ----------
df = df.replace([np.inf, -np.inf], np.nan)
df = df.astype(str)
df = df.replace(["nan", "None", "NaT"], "")

# ---------- GOOGLE SHEET ----------
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=scope
)

client = gspread.authorize(creds)

sheet = client.open_by_key(
    os.environ["SHEET_ID"]
).worksheet("OPD")

if df.empty:
    print("⚠️ No data found. Sheet not updated.")
else:
    sheet.clear()
    sheet.update([df.columns.tolist()] + df.values.tolist())
    print("✅ PostgreSQL OPD data synced successfully")
