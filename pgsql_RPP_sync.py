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

query = """
SELECT
    prr._id,
    prr.patient_rpp_id,
    prr.amount,
    prr.assigned_to_name,
    prr.assigned_to_role_name,
    prr.counsellor_name,
    prr.date_created,
    prr.date_updated,
    prr.due_date,
    prr.enrollment_date,
    prr.hosp_name,
    prr.mobile_number,
    prr.package_diagnosis_name,
    prr.package_name,
    prr.package_price,
    prr.patient_id,
    prr.psychiatrist_name,
    prr.psychologist_name,
    prr.renewalstatus,
    prr.status,
    prr.customized_plan,
    prr.renewed,
    prr.renewal_date,
    prr.lead_source
FROM public.patient_rpp_registration prr
WHERE prr.enrollment_date::date >= DATE '2025-12-01'
  AND prr.enrollment_date::date <= CURRENT_DATE;


"""

df = pd.read_sql(query, conn)
conn.close()

# ---------- 🔥 ULTIMATE GOOGLE SHEET SAFE CLEANING ----------

# 1️⃣ Replace NaN / inf
df = df.replace([np.inf, -np.inf], np.nan)

# 2️⃣ Convert EVERYTHING to string (BEST FIX)
df = df.astype(str)

# 3️⃣ Replace "nan", "None", "NaT" with empty
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

sheet = client.open_by_key(os.environ["SHEET_ID"]).worksheet("RPP")

sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())

print("✅ PostgreSQL RPP data synced successfully")
