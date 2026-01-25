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

# ---------- SQL QUERY ----------
query = """
SELECT
    pa._id,
    pa.patient_appointment_id,
    pa.patient_id,
    pa.patient_ref_id,
    pa.patient_name,
    pa.assigned_type,
    pa.assigned_to_name,
    pa.assigned_to_role_name,
    pa.hosp_name,
    pa.appointment_date,
    pa.date_created,
    pa.appointment_time_slot,
    pa.patient_rpp_id,
    pa.is_online,
    pr.lead_source
FROM public.patient_appointment pa
LEFT JOIN public.patient_registration pr
    ON pa.patient_id = pr.patient_id
WHERE pa.appointment_time_slot IS NOT NULL
  AND pa.appointment_date::date >= DATE '2025-12-01'
  AND pa.appointment_date::date <= CURRENT_DATE
  AND NOT EXISTS (
        SELECT 1
        FROM public.patient_csr_terms pct
        WHERE pct.patientid = pa.patient_id
  );
"""

df = pd.read_sql(query, conn)
conn.close()

# ✅ ADD (SAFE DEBUG – OPTIONAL BUT USEFUL)
print("📊 Rows fetched from PostgreSQL:", len(df))

# ---------- 🔥 ULTIMATE GOOGLE SHEET SAFE CLEANING ----------

# 1️⃣ Replace NaN / inf
df = df.replace([np.inf, -np.inf], np.nan)

# 2️⃣ Convert EVERYTHING to string (MOST IMPORTANT FIX)
df = df.astype(str)

# 3️⃣ Replace string junk with empty
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

# ✅ ADD (SAFE GUARD – EMPTY DATA CHECK)
if df.empty:
    print("⚠️ No data found. Sheet not updated.")
else:
    sheet.clear()
    sheet.update([df.columns.tolist()] + df.values.tolist())
    print("✅ PostgreSQL OPD data synced successfully")
