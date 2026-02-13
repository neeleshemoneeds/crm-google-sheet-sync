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
    pr.patient_name,
    pa.patient_ref_id,
    pr.lead_source,
    pr.mobile_number,
    pa.appointment_date::date AS opd_date,

    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM patient_appointment pa_prev
            WHERE pa_prev.patient_id = pa.patient_id
              AND pa_prev.appointment_time_slot IS NOT NULL
              AND pa_prev.appointment_date::date < pa.appointment_date::date
        )
        THEN 'FOLLOW_UP OPD'
        ELSE 'NEW OPD'
    END AS opd_type,

    CASE 
        WHEN COUNT(csr.appointmentobjectid) > 0 
        THEN 'NTPC'
        ELSE 'NON NTPC'
    END AS csr_type

FROM patient_appointment pa

LEFT JOIN patient_registration pr
    ON pa.patient_id = pr.patient_id

LEFT JOIN patient_csr_terms csr
    ON pa._id = csr.appointmentobjectid

WHERE 
    pa.appointment_time_slot IS NOT NULL
    AND pa.appointment_date::date >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
    AND pa.appointment_date::date <= CURRENT_DATE

GROUP BY 
    pr.patient_name,
    pa.patient_ref_id,
    pr.lead_source,
    pr.mobile_number,
    pa.appointment_date,
    pa.patient_id,
    pa._id;

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
