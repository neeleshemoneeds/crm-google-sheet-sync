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

# ---------- SQL QUERY (FIXED) ----------
query = """
SELECT 
    pr.patient_name,
    pa.patient_ref_id,
    pr.lead_source,
    pr.mobile_number,
    pa.appointment_date AS opd_date,

    -- OPD TYPE (New / Follow-up)
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM patient_appointment prev
            WHERE prev.patient_id = pa.patient_id
              AND prev.appointment_time_slot IS NOT NULL
              AND prev.appointment_date < pa.appointment_date
        )
        THEN 'FOLLOW_UP OPD'
        ELSE 'NEW OPD'
    END AS opd_type,

    -- CSR TYPE (NTPC / NON NTPC)
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM patient_csr_terms csr
            WHERE csr.appointmentobjectid = pa.id
        )
        THEN 'NTPC'
        ELSE 'NON NTPC'
    END AS csr_type

FROM patient_appointment pa

LEFT JOIN patient_registration pr 
    ON pa.patient_id = pr.patient_id

WHERE pa.appointment_time_slot IS NOT NULL

-- 🔥 LAST 12 FULL MONTHS (AUTO ROLLING)
AND pa.appointment_date >= date_trunc('month', CURRENT_DATE) - INTERVAL '12 months'
AND pa.appointment_date < date_trunc('month', CURRENT_DATE);
"""

df = pd.read_sql(query, conn)
conn.close()

print("📊 Rows fetched from PostgreSQL:", len(df))

# ---------- SAFE CLEANING ----------
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
    print("✅ PostgreSQL OPD data synced successfully (Last 12 Months Auto)")
