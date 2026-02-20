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
    patient_id,
    age,
    district_id,
    gender_name,
    hosp_name,
    mobile_number,
    patient_name,
    lead_source,
    marketing_person_name,
    assigned_to_name,
    assigned_to_role_name,
    opd_date,
    appointment_time_slot,
    opd_status
FROM (
    SELECT 
        pr.patient_id,
        pr.age,
        pr.district_id,
        pr.gender_name,
        pa.hosp_name,
        pr.mobile_number,
        pr.patient_name,
        pr.lead_source,
        pr.marketing_person_name,
        pa.assigned_to_name,
        pa.assigned_to_role_name,
        pa.appointment_date::date AS opd_date,
        pa.appointment_time_slot,

        CASE 
            WHEN (
                SELECT COUNT(*) 
                FROM public.patient_appointment pa_prev
                WHERE pa_prev.patient_id = pa.patient_id
                AND pa_prev.appointment_time_slot <> ''
                AND pa_prev.appointment_date::date < pa.appointment_date::date
            ) > 0 
            THEN 'OLD OPD'
            ELSE 'NEW OPD'
        END AS opd_status,

        ROW_NUMBER() OVER (
            PARTITION BY pr.mobile_number, pa.appointment_date::date
            ORDER BY pa.appointment_date DESC
        ) AS rn

    FROM public.patient_registration pr

    LEFT JOIN public.patient_appointment pa
        ON pr.patient_id = pa.patient_id

    LEFT JOIN public.patient_csr_terms csr
        ON pa._id = csr.appointmentobjectid

    WHERE 
        pa.appointment_time_slot <> ''
        AND pa.appointment_status IN (1,5)
        AND csr.appointmentobjectid IS NULL
        AND pr.is_nvf_facility = 'FALSE'
        AND LOWER(pr.patient_name) NOT LIKE 'test%'
        AND LOWER(pr.patient_name) NOT LIKE '%test'
        AND pa.appointment_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
        AND pa.appointment_date::date <= CURRENT_DATE
) t
WHERE rn = 1;


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
