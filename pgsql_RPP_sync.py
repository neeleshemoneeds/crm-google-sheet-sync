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
    port=int(os.environ.get("PG_PORT", 5432))
)

query = """
SELECT 
pr.patient_id,
pr.age,
pr.district_id,
pr.gender_name,
pp.hosp_name,
pr.mobile_number,
pr.patient_name,
pr.lead_source,
pr.marketing_person_name,
pp.assigned_to_name,
pp.assigned_to_role_name,
pp.counsellor_user_id,
pp.enrollment_date,
pp.due_date,
pp.package_diagnosis_name,
pp.package_name,
csr.rppobjectid,
csr.type,

CASE

   
    WHEN NOT EXISTS (
        SELECT 1
        FROM public.patient_rpp_registration old_pp
        WHERE old_pp.patient_id = pp.patient_id
        AND old_pp.enrollment_date::date < pp.enrollment_date::date
    )
    THEN 'NEW PLAN'

   
    WHEN EXISTS (
        SELECT 1
        FROM public.patient_rpp_registration old_pp
        WHERE old_pp.patient_id = pp.patient_id
        AND old_pp.enrollment_date::date < pp.enrollment_date::date
        AND pp.enrollment_date::date <= old_pp.due_date::date
    )
    THEN 'RENEW'

    
    WHEN pp.due_date::date < CURRENT_DATE
         AND NOT EXISTS (
            SELECT 1
            FROM public.patient_rpp_registration next_pp
            WHERE next_pp.patient_id = pp.patient_id
            AND next_pp.enrollment_date::date > pp.due_date::date
         )
    THEN 'INACTIVE'

   
    ELSE 'REVIVAL'

END AS plan_status

FROM public.patient_registration pr

LEFT JOIN public.patient_rpp_registration pp
ON pr.patient_id = pp.patient_id

LEFT JOIN public.patient_csr_terms csr
ON pp._id = csr.rppObjectId

WHERE
pr.is_nvf_facility = 'FALSE'
AND csr.rppobjectid IS NULL
AND pr.lead_source <> 'CSR' 
AND pp.enrollment_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
AND pp.enrollment_date::date <= CURRENT_DATE;


"""

df = pd.read_sql(query, conn)
conn.close()

# ---------- 🔥 GOOGLE SHEET SAFE CLEANING (NO 400 ERRORS) ----------

# Replace inf values
df = df.replace([np.inf, -np.inf], np.nan)

# Convert EVERYTHING to string
df = df.astype(str)

# Clean common invalid literals
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
