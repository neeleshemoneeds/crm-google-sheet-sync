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
WITH latest_roles AS (
    SELECT DISTINCT ON (pa.patient_id, pra.assigned_to_role_name)
        pa.patient_id,
        pra.assigned_to_role_name,
        pra.assigned_to_name
    FROM public.patient_rpp_assignment pra
    JOIN public.patient_appointment pa
        ON pa.patient_rpp_id = pra.patient_rpp_id
    WHERE pra.assigned_to_role_name IN ('Psychologist','Psychiatrist','Counsellor')
    ORDER BY pa.patient_id, pra.assigned_to_role_name, pra.date_created DESC
),

role_pivot AS (
    SELECT
        patient_id,
        MAX(CASE WHEN assigned_to_role_name = 'Psychologist' THEN assigned_to_name END) AS psychologist_name,
        MAX(CASE WHEN assigned_to_role_name = 'Psychiatrist' THEN assigned_to_name END) AS psychiatrist_name,
        MAX(CASE WHEN assigned_to_role_name = 'Counsellor' THEN assigned_to_name END) AS counsellor_name
    FROM latest_roles
    GROUP BY patient_id
),

plan_history AS (
    SELECT
        pp.*,
        LAG(pp.enrollment_date) OVER (PARTITION BY patient_id ORDER BY enrollment_date) AS prev_enrollment,
        LAG(pp.due_date) OVER (PARTITION BY patient_id ORDER BY enrollment_date) AS prev_due,
        LEAD(pp.enrollment_date) OVER (PARTITION BY patient_id ORDER BY enrollment_date) AS next_enrollment
    FROM public.patient_rpp_registration pp
)

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
    psychologist_name,
    psychiatrist_name,
    counsellor_name,
    counsellor_user_id,
    enrollment_date,
    due_date,
    package_diagnosis_name,
    package_name,
    service_name,
    plan_status,
    direct_after_opd
FROM (
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

        rp.psychologist_name,
        rp.psychiatrist_name,
        rp.counsellor_name,

        pp.counsellor_user_id,
        pp.enrollment_date,
        pp.due_date,
        pp.package_diagnosis_name,
        pp.package_name,
        pra.service_name,

        CASE
            WHEN ph.prev_enrollment IS NULL THEN 'NEW PLAN'
            WHEN ph.prev_enrollment IS NOT NULL
                 AND pp.enrollment_date::date <= ph.prev_due::date THEN 'RENEW'
            WHEN pp.due_date::date < CURRENT_DATE
                 AND ph.next_enrollment IS NULL THEN 'INACTIVE'
            ELSE 'REVIVAL'
        END AS plan_status,

        CASE
            WHEN ph.prev_enrollment IS NULL
                 AND NOT EXISTS (
                     SELECT 1
                     FROM public.patient_appointment pa
                     WHERE pa.patient_id = pr.patient_id
                       AND pa.appointment_time_slot IS NOT NULL
                       AND pa.appointment_time_slot <> ''
                 )
            THEN 'Direct Plan'

            WHEN ph.prev_enrollment IS NULL
                 AND EXISTS (
                     SELECT 1
                     FROM public.patient_appointment pa
                     WHERE pa.patient_id = pr.patient_id
                       AND pa.appointment_time_slot IS NOT NULL
                       AND pa.appointment_time_slot <> ''
                 )
            THEN 'After OPD'
            ELSE NULL
        END AS direct_after_opd,

        ROW_NUMBER() OVER (
            PARTITION BY pr.mobile_number, pp.enrollment_date::date
            ORDER BY pp.enrollment_date DESC
        ) AS rn

    FROM public.patient_registration pr
    JOIN plan_history pp
        ON pr.patient_id = pp.patient_id
    LEFT JOIN role_pivot rp
        ON rp.patient_id = pr.patient_id
    LEFT JOIN public.patient_csr_terms csr
        ON pp._id = csr.rppobjectid
    LEFT JOIN public.patient_appointment pa_join
        ON pa_join.patient_id = pr.patient_id
    LEFT JOIN public.patient_rpp_assignment pra
        ON pra.patient_rpp_id = pa_join.patient_rpp_id
    LEFT JOIN plan_history ph
        ON ph._id = pp._id

    WHERE
        pr.is_nvf_facility = 'FALSE'
        AND csr.rppobjectid IS NULL
        AND pr.lead_source <> 'CSR'
        AND LOWER(pr.patient_name) NOT LIKE 'test%'
        AND LOWER(pr.patient_name) NOT LIKE '%test'
        AND pp.enrollment_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
        AND pp.enrollment_date::date <= CURRENT_DATE
) t
WHERE rn = 1;


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
