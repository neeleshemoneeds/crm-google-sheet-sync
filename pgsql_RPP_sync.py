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

-- ========== MAIN QUERY ==========
SELECT
    patient_id, hosp_id, assigned_to, gender_name, hosp_name, mobile_number,
    patient_name, lead_source, marketing_person_name, psychologist_name,
    psychiatrist_name, counsellor_name, counsellor_user_id, enrollment_date,
    due_date, package_diagnosis_name, package_name, service_name, plan_status,
    direct_after_opd, patient_ref_id, months_with_us
FROM (
    SELECT
        pr.patient_id, pp.hosp_id, pp.assigned_to, pr.gender_name, pp.hosp_name,
        pr.mobile_number, pr.patient_name, pr.lead_source, pr.marketing_person_name,
        rp.psychologist_name, rp.psychiatrist_name, rp.counsellor_name,
        pp.patient_ref_id, pp.counsellor_user_id, pp.enrollment_date, pp.due_date,
        pp.package_diagnosis_name, pp.package_name, pra.service_name,

        CASE
            WHEN ph.prev_enrollment IS NULL THEN 'NEW PLAN'
            WHEN ph.prev_enrollment IS NOT NULL
                 AND pp.enrollment_date::date <= ph.prev_due::date THEN 'RENEWAL'
            WHEN ph.prev_enrollment IS NOT NULL
                 AND pp.enrollment_date::date > ph.prev_due::date
                 AND pp.enrollment_date::date <= (ph.prev_due::date + INTERVAL '30 days') THEN 'LATE RENEWAL'
            ELSE 'REVIVAL'
        END AS plan_status,

        CASE
            WHEN ph.prev_enrollment IS NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM public.patient_appointment pa
                     WHERE pa.patient_id = pr.patient_id
                       AND pa.appointment_time_slot IS NOT NULL
                       AND pa.appointment_time_slot <> ''
                 )
            THEN 'Direct Plan'
            WHEN ph.prev_enrollment IS NULL
                 AND EXISTS (
                     SELECT 1 FROM public.patient_appointment pa
                     WHERE pa.patient_id = pr.patient_id
                       AND pa.appointment_time_slot IS NOT NULL
                       AND pa.appointment_time_slot <> ''
                 )
            THEN 'After OPD'
            ELSE NULL
        END AS direct_after_opd,

        (SELECT COUNT(*) FROM public.patient_rpp_registration all_pp
         WHERE all_pp.patient_id = pr.patient_id
           AND all_pp.enrollment_date <= pp.enrollment_date
        ) AS months_with_us,

        ROW_NUMBER() OVER (
            PARTITION BY pr.mobile_number, pp.enrollment_date::date
            ORDER BY pp.enrollment_date DESC
        ) AS rn

    FROM public.patient_registration pr
    JOIN plan_history pp ON pr.patient_id = pp.patient_id
    LEFT JOIN role_pivot rp ON rp.patient_id = pr.patient_id
    LEFT JOIN public.patient_csr_terms csr ON pp._id = csr.rppobjectid
    LEFT JOIN public.patient_appointment pa_join ON pa_join.patient_id = pr.patient_id
    LEFT JOIN public.patient_rpp_assignment pra ON pra.patient_rpp_id = pa_join.patient_rpp_id
    LEFT JOIN plan_history ph ON ph._id = pp._id

    WHERE
        (
            pr.is_nvf_facility = 'FALSE'
            OR pr.is_nvf_support_revoked = 'TRUE'
            OR EXISTS (
                SELECT 1 FROM public.patient_rpp_registration rpp_chk
                WHERE rpp_chk.patient_id = pr.patient_id
                AND LOWER(COALESCE(rpp_chk.remark, '')) <> 'nvf'
            )
        )
        AND csr.rppobjectid IS NULL
        AND pr.lead_source <> 'CSR'
        AND LOWER(pr.patient_name) NOT LIKE 'test%'
        AND LOWER(pr.patient_name) NOT LIKE '%test'
        AND pp.enrollment_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
        AND pp.enrollment_date::date <= CURRENT_DATE
) t
WHERE rn = 1

UNION ALL

-- ========== INACTIVE ROWS ==========
SELECT
    pr.patient_id, lp.hosp_id, lp.assigned_to, pr.gender_name, lp.hosp_name,
    pr.mobile_number, pr.patient_name, pr.lead_source, pr.marketing_person_name,
    rp.psychologist_name, rp.psychiatrist_name, rp.counsellor_name,
    lp.counsellor_user_id, lp.due_date AS enrollment_date, NULL AS due_date,
    lp.package_diagnosis_name, lp.package_name, NULL AS service_name,
    'INACTIVE' AS plan_status, NULL AS direct_after_opd, lp.patient_ref_id,
    (SELECT COUNT(*) FROM public.patient_rpp_registration all_pp
     WHERE all_pp.patient_id = lp.patient_id
    ) + 1 AS months_with_us
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY enrollment_date DESC) AS lrn
    FROM public.patient_rpp_registration
) lp
JOIN public.patient_registration pr ON pr.patient_id = lp.patient_id
LEFT JOIN role_pivot rp ON rp.patient_id = pr.patient_id
WHERE lp.lrn = 1
  AND lp.due_date::date < CURRENT_DATE
  AND NOT EXISTS (
      SELECT 1 FROM public.patient_rpp_registration future_pp
      WHERE future_pp.patient_id = lp.patient_id
        AND future_pp.enrollment_date::date > lp.due_date::date
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.patient_csr_terms csr
      WHERE csr.rppobjectid = lp._id
  )
  AND (
      pr.is_nvf_facility = 'FALSE'
      OR pr.is_nvf_support_revoked = 'TRUE'
      OR EXISTS (
          SELECT 1 FROM public.patient_rpp_registration rpp_chk
          WHERE rpp_chk.patient_id = pr.patient_id
          AND LOWER(COALESCE(rpp_chk.remark, '')) <> 'nvf'
      )
  )
  AND pr.lead_source <> 'CSR'
  AND LOWER(pr.patient_name) NOT LIKE 'test%'
  AND LOWER(pr.patient_name) NOT LIKE '%test'
  AND lp.due_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months'
  AND lp.due_date::date <= CURRENT_DATE
"""

df = pd.read_sql(query, conn)
conn.close()

# ---------- SMART TYPE-AWARE CLEANING ----------

# 1) Define which columns are dates, numbers, and text-that-looks-like-number
date_columns = ['enrollment_date', 'due_date']
number_columns = ['hosp_id', 'assigned_to', 'counsellor_user_id', 'months_with_us']
text_number_columns = ['mobile_number', 'patient_ref_id']  # ye number hai but text rehna chahiye

# 2) Date columns → DD-MM-YYYY string format
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].dt.strftime('%d-%m-%Y')
        df[col] = df[col].fillna("")

# 3) Number columns → proper numeric (int/float)
for col in number_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# 4) Text-number columns → string with apostrophe prefix so Sheets treats as text
for col in text_number_columns:
    if col in df.columns:
        df[col] = df[col].astype(str).replace(["nan", "None", ""], "")
        df[col] = df[col].apply(lambda x: f"'{x}" if x else "")

# 5) Replace inf and NaN safely
df = df.replace([np.inf, -np.inf], np.nan)

# 6) Convert to Google Sheets compatible list (cell by cell cleaning)
def clean_cell(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    if isinstance(val, (int, np.integer)):
        return int(val)
    if isinstance(val, (float, np.floating)):
        return float(val)
    val_str = str(val)
    if val_str in ["nan", "None", "NaT", "inf", "-inf"]:
        return ""
    return val_str

rows = []
for _, row in df.iterrows():
    rows.append([clean_cell(v) for v in row])

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
sheet.update([df.columns.tolist()] + rows, value_input_option='USER_ENTERED')

print("✅ PostgreSQL RPP data synced successfully with proper formatting")
