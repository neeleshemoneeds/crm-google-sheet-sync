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
WITH filtered_rpp AS (
    SELECT *
    FROM public.patient_rpp_registration
    WHERE enrollment_date::date >= date_trunc('month', CURRENT_DATE) - INTERVAL '24 months'
      AND enrollment_date::date <= CURRENT_DATE
),

latest_roles AS (
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
        MAX(CASE WHEN assigned_to_role_name='Psychologist' THEN assigned_to_name END) AS psychologist_name,
        MAX(CASE WHEN assigned_to_role_name='Psychiatrist' THEN assigned_to_name END) AS psychiatrist_name,
        MAX(CASE WHEN assigned_to_role_name='Counsellor' THEN assigned_to_name END) AS counsellor_name
    FROM latest_roles
    GROUP BY patient_id
),

diagnosis_data AS (
    SELECT
        patient_id,
        MAX(diagnosis_name::text) AS diagnosis_name,
        MAX(assessment_name::text) AS assessment_name
    FROM public.patient_provision_diagnosis_treatment
    GROUP BY patient_id
),

appointment_flag AS (
    SELECT DISTINCT patient_id, TRUE AS has_appointment
    FROM public.patient_appointment
    WHERE appointment_time_slot IS NOT NULL
      AND appointment_time_slot <> ''
),

plan_history AS (
    SELECT
        pp.*,
        LAG(pp.enrollment_date::date) OVER (PARTITION BY patient_id ORDER BY enrollment_date::date) AS prev_enrollment,
        LAG(pp.due_date::date) OVER (PARTITION BY patient_id ORDER BY enrollment_date::date) AS prev_due,
        COUNT(*) OVER (PARTITION BY patient_id ORDER BY enrollment_date::date) AS months_with_us
    FROM filtered_rpp pp
),

latest_plan AS (
    SELECT DISTINCT ON (patient_id) *
    FROM filtered_rpp
    ORDER BY patient_id, enrollment_date::date DESC
)

SELECT
    patient_id,
    hosp_id::bigint,
    assigned_to::bigint,
    gender_name,
    hosp_name,
    mobile_number::bigint,
    patient_name,
    lead_source,
    marketing_person_name,
    psychologist_name,
    psychiatrist_name,
    counsellor_name,
    counsellor_user_id::bigint,
    enrollment_date::date,
    due_date::date,
    package_name,
    plan_status,
    direct_after_opd,
    patient_ref_id::bigint,
    months_with_us::bigint,
    diagnosis_name,
    assessment_name,
    amount::bigint
FROM (
    SELECT
        pr.patient_id,
        pp.hosp_id,
        pp.assigned_to,
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
        pp.patient_ref_id,
        pp.enrollment_date,
        pp.due_date,
        pp.package_name,
        pp.amount,
        dd.diagnosis_name,
        dd.assessment_name,
        pp.months_with_us,

        CASE
            WHEN pp.prev_enrollment IS NULL THEN 'NEW PLAN'
            WHEN pp.enrollment_date::date <= pp.prev_due THEN 'RENEWAL'
            WHEN pp.enrollment_date::date <= pp.prev_due + INTERVAL '30 days'
                THEN 'LATE RENEWAL'
            ELSE 'REVIVAL'
        END AS plan_status,

        CASE
            WHEN pp.prev_enrollment IS NULL
                 AND af.has_appointment IS NULL
            THEN 'Direct Plan'
            WHEN pp.prev_enrollment IS NULL
                 AND af.has_appointment = TRUE
            THEN 'After OPD'
        END AS direct_after_opd,

        ROW_NUMBER() OVER (
            PARTITION BY pr.mobile_number, pp.enrollment_date::date
            ORDER BY pp.enrollment_date::date DESC
        ) AS rn

    FROM public.patient_registration pr
    JOIN plan_history pp
        ON pr.patient_id = pp.patient_id

    LEFT JOIN role_pivot rp
        ON rp.patient_id = pr.patient_id

    LEFT JOIN diagnosis_data dd
        ON dd.patient_id = pr.patient_id

    LEFT JOIN appointment_flag af
        ON af.patient_id = pr.patient_id

    LEFT JOIN public.patient_csr_terms csr
        ON pp._id = csr.rppobjectid

    WHERE
        (
            pr.is_nvf_facility = 'FALSE'
            OR pr.is_nvf_support_revoked = 'TRUE'
            OR EXISTS (
                SELECT 1
                FROM public.patient_rpp_registration rpp_chk
                WHERE rpp_chk.patient_id = pr.patient_id
                AND LOWER(COALESCE(rpp_chk.remark,'')) <> 'nvf'
            )
        )
        AND csr.rppobjectid IS NULL
        AND pr.lead_source <> 'CSR'
        AND LOWER(pr.patient_name) NOT LIKE 'test%'
        AND LOWER(pr.patient_name) NOT LIKE '%test'
) t
WHERE rn = 1

UNION ALL

SELECT
    pr.patient_id,
    lp.hosp_id::bigint,
    lp.assigned_to::bigint,
    pr.gender_name,
    lp.hosp_name,
    pr.mobile_number::bigint,
    pr.patient_name,
    pr.lead_source,
    pr.marketing_person_name,
    rp.psychologist_name,
    rp.psychiatrist_name,
    rp.counsellor_name,
    lp.counsellor_user_id::bigint,
    lp.due_date::date AS enrollment_date,
    NULL::date AS due_date,
    lp.package_name,
    'INACTIVE' AS plan_status,
    NULL AS direct_after_opd,
    lp.patient_ref_id::bigint,
    (COUNT(*) OVER (PARTITION BY lp.patient_id)+1)::bigint AS months_with_us,
    dd.diagnosis_name,
    dd.assessment_name,
    lp.amount::bigint

FROM latest_plan lp

JOIN public.patient_registration pr
    ON pr.patient_id = lp.patient_id

LEFT JOIN role_pivot rp
    ON rp.patient_id = pr.patient_id

LEFT JOIN diagnosis_data dd
    ON dd.patient_id = pr.patient_id

WHERE
    lp.due_date::date < CURRENT_DATE

    AND NOT EXISTS (
        SELECT 1
        FROM public.patient_rpp_registration future_pp
        WHERE future_pp.patient_id = lp.patient_id
        AND future_pp.enrollment_date::date > lp.due_date::date
    )

    AND NOT EXISTS (
        SELECT 1
        FROM public.patient_csr_terms csr
        WHERE csr.rppobjectid = lp._id
    )

    AND pr.lead_source <> 'CSR'
    AND LOWER(pr.patient_name) NOT LIKE 'test%'
    AND LOWER(pr.patient_name) NOT LIKE '%test'
    AND lp.due_date::date >= date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months'
    AND lp.due_date::date <= CURRENT_DATE;
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
