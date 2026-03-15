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
    pf.patient_id,
    pf.hosp_name,
    pf.feedback_date::date,
    pf.is_absent,
    pf.updated_by_user_id,

    CASE
        WHEN pr.is_nvf_facility = 'FALSE'
             AND csr.patientid IS NULL
        THEN 'Regular'
        ELSE 'CSR'
    END AS Category_type

FROM public.patient_feedback pf

LEFT JOIN public.patient_registration pr
    ON pf.patient_id = pr.patient_id

LEFT JOIN public.patient_csr_terms csr
    ON pf.patient_id = csr.patientid

WHERE pf.feedback_date::date BETWEEN
      (date_trunc('month', CURRENT_DATE)::date - INTERVAL '12 months')::date
      AND CURRENT_DATE;
"""

df = pd.read_sql(query, conn)
conn.close()

# ---------- SMART TYPE-AWARE CLEANING ----------

# 1) Date columns → DD-MM-YYYY format
date_columns = ['feedback_date']
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].dt.strftime('%d-%m-%Y')
        df[col] = df[col].fillna("")

# 2) Number columns → numeric format
number_columns = ['updated_by_user_id']
for col in number_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# 3) Replace inf and NaN
df = df.replace([np.inf, -np.inf], np.nan)

# 4) Google Sheet safe cleaning
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

sheet = client.open_by_key(os.environ["SHEET_ID"]).worksheet("Feedback")

sheet.clear()
sheet.update([df.columns.tolist()] + rows, value_input_option='USER_ENTERED')

print("✅ PostgreSQL Feedback data synced successfully with proper formatting")
