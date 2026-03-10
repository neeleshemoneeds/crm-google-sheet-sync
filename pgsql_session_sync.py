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
    ps.created_by_user_id,
    ps.session_date,
    pr.lead_source
FROM public.patient_session ps
LEFT JOIN public.patient_registration pr
    ON ps.patient_id = pr.patient_id
WHERE ps.session_date::date >= (date_trunc('month', CURRENT_DATE)::date - INTERVAL '11 months')::date
  AND ps.session_date::date <= CURRENT_DATE
"""

df = pd.read_sql(query, conn)
conn.close()

# ---------- SMART TYPE-AWARE CLEANING ----------

# 1) Date columns → DD-MM-YYYY string format
date_columns = ['session_date']
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].dt.strftime('%d-%m-%Y')
        df[col] = df[col].fillna("")

# 2) Number columns → proper numeric
number_columns = ['created_by_user_id']
for col in number_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# 3) Replace inf and NaN safely
df = df.replace([np.inf, -np.inf], np.nan)

# 4) Convert to Google Sheets compatible list (cell by cell cleaning)
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

sheet = client.open_by_key(os.environ["SHEET_ID"]).worksheet("Session")

sheet.clear()
sheet.update([df.columns.tolist()] + rows, value_input_option='USER_ENTERED')

print("✅ PostgreSQL Session data synced successfully with proper formatting")
