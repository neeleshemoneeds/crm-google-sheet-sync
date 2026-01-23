import os
import json
import psycopg2
import pandas as pd
import gspread
import numpy as np
from google.oauth2.service_account import Credentials

# ---------- PGSQL CONNECTION (GitHub Secrets) ----------
conn = psycopg2.connect(
    host=os.environ["PG_HOST"],
    database=os.environ["PG_DB"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    port=os.environ.get("PG_PORT", 5432)
)

query = """
SELECT 
    pa.*, 
    pr.lead_source
FROM public.patient_appointment pa
LEFT JOIN public.patient_registration pr
    ON pa.patient_id = pr.patient_id
WHERE pa.appointment_time_slot IS NOT NULL
  AND pa.appointment_date::date >= DATE '2025-12-01'
  AND pa.appointment_date::date <= CURRENT_DATE;
"""

df = pd.read_sql(query, conn)
conn.close()

# ---------- 🔥 IMPORTANT FIX (NaN / Infinity handling) ----------
# Google Sheets JSON NaN / inf accept nahi karta
df = df.replace([np.inf, -np.inf], np.nan)
df = df.fillna("")

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

# Existing Google Sheet, OPD tab
sheet = client.open_by_key(os.environ["SHEET_ID"]).worksheet("OPD")

# Clear old data and upload fresh
sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())

print("✅ PostgreSQL OPD data synced successfully")
