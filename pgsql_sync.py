import os
import psycopg2
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json

# ---------- PGSQL CREDS (GitHub Secrets se) ----------
conn = psycopg2.connect(
    host=os.environ["77.37.44.94"],
    database=os.environ["emoneeds_prod"],
    user=os.environ["neelesh"],
    password=os.environ["Neelesh#@5444"],
    port=os.environ.get("PG_PORT", 5432)
)

query = """
SELECT *
FROM patient_data
"""

df = pd.read_sql(query, conn)
conn.close()

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

# Same sheet, NEW TAB
sheet = client.open_by_key(os.environ["SHEET_ID"]).worksheet("OPD")

sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())

print("✅ PostgreSQL data synced")
