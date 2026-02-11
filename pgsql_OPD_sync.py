import pandas as pd
import psycopg2
import os

# ---------------------------
# DATABASE CONNECTION
# ---------------------------
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    database=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    port=os.environ["DB_PORT"]
)

# ---------------------------
# FINAL QUERY (ONLY REQUIRED COLUMNS)
# ---------------------------
query = """
SELECT 
    pa._id,
    pa.patient_ref_id,
    
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

    pr.patient_name,
    pr.lead_source,
    pr.mobile_number,
    pa.appointment_date AS opd_date,

    -- CSR TYPE (NTPC / NON NTPC)
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM patient_csr_terms csr
            WHERE csr.appointmentobjectid = pa._id
        )
        THEN 'NTPC'
        ELSE 'NON NTPC'
    END AS csr_type

FROM patient_appointment pa

LEFT JOIN patient_registration pr 
    ON pa.patient_id = pr.patient_id

WHERE pa.appointment_time_slot IS NOT NULL

-- LAST 12 FULL MONTHS (AUTO ROLLING)
AND pa.appointment_date >= date_trunc('month', CURRENT_DATE) - INTERVAL '12 months'
AND pa.appointment_date < date_trunc('month', CURRENT_DATE)

ORDER BY pa.appointment_date DESC;
"""

# ---------------------------
# FETCH DATA
# ---------------------------
df = pd.read_sql(query, conn)

conn.close()

# ---------------------------
# EXPORT TO CSV (optional test)
# ---------------------------
df.to_csv("opd_output.csv", index=False)

print("✅ OPD Data Synced Successfully")
