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
    counsellor_user_id,
    enrollment_date,
    due_date,
    package_diagnosis_name,
    package_name,
    plan_status,
    direct_after_opd,
    service_name  -- ✅ Naya column
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
        pra.assigned_to_name,
        pra.assigned_to_role_name,
		pra.service_name,
        pp.counsellor_user_id,
        pp.enrollment_date,
        pp.due_date,
        pp.package_diagnosis_name,
        pp.package_name,
          -- ✅ patient_rpp_assignment se service_name
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
        END AS plan_status,
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM public.patient_rpp_registration old_pp
                WHERE old_pp.patient_id = pp.patient_id
                AND old_pp.enrollment_date::date < pp.enrollment_date::date
            )
            AND NOT EXISTS (
                SELECT 1
                FROM public.patient_appointment pa
                WHERE pa.patient_id = pp.patient_id
                AND pa.appointment_time_slot IS NOT NULL
                AND pa.appointment_time_slot <> ''
            )
            THEN 'Direct Plan'
            WHEN NOT EXISTS (
                SELECT 1
                FROM public.patient_rpp_registration old_pp
                WHERE old_pp.patient_id = pp.patient_id
                AND old_pp.enrollment_date::date < pp.enrollment_date::date
            )
            AND EXISTS (
                SELECT 1
                FROM public.patient_appointment pa
                WHERE pa.patient_id = pp.patient_id
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
    LEFT JOIN public.patient_rpp_registration pp
        ON pr.patient_id = pp.patient_id
    LEFT JOIN public.patient_csr_terms csr
        ON pp._id = csr.rppObjectId
    -- ✅ patient_appointment ke through patient_rpp_assignment join
    LEFT JOIN public.patient_appointment pa_join
        ON pa_join.patient_id = pr.patient_id
    LEFT JOIN public.patient_rpp_assignment pra
        ON pra.patient_rpp_id = pa_join.patient_rpp_id
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
