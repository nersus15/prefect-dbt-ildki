{{ config(materialized='table') }}

SELECT
    satusehat_id AS encounter_id,
    patient_id,
    organization_id,
    -- Ekstraksi status dari JSON body
    get_json_string(body, '$.status') AS status,
    -- Ekstraksi kelas kunjungan (misal: ambulatory, inpatient)
    get_json_string(body, '$.class.code') AS class_code,
    
    CAST(created_at AS DATE) AS encounter_date, 
    
    created_at,
    updated_at
FROM {{ source('ildki', 'logs__encounters') }}
WHERE deleted_at IS NULL