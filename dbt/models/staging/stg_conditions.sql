{{ config(materialized='table') }}

SELECT
    satusehat_id AS condition_id,
    encounter_id,
    patient_id,
    -- Mengambil kode diagnosa (ICD-10)
    get_json_string(body, '$.code.coding[0].code') AS icd10_code,
    get_json_string(body, '$.code.coding[0].display') AS diagnosis_name,
    get_json_string(body, '$.clinicalStatus.coding[0].code') AS clinical_status
FROM {{ source('ildki', 'logs__conditions') }}
WHERE deleted_at IS NULL