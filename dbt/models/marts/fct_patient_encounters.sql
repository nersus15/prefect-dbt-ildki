{{ config(
    materialized='table',
    engine='OLAP',
    primary_key='encounter_id',
    distributed_by=['encounter_id']
) }}

WITH encounters AS (
    SELECT * FROM {{ ref('stg_encounters') }}
),

conditions AS (
    -- Kita ambil satu diagnosa utama per encounter (jika ada)
    SELECT 
        encounter_id,
        icd10_code,
        diagnosis_name
    FROM {{ ref('stg_conditions') }}
),

patients AS (
    SELECT 
        satusehat_id AS patient_id,
        get_json_string(body, '$.gender') AS gender,
        get_json_string(body, '$.birthDate') AS birth_date
    FROM {{ source('ildki', 'logs__patients') }}
)

SELECT
    e.encounter_id,
    e.encounter_date,
    e.status AS encounter_status,
    e.class_code,
    p.gender,
    -- Hitung Umur saat kunjungan
    timestampdiff(
        YEAR,
        str_to_date(p.birth_date, '%Y-%m-%d'),
        e.encounter_date
    ) AS patient_age,
    c.icd10_code,
    c.diagnosis_name,
    1 AS total_visits -- Helper untuk agregasi sum()
FROM encounters e
LEFT JOIN conditions c ON e.encounter_id = c.encounter_id
LEFT JOIN patients p ON e.patient_id = p.patient_id