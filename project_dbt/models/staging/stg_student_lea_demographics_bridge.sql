{{ config(
        materialized='table',
        schema='staging',
    )
}}


SELECT
    student_lea.local_education_agency_key,
    student_lea.student_key,
    student_lea.student_first_name,
    student_lea.student_middle_name,
    student_lea.student_last_surname,
    demographic_dim.demographic_parent_key,
    descriptors.short_description AS descriptor_short_description
FROM {{ ref('student_local_education_agency_dim') }} student_lea
LEFT JOIN {{ ref('student_local_education_agency_demographics_bridge') }} student_demo_bridge
    ON student_demo_bridge.student_local_education_agency_key = student_lea.student_local_education_agency_key
LEFT JOIN {{ ref('demographic_dim') }} demographic_dim
    ON demographic_dim.demographic_key = student_demo_bridge.demographic_key
LEFT JOIN {{ ref('edfi_descriptors') }} descriptors
    ON descriptors.code_value = demographic_dim.demographic_label
WHERE demographic_dim.demographic_parent_key IS NOT NULL
