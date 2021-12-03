{{ config(
        materialized='table',
        schema='staging',
    )
}}


SELECT 
    NULL AS state_unique_id,
    student_key,
    student_last_surname,
    student_first_name,
    student_last_surname || ', ' || student_first_name AS student_display_name,
    sex AS gender,
    birth_date,
    grade_level,
    IF(limited_english_proficiency = 'Limited', 'Yes', 'No') AS is_ell,
    limited_english_proficiency,
    NULL AS has_iep,
    school_key,
    school_name,
    enrollment_date_key
FROM {{ ref('stg_student_school_dim') }} student_school_dim
