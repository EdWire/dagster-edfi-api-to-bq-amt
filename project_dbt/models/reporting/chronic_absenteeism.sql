{{ config(
        materialized='table',
        schema='reporting',
    )
}}


SELECT
    {{ normalize_school_name('attendance_fact.school_name') }} AS school_name,
    attendance_fact.student_key,
    attendance_fact.student_last_surname || ', ' || attendance_fact.student_first_name AS student_display_name,
    student_school_dim.grade_level,
    student_school_dim.is_enrolled,
    attendance_fact.date,
    attendance_fact.reported_as_absent_from_school,
    attendance_fact.school_attendance_descriptor,
    attendance_fact.number_of_days_enrolled,
    IF(SUM(reported_as_absent_from_school) OVER (PARTITION BY attendance_fact.student_school_key) >= 15, "Yes", "No") AS is_chronically_absent,
    IF((
        (attendance_fact.number_of_days_enrolled - SUM(attendance_fact.reported_as_absent_from_school) OVER (PARTITION BY attendance_fact.student_school_key)) 
         / attendance_fact.number_of_days_enrolled
    ) < 0.92, "Yes", "No") AS early_warning 
FROM {{ ref('stg_attendance_fact') }} attendance_fact
LEFT JOIN {{ ref('stg_student_school_dim') }} student_school_dim
    ON attendance_fact.student_school_key = student_school_dim.student_school_key
