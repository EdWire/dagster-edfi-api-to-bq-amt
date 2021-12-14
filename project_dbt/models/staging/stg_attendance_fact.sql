{{ config(
        materialized='table',
        schema='staging',
    )
}}


SELECT
    attendance_fact.student_school_key,
    school_dim.school_key,
    school_dim.school_name,
    attendance_fact.student_key,
    student_school_dim.student_last_surname,
    student_school_dim.student_middle_name,
    student_school_dim.student_first_name,
    date_dim.date,
    attendance_fact.school_attendance_event_category_descriptor AS school_attendance_descriptor,
    attendance_fact.reported_as_absent_from_school,
    COUNT(date_dim.date) 
        OVER (PARTITION BY attendance_fact.student_school_key) AS number_of_days_enrolled
FROM {{ ref('attendance_fact') }} attendance_fact
LEFT JOIN {{ ref('stg_student_school_dim') }} student_school_dim
    ON attendance_fact.student_school_key = student_school_dim.student_school_key
LEFT JOIN {{ ref('school_dim') }} school_dim
    ON school_dim.school_key = attendance_fact.school_key
LEFT JOIN {{ ref('date_dim') }} date_dim
    ON date_dim.date_key = attendance_fact.date_key
ORDER BY date_dim.date

