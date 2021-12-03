{{ config(
        materialized='table',
        schema='staging',
    )
}}


SELECT
    student_school_dim.* EXCEPT(grade_level),
    {{ convert_grade_level('grade_level') }} AS grade_level,
    school_dim.school_name
FROM {{ ref('student_school_dim') }} student_school_dim
LEFT JOIN {{ ref('school_dim') }} school_dim ON school_dim.school_key = student_school_dim.school_key
