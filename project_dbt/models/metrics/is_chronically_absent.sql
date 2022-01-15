
WITH student_attendance AS (

    SELECT
        student_school_key                              AS student_school_key,
        student_key                                     AS student_key,
        school_key                                      AS school_key,
        SUM(event_duration)                             AS sum_of_absences,
        {# SUM(reported_as_absent_from_school) AS sum_of_absences, #} -- alternative if event duration is not used 
        COUNT(1)                                        AS number_of_days_enrolled,
        (COUNT(1) - SUM(event_duration)) / COUNT(1)     AS average_daily_attendance
    FROM {{ ref('stg_student_attendance_fact') }}
    GROUP BY 1, 2, 3

)

SELECT
    student_attendance.student_school_key                                              AS student_school_key,
    student_attendance.student_key                                                     AS student_key,
    IF(student_attendance.sum_of_absences >= 15, 1, 0)                                 AS is_chronically_absent,
    IF(student_attendance.average_daily_attendance < 0.92, 1, 1)                       AS early_warning,
    stg_student_dim.local_education_agency_name                     AS local_education_agency_name,
    stg_student_dim.school_name                                     AS school_name,
    stg_student_dim.student_last_surname                            AS student_last_surname,
    stg_student_dim.student_first_name                              AS student_first_name,
    stg_student_dim.student_display_name                            AS student_display_name,
    stg_student_dim.enrollment_date                                 AS enrollment_date,
    stg_student_dim.exit_date                                       AS exit_date,
    stg_student_dim.is_enrolled                                     AS is_enrolled,
    stg_student_dim.grade_level                                     AS grade_level,
    stg_student_dim.gender                                          AS gender,
    stg_student_dim.limited_english_proficiency                     AS limited_english_proficiency,
    stg_student_dim.is_english_language_learner                     AS is_english_language_learner,
    stg_student_dim.in_special_education_program                    AS in_special_education_program,
    stg_student_dim.is_hispanic                                     AS is_hispanic,
    stg_student_dim.race                                            AS race,
    stg_student_dim.race_and_ethnicity_roll_up                      AS race_and_ethnicity_roll_up
FROM student_attendance
LEFT JOIN {{ ref('stg_student_dim') }} stg_student_dim
    ON student_attendance.student_school_key = stg_student_dim.student_school_key