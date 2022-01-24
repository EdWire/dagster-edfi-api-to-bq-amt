
SELECT
    dim_student.student_unique_id                                          AS student_unique_id,
    dim_student.student_display_name                                       AS student_display_name,
    dim_school.school_id                                                   AS school_id,
    dim_date.date                                                          AS date,
    dim_date.month                                                         AS month,
    dim_date.month_sort_order                                              AS month_sort_order,
    fct_student_attendance.school_attendance_event_category_descriptor     AS school_attendance_event_category_descriptor,
    fct_student_attendance.event_duration                                  AS event_duration,
    fct_student_attendance.reported_as_present_at_school                   AS reported_as_present_at_school,
    fct_student_attendance.reported_as_absent_from_school                  AS reported_as_absent_from_school,
    fct_student_attendance.reported_as_present_at_home_room                AS reported_as_present_at_home_room,
    fct_student_attendance.reported_as_absent_from_home_room               AS reported_as_absent_from_home_room,
    fct_student_attendance.reported_as_is_present_in_all_sections          AS reported_as_is_present_in_all_sections,
    fct_student_attendance.reported_as_absent_from_any_section             AS reported_as_absent_from_any_section,
    dim_local_education_agency.local_education_agency_name                 AS local_education_agency_name,
    dim_school.school_name                                                 AS school_name,
    dim_student.student_last_surname                                       AS student_last_surname,
    dim_student.student_first_name                                         AS student_first_name,
    dim_student.school_enrollment_date                                     AS school_enrollment_date,
    dim_student.school_exit_date                                           AS school_exit_date,
    dim_student.is_enrolled_at_school                                      AS is_enrolled_at_school,
    dim_student.grade_level                                                AS grade_level,
    dim_student.gender                                                     AS gender,
    dim_student.limited_english_proficiency                                AS limited_english_proficiency,
    dim_student.is_english_language_learner                                AS is_english_language_learner,
    dim_student.in_special_education_program                               AS in_special_education_program,
    dim_student.is_hispanic                                                AS is_hispanic,
    dim_student.race                                                       AS race,
    dim_student.race_and_ethnicity_roll_up                                 AS race_and_ethnicity_roll_up
FROM {{ ref('fct_student_attendance') }} fct_student_attendance
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON fct_student_attendance.school_key = dim_student.school_key
    AND fct_student_attendance.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_date') }} dim_date
    ON fct_student_attendance.date = dim_date.date
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON dim_student.school_key = dim_school.school_key
LEFT JOIN {{ ref('dim_local_education_agency') }} dim_local_education_agency
    ON dim_student.local_education_agency_key = dim_local_education_agency.local_education_agency_key
