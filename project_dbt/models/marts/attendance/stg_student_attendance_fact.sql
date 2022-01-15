
SELECT
    student_attendance_fact.student_school_key                              AS student_school_key,
    student_attendance_fact.student_key                                     AS student_key,
    student_attendance_fact.school_key                                      AS school_key,
    date_dim.date                                                   AS date,
    student_attendance_fact.school_attendance_event_category_descriptor     AS school_attendance_event_category_descriptor,
    student_attendance_fact.event_duration                                  AS event_duration,
    student_attendance_fact.reported_as_present_at_school                   AS reported_as_present_at_school,
    student_attendance_fact.reported_as_absent_from_school                  AS reported_as_absent_from_school,
    student_attendance_fact.reported_as_present_at_home_room                AS reported_as_present_at_home_room,
    student_attendance_fact.reported_as_absent_from_home_room               AS reported_as_absent_from_home_room,
    student_attendance_fact.reported_as_is_present_in_all_sections          AS reported_as_is_present_in_all_sections,
    student_attendance_fact.reported_as_absent_from_any_section             AS reported_as_absent_from_any_section,
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
FROM {{ ref('student_attendance_fact') }} student_attendance_fact
LEFT JOIN {{ ref('stg_student_dim') }} stg_student_dim
    ON student_attendance_fact.student_school_key = stg_student_dim.student_school_key
LEFT JOIN {{ ref('date_dim') }} date_dim
    ON student_attendance_fact.date_key = date_dim.date_key
