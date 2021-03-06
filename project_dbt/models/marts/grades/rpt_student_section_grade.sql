
SELECT DISTINCT
    dim_student.student_unique_id                               AS student_unique_id,
    dim_student.student_display_name                            AS student_display_name,
    fct_student_section_grade.school_year                       AS school_year,
    dim_student_section.local_course_code                       AS local_course_code,
    dim_student_section.course_title                            AS course_title,
    dim_section.section_identifier                              AS section_identifier,
    dim_student_section.academic_subject                        AS academic_subject,
    dim_session.session_name                       AS session_name,
    dim_session.term_name                          AS term_name,
    dim_section.course_gpa_applicability                        AS course_gpa_applicability,
    dim_section.available_credits                               AS available_credits,
    dim_grading_period.grading_period_description               AS grading_period_description,
    IF(
        CURRENT_DATE BETWEEN dim_grading_period.grading_period_begin_date AND dim_grading_period.grading_period_end_date,
        TRUE,
        FALSE
    )                                                           AS is_current_grading_period,
    fct_student_section_grade.grade_type                        AS grade_type,
    fct_student_section_grade.numeric_grade_earned              AS numeric_grade_earned,
    fct_student_section_grade.letter_grade_earned               AS letter_grade_earned,
    IF(
        CURRENT_DATE BETWEEN dim_student_section.student_section_start_date AND dim_student_section.student_section_end_date,
        "Yes",
        "No"
    )                                                           AS is_currently_enrolled_in_section,
    dim_local_education_agency.local_education_agency_name      AS local_education_agency_name,
    dim_school.school_name                                      AS school_name,
    dim_student.student_last_surname                            AS student_last_surname,
    dim_student.student_first_name                              AS student_first_name,
    dim_student.is_actively_enrolled                            AS is_actively_enrolled,
    dim_student.grade_level                                     AS grade_level,
    dim_student.grade_level_id                                  AS grade_level_id,
    dim_student.gender                                          AS gender,
    dim_student.limited_english_proficiency                     AS limited_english_proficiency,
    dim_student.is_english_language_learner                     AS is_english_language_learner,
    dim_student.in_special_education_program                    AS in_special_education_program,
    dim_student.is_hispanic                                     AS is_hispanic,
    dim_student.race_and_ethnicity_roll_up                      AS race_and_ethnicity_roll_up
FROM {{ ref('fct_student_section_grade') }} fct_student_section_grade
LEFT JOIN {{ ref('dim_student_section') }} dim_student_section
    ON fct_student_section_grade.student_section_key = dim_student_section.student_section_key
LEFT JOIN {{ ref('dim_section') }} dim_section
    ON dim_student_section.section_key = dim_section.section_key
LEFT JOIN {{ ref('dim_session') }} dim_session
    ON dim_student_section.session_key = dim_session.session_key
LEFT JOIN {{ ref('dim_grading_period') }} dim_grading_period
    ON fct_student_section_grade.grading_period_key = dim_grading_period.grading_period_key
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON fct_student_section_grade.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON fct_student_section_grade.school_key = dim_school.school_key
LEFT JOIN {{ ref('dim_local_education_agency') }} dim_local_education_agency
    ON dim_school.local_education_agency_key = dim_local_education_agency.local_education_agency_key
