
SELECT
    student_assessment_fact.school_year                         AS school_year,
    assessment_fact.title                                       AS title,
    assessment_fact.namespace                                   AS namespace,
    assessment_fact.academic_subject                            AS academic_subject,
    student_assessment_fact.student_key                         AS student_key,
    student_assessment_fact.student_school_key                  AS student_school_key,
    student_assessment_fact.student_assessment_identifier       AS student_assessment_identifier,
    assessment_fact.identification_code                         AS identification_code,
    assessment_fact.objective_assessment_description            AS objective_assessment_description,
    student_assessment_fact.reporting_method                    AS reporting_method,
    student_assessment_fact.student_score                       AS student_score,
    student_dim.local_education_agency_name                     AS local_education_agency_name,
    student_dim.school_name                                     AS school_name,
    student_dim.student_last_surname                            AS student_last_surname,
    student_dim.student_first_name                              AS student_first_name,
    student_dim.student_display_name                            AS student_display_name,
    student_dim.enrollment_date                                 AS enrollment_date,
    student_dim.exit_date                                       AS exit_date,
    student_dim.is_enrolled                                     AS is_enrolled,
    student_dim.grade_level                                     AS grade_level,
    student_dim.gender                                          AS gender,
    student_dim.limited_english_proficiency                     AS limited_english_proficiency,
    student_dim.is_english_language_learner                     AS is_english_language_learner,
    student_dim.in_special_education_program                    AS in_special_education_program,
    student_dim.is_hispanic                                     AS is_hispanic,
    student_dim.race                                            AS race,
    student_dim.race_and_ethnicity_roll_up                      AS race_and_ethnicity_roll_up
FROM {{ ref('student_assessment_fact') }} student_assessment_fact
LEFT JOIN {{ ref('assessment_fact') }} assessment_fact
    ON student_assessment_fact.school_year = assessment_fact.school_year
    AND student_assessment_fact.assessment_key = assessment_fact.assessment_key
    AND student_assessment_fact.objective_assessment_key = assessment_fact.objective_assessment_key
    AND student_assessment_fact.reporting_method = assessment_fact.reporting_method
LEFT JOIN {{ ref('student_dim') }} student_dim
    ON student_assessment_fact.school_year = student_dim.school_year
    AND student_assessment_fact.student_school_key = student_dim.student_school_key
