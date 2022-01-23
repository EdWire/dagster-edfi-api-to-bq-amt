
WITH assessments AS (

    SELECT
        fct_student_assessment.student_assessment_identifier,
        ARRAY_AGG(
            STRUCT(
                fct_student_assessment.reporting_method                    AS reporting_method,
                CAST(fct_student_assessment.student_score AS float64)      AS student_score
            )
        ) AS assessment_student_score
    FROM {{ ref('fct_student_assessment') }} fct_student_assessment
    LEFT JOIN {{ ref('fct_assessment') }} fct_assessment
        ON fct_student_assessment.school_year = fct_assessment.school_year
        AND fct_student_assessment.assessment_key = fct_assessment.assessment_key
        AND fct_student_assessment.objective_assessment_key = fct_assessment.objective_assessment_key
        AND fct_student_assessment.reporting_method = fct_assessment.reporting_method
    WHERE fct_assessment.objective_assessment_key = ""
    GROUP BY fct_student_assessment.student_assessment_identifier

),

objective_assessments AS (

    SELECT
        fct_student_assessment.student_assessment_identifier,
        ARRAY_AGG(
            STRUCT(
                fct_assessment.identification_code                       AS identification_code,
                fct_assessment.objective_assessment_description          AS description,
                fct_student_assessment.reporting_method                  AS reporting_method,
                CAST(fct_student_assessment.student_score AS float64)    AS student_score
            )
        ) AS objective_assessment_student_score
    FROM {{ ref('fct_student_assessment') }} fct_student_assessment
    LEFT JOIN {{ ref('fct_assessment') }} fct_assessment
        ON fct_student_assessment.school_year = fct_assessment.school_year
        AND fct_student_assessment.assessment_key = fct_assessment.assessment_key
        AND fct_student_assessment.objective_assessment_key = fct_assessment.objective_assessment_key
        AND fct_student_assessment.reporting_method = fct_assessment.reporting_method
    WHERE fct_assessment.objective_assessment_key != ""
    GROUP BY fct_student_assessment.student_assessment_identifier

)

SELECT
    fct_student_assessment.school_year                          AS school_year,
    fct_assessment.title                                        AS title,
    fct_assessment.namespace                                    AS namespace,
    fct_assessment.academic_subject                             AS academic_subject,
    dim_student.student_unique_id                               AS student_unique_id,
    fct_student_assessment.student_assessment_identifier        AS student_assessment_identifier,
    objective_assessments.objective_assessment_student_score    AS objective_assessment_student_score,
    assessments.assessment_student_score                        AS assessment_student_score,
    dim_school.school_name                                     AS school_name,
    dim_student.student_last_surname                            AS student_last_surname,
    dim_student.student_first_name                              AS student_first_name,
    dim_student.student_display_name                            AS student_display_name,
    dim_student.school_enrollment_date                          AS school_enrollment_date,
    dim_student.school_exit_date                                AS school_exit_date,
    dim_student.is_enrolled_at_school                           AS is_enrolled_at_school,
    dim_student.grade_level                                     AS grade_level,
    dim_student.gender                                          AS gender,
    dim_student.limited_english_proficiency                     AS limited_english_proficiency,
    dim_student.is_english_language_learner                     AS is_english_language_learner,
    dim_student.in_special_education_program                    AS in_special_education_program,
    dim_student.is_hispanic                                     AS is_hispanic,
    dim_student.race                                            AS race,
    dim_student.race_and_ethnicity_roll_up                      AS race_and_ethnicity_roll_up
FROM {{ ref('fct_student_assessment') }} fct_student_assessment
LEFT JOIN {{ ref('fct_assessment') }} fct_assessment
    ON fct_student_assessment.school_year = fct_assessment.school_year
    AND fct_student_assessment.assessment_key = fct_assessment.assessment_key
    AND fct_student_assessment.objective_assessment_key = fct_assessment.objective_assessment_key
    AND fct_student_assessment.reporting_method = fct_assessment.reporting_method
LEFT JOIN assessments
    ON fct_student_assessment.student_assessment_identifier = assessments.student_assessment_identifier
LEFT JOIN objective_assessments
    ON fct_student_assessment.student_assessment_identifier = objective_assessments.student_assessment_identifier
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON fct_student_assessment.school_key = dim_student.school_key
    AND fct_student_assessment.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON dim_student.school_key = dim_school.school_key
WHERE fct_student_assessment.objective_assessment_key = ""
