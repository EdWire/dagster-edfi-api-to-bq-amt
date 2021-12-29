SELECT
    CONCAT(
        student_assessments.assessment_reference.assessment_identifier, "-",
        student_assessments.assessment_reference.namespace, "-",
        student_assessments.student_assessment_identifier, "-",
        score_results.assessment_reporting_method_descriptor, "-",
        performance_levels.performance_level_descriptor, "-",
        student_objective_assessments.objective_assessment_reference.identification_code, "-"
        -- student_objective_assessments
    )                                                                               AS student_assessment_fact_key,
    CONCAT(
        student_assessments.assessment_reference.assessment_identifier, "-",
        student_assessments.assessment_reference.namespace, "-",
        student_assessments.student_assessment_identifier, "-",
        student_assessments.student_reference.student_unique_id
    )                                                                               AS student_assessment_key,
    IF(student_objective_assessments.objective_assessment_reference.identification_code IS NOT NULL,
        CONCAT(
            student_assessments.student_reference.student_unique_id, "-",
            student_objective_assessments.objective_assessment_reference.identification_code, "-",
            student_objective_assessments.objective_assessment_reference.assessment_identifier, "-",
            student_assessments.student_assessment_identifier, "-",
            student_objective_assessments.objective_assessment_reference.namespace
        ), NULL
    )                                                                               AS student_objective_assessment_key,
    IF(student_objective_assessments.objective_assessment_reference.identification_code IS NOT NULL,
        CONCAT(
            student_objective_assessments.objective_assessment_reference.assessment_identifier, "-",
            student_objective_assessments.objective_assessment_reference.identification_code, "-",
            student_objective_assessments.objective_assessment_reference.namespace
        ), NULL
    )                                                                               AS objective_assessment_key,
    CONCAT(
        student_assessments.assessment_reference.assessment_identifier, "-",
        student_assessments.assessment_reference.namespace
)                                                                                   AS assessment_key,
    student_assessments.assessment_reference.assessment_identifier                  AS assessment_identifier,
    student_assessments.assessment_reference.namespace                              AS namespace,
    student_assessments.student_assessment_identifier                               AS student_assessment_identifier,
    student_assessments.student_reference.student_unique_id                         AS student_unique_id,
    CONCAT(
        student_assessments.student_reference.student_unique_id, "-",
        student_school_associations.school_reference.school_id
    )                                                                               AS student_school_key,
    student_school_associations.school_reference.school_id                          AS school_key,
    FORMAT_DATE('%Y%m%d', student_assessments.administration_date)                  AS administration_date,
    student_assessments.when_assessed_grade_level_descriptor                        AS assessed_grade_level,
    COALESCE(
        student_objective_assessments_score_results.result,
        score_results.result
    )                                                                               AS student_score,
    -- result data type
    -- reporting method
    -- performance result
FROM {{ ref('edfi_student_assessments') }} student_assessments
LEFT JOIN UNNEST(student_assessments.score_results) AS score_results
LEFT JOIN UNNEST(student_assessments.performance_levels) AS performance_levels
LEFT JOIN UNNEST(student_assessments.student_objective_assessments) AS student_objective_assessments
LEFT JOIN UNNEST(student_objective_assessments.score_results) AS student_objective_assessments_score_results
LEFT JOIN {{ ref('edfi_student_school_associations') }} student_school_associations
    ON student_assessments.school_year = student_school_associations.school_year
    AND student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
