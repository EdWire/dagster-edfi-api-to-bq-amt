
SELECT
    CONCAT(
        assessments.assessment_identifier, "-",
        assessments.namespace, "-",
        -- assessment assessed grade level,
        scores.assessment_reporting_method_descriptor, "-",
        academic_subjects.academic_subject_descriptor, "-",
        objective_assessments.identification_code, "-",
        -- objective assessment parent objective code
        objective_assessment_scores.assessment_reporting_method_descriptor, "-"
        -- objective learning standard
    )                                                   AS assessment_fact_key,
    CONCAT(
        assessments.assessment_identifier, "-",
        assessments.namespace
    )                                                   AS assessment_key,
    assessments.assessment_identifier                   AS assessment_identifier,
    assessments.namespace                               AS namespace,
    assessments.assessment_title                        AS title,
    IFNULL(assessments.assessment_version, 0)           AS version,
    assessments.assessment_category_descriptor	        AS category,
    assessed_grade_levels.grade_level_descriptor        AS assessed_grade_level,
    academic_subjects.academic_subject_descriptor       AS academic_subject,
    COALESCE(
        scores.result_datatype_type_descriptor,
        objective_assessment_scores.result_datatype_type_descriptor
    )                                                   AS result_data_type,
    COALESCE(
        scores.assessment_reporting_method_descriptor,
        objective_assessment_scores.assessment_reporting_method_descriptor
    )                                                  AS reporting_method,
    IF(objective_assessments.assessment_reference.assessment_identifier IS NOT NULL,
        CONCAT(
            objective_assessments.assessment_reference.assessment_identifier, "-",
            objective_assessments.identification_code, "-",
            assessment_reference.namespace
        ),
        NULL
    )                                                  AS objective_assessment_key,
    objective_assessments.identification_code          AS identification_code,
    -- parent_objective_assessment_key,
    objective_assessments.description                  AS objective_assessment_description,
    objective_assessments.percent_of_assessment        AS percent_of_assessment,
    COALESCE(
        scores.minimum_score,
        objective_assessment_scores.minimum_score
    )                                                  AS min_score,
    COALESCE(
        scores.maximum_score,
        objective_assessment_scores.maximum_score
    )                                                  AS max_score
FROM {{ ref('edfi_assessments') }} assessments
LEFT JOIN UNNEST(assessments.assessed_grade_levels) AS assessed_grade_levels
LEFT JOIN UNNEST(assessments.academic_subjects) AS academic_subjects
LEFT JOIN UNNEST(assessments.scores) AS scores
LEFT JOIN {{ ref('edfi_objective_assessments') }} objective_assessments
    ON assessments.assessment_identifier = objective_assessments.assessment_reference.assessment_identifier
    AND assessments.namespace = objective_assessments.assessment_reference.namespace
LEFT JOIN UNNEST(objective_assessments.scores) AS objective_assessment_scores
