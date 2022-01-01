
-- assessment score results
SELECT
    CONCAT(
        assessments.assessment_identifier, "-",
        assessments.namespace, "-",
        IF(
            assessed_grade_levels.grade_level_descriptor IS NOT NULL,
            assessed_grade_levels.grade_level_descriptor,
            "0"
        ), "-",
        scores.assessment_reporting_method_descriptor, "-",
        IF(
            academic_subjects.academic_subject_descriptor IS NOT NULL,
            academic_subjects.academic_subject_descriptor,
            "0"
        )
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
    scores.result_datatype_type_descriptor              AS result_data_type,
    scores.assessment_reporting_method_descriptor       AS reporting_method,
    ""                                                  AS objective_assessment_key,
    ""                                                  AS identification_code,
    ""                                                  AS parent_objective_assessment_key,
    ""                                                  AS objective_assessment_description,
    NULL                                                AS percent_of_assessment,
    scores.minimum_score                                AS min_score,
    scores.maximum_score                                AS max_score
FROM {{ ref('edfi_assessments') }} assessments
LEFT JOIN UNNEST(assessments.assessed_grade_levels) AS assessed_grade_levels
LEFT JOIN UNNEST(assessments.academic_subjects) AS academic_subjects
LEFT JOIN UNNEST(assessments.scores) AS scores


UNION ALL


-- objective assessment score results
SELECT
    CONCAT(
        assessments.assessment_identifier, "-",
        assessments.namespace, "-",
        IF(
            assessed_grade_levels.grade_level_descriptor IS NOT NULL,
            assessed_grade_levels.grade_level_descriptor,
            "0"
        ), "-",
        scores.assessment_reporting_method_descriptor, "-",
        IF(
            academic_subjects.academic_subject_descriptor IS NOT NULL,
            academic_subjects.academic_subject_descriptor,
            "0"
        ), "-",
        objective_assessments.identification_code, "-",
        IF(
            objective_assessments.parent_objective_assessment_reference.identification_code IS NOT NULL,
            objective_assessments.parent_objective_assessment_reference.identification_code,
            "0"
        ), "-",
        objective_assessment_scores.assessment_reporting_method_descriptor, "-",
        IF(
            objective_assessment_learning_standards.learning_standard_id IS NOT NULL,
            objective_assessment_learning_standards.learning_standard_id,
            "0"
        )
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
    objective_assessment_scores.result_datatype_type_descriptor                 AS result_data_type,
    objective_assessment_scores.assessment_reporting_method_descriptor          AS reporting_method,
    IF(objective_assessments.assessment_reference.assessment_identifier IS NOT NULL,
        CONCAT(
            objective_assessments.assessment_reference.assessment_identifier, "-",
            objective_assessments.identification_code, "-",
            assessment_reference.namespace
        ),
        NULL
    )                                                  AS objective_assessment_key,
    objective_assessments.identification_code          AS identification_code,
    IF(objective_assessments.parent_objective_assessment_reference.identification_code IS NOT NULL,
        CONCAT(
            objective_assessments.parent_objective_assessment_reference.assessment_identifier, "-",
            objective_assessments.parent_objective_assessment_reference.identification_code, "-",
            objective_assessments.parent_objective_assessment_reference.namespace
        ),
        NULL
    )                                                  AS parent_objective_assessment_key,
    objective_assessments.description                  AS objective_assessment_description,
    objective_assessments.percent_of_assessment        AS percent_of_assessment,
    objective_assessment_scores.minimum_score          AS min_score,
    objective_assessment_scores.maximum_score          AS max_score
FROM {{ ref('edfi_assessments') }} assessments
LEFT JOIN UNNEST(assessments.assessed_grade_levels) AS assessed_grade_levels
LEFT JOIN UNNEST(assessments.academic_subjects) AS academic_subjects
LEFT JOIN UNNEST(assessments.scores) AS scores
LEFT JOIN {{ ref('edfi_objective_assessments') }} objective_assessments
    ON assessments.assessment_identifier = objective_assessments.assessment_reference.assessment_identifier
    AND assessments.namespace = objective_assessments.assessment_reference.namespace
LEFT JOIN UNNEST(objective_assessments.scores) AS objective_assessment_scores
LEFT JOIN UNNEST(objective_assessments.learning_standards) AS objective_assessment_learning_standards
