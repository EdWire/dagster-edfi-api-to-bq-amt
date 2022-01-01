
SELECT
    assessment_fact.title,
    assessment_fact.namespace,
    assessment_fact.academic_subject,
    student_assessment_fact.student_unique_id,
    student_assessment_fact.student_assessment_identifier,
    assessment_fact.identification_code,
    assessment_fact.objective_assessment_description,
    student_assessment_fact.reporting_method,
    student_assessment_fact.student_score
FROM {{ ref('student_assessment_fact') }} student_assessment_fact
LEFT JOIN {{ ref('assessment_fact') }} assessment_fact
    ON student_assessment_fact.assessment_key = assessment_fact.assessment_key
    AND student_assessment_fact.objective_assessment_key = assessment_fact.objective_assessment_key
    AND student_assessment_fact.reporting_method = assessment_fact.reporting_method
