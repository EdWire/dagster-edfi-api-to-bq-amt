SELECT
    to_hex(md5(cast(coalesce(cast(assessments.assessment_identifier as 
    string
), '') || '-' || coalesce(cast(assessments.namespace as 
    string
), '') || '-' || coalesce(cast(objective_assessments.identification_code as 
    string
), '') as 
    string
)))                                               AS objective_assessment_key,
    to_hex(md5(cast(coalesce(cast(assessments.assessment_identifier as 
    string
), '') || '-' || coalesce(cast(assessments.namespace as 
    string
), '') as 
    string
)))                                               AS assessment_key,
    objective_assessments.school_year                   AS school_year,
    objective_assessments.identification_code           AS identification_code,
    objective_assessments.academic_subject_descriptor   AS academic_subject,
    objective_assessments.description                   AS description
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_objective_assessments` objective_assessments
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_assessments` assessments
    ON objective_assessments.assessment_reference.assessment_identifier = assessments.assessment_identifier
    AND objective_assessments.assessment_reference.namespace = assessments.namespace
    AND objective_assessments.school_year = assessments.school_year