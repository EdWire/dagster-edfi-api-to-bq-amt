SELECT
    to_hex(md5(cast(coalesce(cast(assessments.assessment_identifier as 
    string
), '') || '-' || coalesce(cast(assessments.namespace as 
    string
), '') as 
    string
)))                                               AS assessment_key,
    to_hex(md5(cast(coalesce(cast(education_organization_reference.education_organization_id as 
    string
), '') || '-' || coalesce(cast(assessments.school_year as 
    string
), '') as 
    string
)))                                               AS education_organization_key,
    assessments.school_year                             AS school_year,
    assessments.assessment_identifier                   AS assessment_identifier,
    assessments.assessment_family                       AS assessment_family,
    assessments.namespace                               AS namespace,
    assessments.assessment_title                        AS title,
    IFNULL(assessments.assessment_version, 0)           AS version,
    assessments.assessment_category_descriptor	        AS category,
    assessment_form                                     AS form,
    IF(adaptive_assessment IS TRUE, 'Yes', 'No')        AS adaptive_assessment
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_assessments` assessments