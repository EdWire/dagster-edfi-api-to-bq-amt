{{ config(
        materialized='table',
        schema='edfi',
    )
}}


SELECT
    JSON_VALUE(data, '$.courseCode') AS course_code,
    JSON_VALUE(data, '$.courseTitle') AS course_title,
    JSON_VALUE(data, '$.courseDescription') AS course_description,
    SPLIT(JSON_VALUE(data, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
    SPLIT(JSON_VALUE(data, "$.careerPathwayDescriptor"), '#')[OFFSET(1)] AS career_pathway_descriptor,
    SPLIT(JSON_VALUE(data, "$.courseDefinedByDescriptor"), '#')[OFFSET(1)] AS course_defined_by_descriptor,
    STRUCT(
        JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
    ) AS education_organization_reference,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(levels, "$.competencyLevelDescriptor"), '#')[OFFSET(1)] AS competency_level_descriptor,
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.competencyLevels")) levels 
    ) AS competency_levels,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(codes, "$.courseIdentificationSystemDescriptor"), '#')[OFFSET(1)] AS course_identification_system_descriptor,
            SPLIT(JSON_VALUE(codes, "$.assigningOrganizationIdentificationCode"), '#')[OFFSET(1)] AS assigning_organization_identification_code,
            JSON_VALUE(codes, "$.courseCatalogURL") AS course_catalog_url,
            JSON_VALUE(codes, "$.identificationCode") AS identification_code
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.identificationCodes")) codes 
    ) AS identification_codes,
FROM {{ source('raw_sources', 'edfi_courses') }}

