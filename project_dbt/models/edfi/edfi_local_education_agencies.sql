{{ config(
        materialized='table',
        schema='edfi',
    )
}}


SELECT
    JSON_VALUE(data, '$.localEducationAgencyId') AS local_education_agency_id,
    JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution
FROM {{ source('raw_sources', 'edfi_local_education_agencies') }}
