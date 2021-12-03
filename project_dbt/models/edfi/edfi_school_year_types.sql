{{ config(
        materialized='table',
        schema='edfi',
    )
}}


SELECT
    JSON_VALUE(data, '$.schoolYear') AS school_year,
    CAST(JSON_VALUE(data, '$.currentSchoolYear') AS BOOL) AS current_school_year,
    JSON_VALUE(data, '$.schoolYearDescription') AS school_year_description
FROM {{ source('raw_sources', 'edfi_school_year_types') }}
