{{ config(
        materialized='table',
        schema='edfi',
    )
}}


SELECT
    SPLIT(JSON_VALUE(data, "$.gradingPeriodDescriptor"), '#')[OFFSET(1)] AS grading_period_descriptor,
    CAST(JSON_VALUE(data, "$.periodSequence") AS int64) AS period_sequence,
    STRUCT(
        JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
    ) AS school_reference,
    STRUCT(
        JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS school_year
    ) AS school_year_type_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
    CAST(JSON_VALUE(data, "$.totalInstructionalDays") AS int64) AS total_instructional_days
FROM {{ source('raw_sources', 'edfi_grading_periods') }}
