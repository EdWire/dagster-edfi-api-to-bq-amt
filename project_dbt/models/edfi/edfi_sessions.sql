
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.sessionName') AS session_name,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS school_year
        ) AS school_year_type_reference,
        SPLIT(JSON_VALUE(data, "$.termDescriptor"), '#')[OFFSET(1)] AS term_descriptor,
        CAST(JSON_VALUE(data, "$.totalInstructionalDays") AS int64) AS total_instructional_days,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(grading_periods, '$.gradingPeriodReference.gradingPeriodDescriptor'), '#')[OFFSET(1)] AS grading_period_descriptor,
                CAST(JSON_VALUE(grading_periods, "$.gradingPeriodReference.periodSequence") AS int64) AS period_sequence,
                JSON_VALUE(grading_periods, "$.gradingPeriodReference.schoolId") AS school_id,
                JSON_VALUE(grading_periods, "$.gradingPeriodReference.schoolYear") AS school_year
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradingPeriods")) grading_periods 
        ) AS grading_periods
    FROM {{ source('raw_sources', 'edfi_sessions') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year_type_reference.school_year,
                school_reference.school_id,
                session_name
            ORDER BY extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('edfi_deletes') }}
    )
