
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.calendarCode') AS calendar_code,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS school_year
        ) AS school_year_type_reference,
        SPLIT(JSON_VALUE(data, "$.calendarTypeDescriptor"), '#')[OFFSET(1)] AS calendar_type_descriptor,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] AS grade_level_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradeLevels")) grade_levels 
        ) AS grade_levels
    FROM {{ source('raw_sources', 'edfi_calendars') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year_type_reference.school_year,
                school_reference.school_id,
                calendar_code
            ORDER BY extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (id, extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('edfi_deletes') }}
    )
