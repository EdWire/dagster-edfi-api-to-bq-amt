
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.date')) AS date,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(calendar_events, "$.calendarEventDescriptor"), '#')[OFFSET(1)] AS calendar_event_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.calendarEvents")) calendar_events 
        ) AS calendar_events,
        STRUCT(
            JSON_VALUE(data, '$.calendarReference.calendarCode') AS calendar_code,
            JSON_VALUE(data, '$.calendarReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.calendarReference.schoolYear') AS int64) AS school_year
        ) AS calendar_reference
    FROM {{ source('staging', 'base_edfi_calendar_dates') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                calendar_reference.school_year,
                calendar_reference.school_id,
                calendar_reference.calendar_code,
                date 
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank, school_year),
    COALESCE(calendar_reference.school_year, school_year) AS school_year
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
