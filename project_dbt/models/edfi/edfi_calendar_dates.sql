
SELECT
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.date')) AS date,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(calendar_events, "$.calendarEventDescriptor"), '#')[OFFSET(1)] AS calendar_event_descriptor
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.calendarEvents")) calendar_events 
    ) AS calendar_events,
    STRUCT(
        JSON_VALUE(data, '$.calendarReference.calendarCode') AS calendar_code,
        JSON_VALUE(data, '$.calendarReference.schoolId') AS school_id,
        JSON_VALUE(data, '$.calendarReference.schoolYear') AS school_year
    ) AS calendar_reference
FROM {{ source('raw_sources', 'edfi_calendar_dates') }}
