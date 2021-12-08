
SELECT
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
