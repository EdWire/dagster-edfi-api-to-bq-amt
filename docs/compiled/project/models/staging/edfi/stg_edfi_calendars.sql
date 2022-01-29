WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.calendarCode') AS calendar_code,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
        ) AS school_year_type_reference,
        SPLIT(JSON_VALUE(data, "$.calendarTypeDescriptor"), '#')[OFFSET(1)] AS calendar_type_descriptor,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] AS grade_level_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradeLevels")) grade_levels 
        ) AS grade_levels
    FROM `gcp-proj-id`.`dev_staging`.`base_edfi_calendars`

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                school_year_type_reference.school_year,
                school_reference.school_id,
                calendar_code
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank, school_year),
    COALESCE(school_year_type_reference.school_year, school_year) AS school_year
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_deletes` edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )