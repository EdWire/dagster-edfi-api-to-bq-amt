
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.localCourseCode') AS local_course_code,
        JSON_VALUE(data, '$.localCourseTitle') AS local_course_title,
        STRUCT(
            JSON_VALUE(data, '$.courseReference.courseCode') AS course_code,
            JSON_VALUE(data, '$.courseReference.educationOrganizationId') AS education_organization_id
        ) AS course_reference,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            JSON_VALUE(data, '$.sessionReference.schoolId') AS school_id,
            JSON_VALUE(data, '$.sessionReference.schoolYear') AS school_year,
            JSON_VALUE(data, '$.sessionReference.sessionName') AS session_name
        ) AS session_reference,
    FROM {{ source('raw_sources', 'edfi_course_offerings') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                session_reference.school_year,
                session_reference.school_id,
                session_reference.session_name,
                local_course_code
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
