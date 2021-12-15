
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.sectionIdentifier') AS section_identifier,
        JSON_VALUE(data, '$.sectionName') AS section_name,
        STRUCT(
            JSON_VALUE(data, '$.courseOfferingReference.localCourseCode') AS local_course_code,
            JSON_VALUE(data, '$.courseOfferingReference.schoolId') AS school_id,
            JSON_VALUE(data, '$.courseOfferingReference.schoolYear') AS school_year,
            JSON_VALUE(data, '$.courseOfferingReference.sessionName') AS session_name
        ) AS course_offering_reference,
        CAST(JSON_VALUE(data, '$.availableCreditConversion') AS float64) AS available_credit_conversion,
        CAST(JSON_VALUE(data, '$.availableCredits') AS float64) AS available_credits,
        SPLIT(JSON_VALUE(data, '$.availableCreditTypeDescriptor'), '#')[OFFSET(1)] AS available_credit_type_descriptor,
        STRUCT(
            JSON_VALUE(data, '$.locationReference.classroomIdentificationCode') AS classroom_identification_code,
            JSON_VALUE(data, '$.locationReference.schoolId') AS school_id
        ) AS location_reference,
        STRUCT(
            JSON_VALUE(data, '$.locationSchoolReference.schoolId') AS school_id
        ) AS location_school_reference,
    FROM {{ source('raw_sources', 'edfi_sections') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                course_offering_reference.school_year,
                course_offering_reference.school_id,
                course_offering_reference.session_name,
                course_offering_reference.local_course_code,
                section_identifier
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
