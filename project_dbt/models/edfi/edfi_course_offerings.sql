
SELECT
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

