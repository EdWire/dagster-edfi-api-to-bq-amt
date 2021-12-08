
SELECT
    STRUCT(
        JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
    ) AS staff_reference,
    STRUCT(
        JSON_VALUE(data, '$.sectionReference.localCourseCode') AS local_course_code,
        JSON_VALUE(data, '$.sectionReference.schoolId') AS school_id,
        JSON_VALUE(data, '$.sectionReference.schoolYear') AS school_year,
        JSON_VALUE(data, '$.sectionReference.sectionIdentifier') AS section_identifier,
        JSON_VALUE(data, '$.sectionReference.sessionName') AS session_name
    ) AS section_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
    SPLIT(JSON_VALUE(data, "$.classroomPositionDescriptor"), '#')[OFFSET(1)] AS classroom_position_descriptor,
    CAST(JSON_VALUE(data, '$.highlyQualifiedTeacher') AS BOOL) AS highly_qualified_teacher,
    JSON_VALUE(data, '$.percentageContribution') AS percentage_contribution
FROM {{ source('raw_sources', 'edfi_staff_section_associations') }}
