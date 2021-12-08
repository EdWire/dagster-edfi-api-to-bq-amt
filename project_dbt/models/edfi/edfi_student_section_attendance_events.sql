
SELECT
    STRUCT(
        JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
    ) AS student_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.eventDate')) AS event_date,
    STRUCT(
        JSON_VALUE(data, '$.sectionReference.localCourseCode') AS local_course_code,
        JSON_VALUE(data, '$.sectionReference.schoolId') AS school_id,
        JSON_VALUE(data, '$.sectionReference.schoolYear') AS school_year,
        JSON_VALUE(data, '$.sectionReference.sectionIdentifier') AS section_identifier,
        JSON_VALUE(data, '$.sectionReference.sessionName') AS session_name
    ) AS section_reference,
    ARRAY(
        SELECT AS STRUCT 
            JSON_VALUE(class_periods, "$.classPeriodName") AS class_period_name,
            JSON_VALUE(class_periods, '$.schoolId') AS school_id,
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.classPeriods")) class_periods 
    ) AS class_periods,
    JSON_VALUE(data, '$.arrivalTime') AS arrival_time,
    JSON_VALUE(data, '$.departureTime') AS departure_time,
    JSON_VALUE(data, '$.attendanceEventReason') AS attendance_event_reason,
    CAST(JSON_VALUE(data, '$.eventDuration') AS float64) AS event_duration,
    CAST(JSON_VALUE(data, '$.sectionAttendanceDuration') AS float64) AS section_attendance_duration,
    SPLIT(JSON_VALUE(data, '$.attendanceEventCategoryDescriptor'), '#')[OFFSET(1)] AS attendance_event_category_descriptor,
    SPLIT(JSON_VALUE(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] AS educational_environment_descriptor,
FROM {{ source('raw_sources', 'edfi_student_section_attendance_events') }}
