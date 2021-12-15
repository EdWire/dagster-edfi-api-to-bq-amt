
{%
    set tables = [
        "edfi_local_education_agencies_deletes",
        "edfi_schools_deletes",
        "edfi_students_deletes",
        "edfi_student_education_organization_associations_deletes",
        "edfi_student_school_associations_deletes",
        "edfi_calendars_deletes",
        "edfi_calendars_deletes",
        "edfi_calendar_dates_deletes",
        "edfi_courses_deletes",
        "edfi_course_offerings_deletes",
        "edfi_discipline_actions_deletes",
        "edfi_discipline_incident_deletes",
        "edfi_grades_deletes",
        "edfi_grading_periods_deletes",
        "edfi_staff_discipline_incident_associations_deletes",
        "edfi_student_discipline_incident_associations_deletes",
        "edfi_parents_deletes",
        "edfi_sections_deletes",
        "edfi_staffs_deletes",
        "edfi_staff_education_organization_assignment_associations_deletes",
        "edfi_staff_section_associations_deletes",
        "edfi_student_parent_associations_deletes",
        "edfi_student_school_attendance_events_deletes",
        "edfi_student_section_associations_deletes",
        "edfi_student_section_attendance_events_deletes",
        "edfi_sessions_deletes",
        "edfi_cohort_type_descriptors_deletes",
        "edfi_disability_descriptors_deletes",
        "edfi_language_descriptors_deletes",
        "edfi_language_use_descriptors_deletes",
        "edfi_race_descriptors_deletes"
    ]
%}


{% for table in tables %}

    SELECT
        JSON_VALUE(data, '$.Id') AS id,
        JSON_VALUE(data, '$.ChangeVersion') AS change_version,
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
    FROM {{ source('raw_sources', table) }}
    {% if not loop.last %} UNION ALL {% endif %}

{% endfor %}
