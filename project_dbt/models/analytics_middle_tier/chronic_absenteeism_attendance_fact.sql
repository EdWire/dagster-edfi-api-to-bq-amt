{{ config(
        materialized='table',
        schema='analytics_middle_tier',
    )
}}


SELECT
    CONCAT(ssa.student_reference.student_unique_id, '-',
        ssa.school_reference.school_id) AS student_school_key,
    ssa.student_reference.student_unique_id AS student_key,
    ssa.school_reference.school_id AS school_key,
    FORMAT_DATE('%Y%m%d', calendar_dates.date) AS date_key,
    MIN(school_attendance.attendance_event_category_descriptor) AS school_attendance_event_category_descriptor, --not in core amt
    school_attendance.event_duration, --not in core amt
    MAX(IF(school_attendance.attendance_event_category_descriptor = 'In Attendance', 1, 0)) AS reported_as_present_at_school,
    MAX(IF(
        school_attendance.attendance_event_category_descriptor IN ('Excused Absence', 'Unexcused Absence'), 1, 0
    )) AS reported_as_absent_from_school,
    MAX(IF(
        school_attendance.attendance_event_category_descriptor = 'In Attendance' 
            AND student_section_associations.homeroom_indicator IS TRUE,
        1, 0
    )) AS reported_as_present_at_home_room,
    MAX(IF(
        school_attendance.attendance_event_category_descriptor IN ('Excused Absence', 'Unexcused Absence') 
            AND student_section_associations.homeroom_indicator IS TRUE,
        1, 0
    )) AS reported_as_absent_from_home_room,
    NULL AS reported_as_is_present_in_all_sections,
    NULL AS reported_as_absent_from_any_section
FROM {{ ref('edfi_student_school_associations') }} ssa
LEFT JOIN {{ ref('edfi_students') }} students ON students.student_unique_id = ssa.student_reference.student_unique_id
LEFT JOIN {{ ref('edfi_calendar_dates') }} calendar_dates
    ON calendar_dates.calendar_reference.school_id = ssa.school_reference.school_id
    AND ssa.entry_date <= calendar_dates.date
    AND (
        ssa.exit_withdraw_date IS NULL
        OR ssa.exit_withdraw_date >= calendar_dates.date
    )
CROSS JOIN UNNEST(calendar_dates.calendar_events) AS calendar_events
-- school attendance
LEFT JOIN {{ ref('edfi_student_school_attendance_events') }} school_attendance
    ON school_attendance.student_reference.student_unique_id = ssa.student_reference.student_unique_id
    AND school_attendance.school_reference.school_id = ssa.school_reference.school_id
    AND (
		ssa.school_year_type_reference.school_year IS NULL
        OR 
		school_attendance.session_reference.school_year = ssa.school_year_type_reference.school_year
	)
    AND school_attendance.event_date = calendar_dates.date
-- section attendance
LEFT JOIN{{ ref('edfi_student_section_attendance_events') }} section_attendance
    ON section_attendance.student_reference.student_unique_id = ssa.student_reference.student_unique_id
    AND section_attendance.section_reference.school_id = ssa.school_reference.school_id
    AND section_attendance.event_date = calendar_dates.date
    AND (
		ssa.school_year_type_reference.school_year IS NULL
        OR 
		section_attendance.section_reference.school_year = ssa.school_year_type_reference.school_year
	)
LEFT JOIN {{ ref('edfi_student_section_associations') }} student_section_associations
    ON student_section_associations.student_reference.student_unique_id = section_attendance.student_reference.student_unique_id
    AND student_section_associations.section_reference.local_course_code = section_attendance.section_reference.local_course_code
    AND student_section_associations.section_reference.school_id = section_attendance.section_reference.school_id
    AND student_section_associations.section_reference.school_year = section_attendance.section_reference.school_year
    AND student_section_associations.section_reference.section_identifier = section_attendance.section_reference.section_identifier
    AND student_section_associations.section_reference.session_name = section_attendance.section_reference.session_name
WHERE
    calendar_dates.date <= CURRENT_DATE
    AND calendar_events.calendar_event_descriptor = 'Instructional day'
GROUP BY
    ssa.student_reference.student_unique_id,
    ssa.school_reference.school_id,
    calendar_dates.date,
    school_attendance.event_duration
