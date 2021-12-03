{{ config(
        materialized='table',
        schema='analytics_middle_tier',
    )
}}


WITH teachers AS (
    SELECT
        ssa.section_reference.school_id,
        ssa.section_reference.local_course_code,
        ssa.section_reference.school_year,
        ssa.section_reference.section_identifier,
        ssa.section_reference.session_name,
        STRING_AGG(staff.first_name || ' ' || staff.last_surname, ', ') AS teachers
    FROM {{ ref('edfi_staff_section_associations') }} ssa
    LEFT JOIN {{ ref('edfi_staffs') }} staff ON staff.staff_unique_id = ssa.staff_reference.staff_unique_id
    GROUP BY ssa.section_reference.school_id,
        ssa.section_reference.local_course_code,
        ssa.section_reference.school_year,
        ssa.section_reference.section_identifier,
        ssa.section_reference.session_name
)

SELECT
    CONCAT(student_reference.student_unique_id, '-',
        section_reference.school_id, '-',
        section_reference.local_course_code, '-',
        section_reference.school_year, '-',
        section_reference.section_identifier, '-',
        section_reference.session_name, '-',
        FORMAT_DATE('%Y%m%d', begin_date)
    ) AS student_section_key,
    student_reference.student_unique_id AS student_key,
    CONCAT(section_reference.school_id, '-',
        section_reference.local_course_code, '-',
        section_reference.school_year, '-',
        section_reference.section_identifier, '-',
        section_reference.session_name, '-',
        FORMAT_DATE('%Y%m%d', begin_date)
    ) AS section_key,
    section_reference.local_course_code,
    courses.academic_subject_descriptor AS subject,
    courses.course_title,
    teachers.teachers AS teacher_name,
    FORMAT_DATE('%Y%m%d', ssa.begin_date) AS student_section_start_date_key,
    FORMAT_DATE('%Y%m%d', ssa.end_date) AS student_section_end_date_key,
    section_reference.school_id AS school_key,
    section_reference.school_year
FROM {{ ref('edfi_student_section_associations') }} ssa
LEFT JOIN {{ ref('edfi_course_offerings') }} course_offerings
    ON course_offerings.local_course_code = ssa.section_reference.local_course_code
    AND course_offerings.school_reference.school_id = ssa.section_reference.school_id
    AND course_offerings.session_reference.school_year = ssa.section_reference.school_year
    AND course_offerings.session_reference.session_name = ssa.section_reference.session_name
LEFT JOIN {{ ref('edfi_courses') }} courses
    ON courses.course_code = course_offerings.course_reference.course_code
    AND courses.education_organization_reference.education_organization_id = course_offerings.course_reference.education_organization_id
LEFT JOIN teachers
    ON teachers.local_course_code = ssa.section_reference.local_course_code
    AND teachers.school_year = ssa.section_reference.school_year
    AND teachers.section_identifier = ssa.section_reference.section_identifier
    AND teachers.session_name = ssa.section_reference.session_name
