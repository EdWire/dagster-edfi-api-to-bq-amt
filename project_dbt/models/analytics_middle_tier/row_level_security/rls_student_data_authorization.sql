
SELECT
    ssa.student_reference.student_unique_id AS student_key,
    ssa.section_reference.school_id AS school_key,
    sections.id AS section_id,
    ssa.begin_date,
    ssa.end_date
FROM {{ ref('edfi_student_section_associations') }} ssa
LEFT JOIN {{ ref('edfi_sections') }} sections
    ON sections.course_offering_reference.local_course_code = ssa.section_reference.local_course_code
    AND sections.course_offering_reference.school_id = ssa.section_reference.school_id
    AND sections.course_offering_reference.school_year = ssa.section_reference.school_year
    AND sections.section_identifier = ssa.section_reference.section_identifier
    AND sections.course_offering_reference.session_name = ssa.section_reference.session_name
