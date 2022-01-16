{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    student_section_association_reference.student_unique_id AS student_key,
    student_section_association_reference.school_id AS school_key,
    CONCAT(descriptors.descriptor_id, '-',
           student_section_association_reference.school_id, '-',
           FORMAT_DATE('%Y%m%d', student_section_association_reference.begin_date)
    ) AS grading_period_key,
    CONCAT(student_section_association_reference.student_unique_id, '-',
           student_section_association_reference.school_id, '-',
           student_section_association_reference.local_course_code, '-',
           grading_period_reference.school_year, '-',
           student_section_association_reference.section_identifier, '-',
           student_section_association_reference.session_name, '-',
           FORMAT_DATE('%Y%m%d', student_section_association_reference.begin_date)
    ) AS student_section_key,
    CONCAT(student_section_association_reference.school_id, '-',
        student_section_association_reference.local_course_code, '-',
        student_section_association_reference.school_year, '-',
        student_section_association_reference.section_identifier, '-',
        student_section_association_reference.session_name, '-',
        FORMAT_DATE('%Y%m%d', student_section_association_reference.begin_date)
    ) AS section_key,
    numeric_grade_earned,
    letter_grade_earned,
    grade_type_descriptor AS grade_type
FROM {{ ref('stg_edfi_grades') }} grades
LEFT JOIN {{ ref('stg_edfi_descriptors') }} descriptors
    ON grades.school_year = descriptors.school_year
    AND descriptors.code_value = grading_period_reference.grading_period_descriptor
