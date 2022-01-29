


SELECT
    to_hex(md5(cast(coalesce(cast(grades.student_section_association_reference.school_id as 
    string
), '') || '-' || coalesce(cast(grades.grading_period_reference.school_year as 
    string
), '') as 
    string
)))                                                                   AS school_key,
    to_hex(md5(cast(coalesce(cast(grades.student_section_association_reference.student_unique_id as 
    string
), '') || '-' || coalesce(cast(grades.grading_period_reference.school_year as 
    string
), '') as 
    string
)))                                                                   AS student_key,
    to_hex(md5(cast(coalesce(cast(grades.grading_period_reference.school_id as 
    string
), '') || '-' || coalesce(cast(grades.grading_period_reference.school_year as 
    string
), '') || '-' || coalesce(cast(grades.grading_period_reference.grading_period_descriptor as 
    string
), '') || '-' || coalesce(cast(grades.grading_period_reference.period_sequence as 
    string
), '') as 
    string
)))                                                                   AS grading_period_key,
    to_hex(md5(cast(coalesce(cast(student_section_association_reference.school_id as 
    string
), '') || '-' || coalesce(cast(grading_period_reference.school_year as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.session_name as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.local_course_code as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.section_identifier as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.student_unique_id as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.begin_date as 
    string
), '') as 
    string
)))                                                                   AS student_section_key,
    to_hex(md5(cast(coalesce(cast(student_section_association_reference.school_id as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.local_course_code as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.school_year as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.section_identifier as 
    string
), '') || '-' || coalesce(cast(student_section_association_reference.session_name as 
    string
), '') as 
    string
)))                                                                   AS section_key,
    grading_period_reference.school_year                                    AS school_year,
    numeric_grade_earned                                                    AS numeric_grade_earned,
    letter_grade_earned                                                     AS letter_grade_earned,
    grade_type_descriptor                                                   AS grade_type
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_grades` grades
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_descriptors` descriptors
    ON grades.school_year = descriptors.school_year
    AND descriptors.code_value = grading_period_reference.grading_period_descriptor