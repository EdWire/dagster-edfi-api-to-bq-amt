


SELECT
    to_hex(md5(cast(coalesce(cast(ssa.student_reference.student_unique_id as 
    string
), '') as 
    string
)))                                           AS student_key,
    to_hex(md5(cast(coalesce(cast(ssa.section_reference.school_id as 
    string
), '') as 
    string
)))                                           AS school_key,
    to_hex(md5(cast(coalesce(cast(sections.id as 
    string
), '') as 
    string
)))                                          AS section_id,
    ssa.begin_date,
    ssa.end_date
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_student_section_associations` ssa
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_sections` sections
    ON ssa.school_year = sections.school_year
    AND sections.course_offering_reference.local_course_code = ssa.section_reference.local_course_code
    AND sections.course_offering_reference.school_id = ssa.section_reference.school_id
    AND sections.course_offering_reference.school_year = ssa.section_reference.school_year
    AND sections.section_identifier = ssa.section_reference.section_identifier
    AND sections.course_offering_reference.session_name = ssa.section_reference.session_name