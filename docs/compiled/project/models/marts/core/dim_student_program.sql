


SELECT
    to_hex(md5(cast(coalesce(cast(edfi_student_sped_associations.student_reference.student_unique_id as 
    string
), '') || '-' || coalesce(cast(edfi_student_sped_associations.school_year as 
    string
), '') || '-' || coalesce(cast(edfi_programs.program_name as 
    string
), '') || '-' || coalesce(cast(edfi_programs.program_type_descriptor as 
    string
), '') || '-' || coalesce(cast(edfi_programs.education_organization_reference.education_organization_id as 
    string
), '') || '-' || coalesce(cast(edfi_student_sped_associations.begin_date as 
    string
), '') as 
    string
)))                                                                       AS student_program_key,
    to_hex(md5(cast(coalesce(cast(edfi_programs.education_organization_reference.education_organization_id as 
    string
), '') || '-' || coalesce(cast(edfi_student_sped_associations.school_year as 
    string
), '') as 
    string
)))                                                                       AS education_organization_key,
    to_hex(md5(cast(coalesce(cast(edfi_student_sped_associations.student_reference.student_unique_id as 
    string
), '') || '-' || coalesce(cast(edfi_student_sped_associations.school_year as 
    string
), '') as 
    string
)))                                                                       AS student_key,
    edfi_student_sped_associations.school_year                                  AS school_year,
    edfi_student_sped_associations.begin_date                                   AS begin_date,
    edfi_student_sped_associations.end_date                                     AS end_date,
    edfi_programs.program_name                                                  AS program_name
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_student_special_education_program_associations` edfi_student_sped_associations
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_programs` edfi_programs
    ON edfi_student_sped_associations.school_year = edfi_programs.school_year
    AND edfi_student_sped_associations.program_reference.program_type_descriptor = edfi_programs.program_type_descriptor
    AND edfi_student_sped_associations.program_reference.education_organization_id = edfi_programs.education_organization_reference.education_organization_id