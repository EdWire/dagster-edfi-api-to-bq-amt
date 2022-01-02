
SELECT
    CONCAT(
        edfi_student_sped_associations.student_reference.student_unique_id, "-",
        edfi_student_school_associations.school_reference.school_id, "-",
        edfi_programs.program_name, "-",
        edfi_programs.program_type_descriptor, "-",
        edfi_programs.education_organization_reference.education_organization_id, "-",
        FORMAT_DATE('%Y%m%d', edfi_student_sped_associations.begin_date)
    )                                                                           AS student_school_program_key,
    FORMAT_DATE('%Y%m%d', edfi_student_sped_associations.begin_date)            AS begin_date_key,
    edfi_programs.education_organization_reference.education_organization_id    AS education_organization_id,
    edfi_programs.program_name                                                  AS program_name,
    edfi_student_sped_associations.student_reference.student_unique_id          AS student_key,
    CONCAT(
        edfi_student_sped_associations.student_reference.student_unique_id, "-",
        edfi_student_school_associations.school_reference.school_id
    ) AS student_school_key
FROM {{ ref('edfi_student_special_education_program_associations') }} edfi_student_sped_associations
LEFT JOIN {{ ref('edfi_programs') }} edfi_programs
    ON edfi_student_sped_associations.school_year = edfi_programs.school_year
    AND edfi_student_sped_associations.program_reference.program_type_descriptor = edfi_programs.program_type_descriptor
    AND edfi_student_sped_associations.program_reference.education_organization_id = edfi_programs.education_organization_reference.education_organization_id
LEFT JOIN {{ ref('edfi_student_school_associations') }} edfi_student_school_associations
    ON edfi_student_sped_associations.student_reference.student_unique_id = edfi_student_school_associations.student_reference.student_unique_id
