
SELECT
    seoa.student_reference.student_unique_id || '-' || seoa.education_organization_reference.education_organization_id AS student_local_education_agency_key,
    seoa.student_reference.student_unique_id AS student_key,
    seoa.education_organization_reference.education_organization_id AS local_education_agency_key,
    students.first_name AS student_first_name,
    students.middle_name AS student_middle_name,
    students.last_surname AS student_last_surname,
    COALESCE(seoa.limited_english_proficiency_descriptor, 'Not applicable') AS limited_english_proficiency,
    COALESCE(seoa.hispanic_latino_ethnicity, FALSE) AS is_hispanic,
    seoa.sex_descriptor AS sex,
    COALESCE(
        (SELECT indicator FROM UNNEST(seoa.student_indicators) WHERE name = 'Internet Access In Residence'),
        'n/a'
    ) AS internet_access_in_residence
FROM {{ ref('edfi_student_education_organization_associations') }} seoa
LEFT JOIN {{ ref('edfi_students') }} students
    ON seoa.school_year = students.school_year
    AND seoa.student_reference.student_unique_id = students.student_unique_id
