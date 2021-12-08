
WITH currently_enrolled AS (
    SELECT DISTINCT 
        schools.local_education_agency_id,
        ssa.student_reference.student_unique_id
    FROM {{ ref('edfi_student_school_associations') }} ssa
    LEFT JOIN {{ ref('edfi_schools') }} schools ON ssa.school_reference.school_id = schools.school_id
    WHERE ssa.exit_withdraw_date IS NULL OR ssa.exit_withdraw_date >= CURRENT_DATE
)

SELECT
    seoa.student_unique_id || '-' || seoa.education_organization_id AS student_local_education_agency_key,
    seoa.student_unique_id AS student_key,
    seoa.education_organization_id AS local_education_agency_key,
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
FROM currently_enrolled
LEFT JOIN {{ ref('edfi_student_education_organization_associations') }} seoa
    ON seoa.education_organization_id = currently_enrolled.local_education_agency_id
    AND seoa.student_unique_id = currently_enrolled.student_unique_id
LEFT JOIN {{ ref('edfi_students') }} students
    ON students.student_unique_id = seoa.student_unique_id

