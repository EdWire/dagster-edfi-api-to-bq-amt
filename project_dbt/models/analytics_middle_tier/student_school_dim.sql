
SELECT
    CONCAT(ssa.student_reference.student_unique_id, '-',
        ssa.school_reference.school_id)  AS student_school_key,
    ssa.student_reference.student_unique_id AS student_key,
    ssa.school_reference.school_id AS school_key,
    IF(ssa.school_year_type_reference.school_year IS NULL, 'Unknown', ssa.school_year_type_reference.school_year) AS school_year,
    students.first_name AS student_first_name,
    students.middle_name AS student_middle_name,
    students.last_surname AS student_last_surname,
    ssa.entry_date AS enrollment_date_key,
    ssa.exit_withdraw_date AS exit_date_key, --not in core amt
    IF(
        ssa.exit_withdraw_date IS NULL
        OR CURRENT_DATE BETWEEN ssa.entry_date AND ssa.exit_withdraw_date,
        TRUE, FALSE) AS is_enrolled, --not in core amt
    ssa.entry_grade_level_descriptor AS grade_level,
    COALESCE(
        school_ed_org.limited_english_proficiency_descriptor,
        district_ed_org.limited_english_proficiency_descriptor,
        'Not applicable'
    ) AS limited_english_proficiency,
    COALESCE(
        school_ed_org.hispanic_latino_ethnicity,
        district_ed_org.hispanic_latino_ethnicity,
        FALSE
    ) AS is_hispanic,
    COALESCE(
        school_ed_org.sex_descriptor,
        district_ed_org.sex_descriptor
    ) AS sex,
    students.birth_date,
    COALESCE(
        (SELECT indicator FROM UNNEST(school_ed_org.student_indicators) WHERE name = 'Internet Access In Residence'),
        (SELECT indicator FROM UNNEST(district_ed_org.student_indicators) WHERE name = 'Internet Access In Residence'),
        'n/a'
    ) AS internet_access_in_residence
FROM {{ ref('edfi_student_school_associations') }} ssa
LEFT JOIN {{ ref('edfi_schools') }} schools ON schools.school_id = ssa.school_reference.school_id
LEFT JOIN {{ ref('edfi_students') }} students ON students.student_unique_id = ssa.student_reference.student_unique_id
LEFT JOIN {{ ref('edfi_student_education_organization_associations') }} school_ed_org 
    ON school_ed_org.student_reference.student_unique_id = ssa.student_reference.student_unique_id
    AND school_ed_org.education_organization_reference.education_organization_id = ssa.school_reference.school_id
LEFT JOIN {{ ref('edfi_student_education_organization_associations') }} district_ed_org 
    ON district_ed_org.student_reference.student_unique_id = ssa.student_reference.student_unique_id
    AND district_ed_org.education_organization_reference.education_organization_id = schools.local_education_agency_id
