
WITH demographics AS (
    SELECT
        CONCAT('CohortYear:',
            cohort_years.school_year, '-',
            cohort_years.cohort_type_descriptor, '-', 
            seoa.student_unique_id, '-',
            seoa.education_organization_id
        ) AS student_school_demographic_bridge_key,
        CONCAT(seoa.student_unique_id, '-', seoa.education_organization_id) AS student_local_education_agency_key,
        CONCAT('CohortYear:',
            cohort_years.school_year, '-',
            cohort_years.cohort_type_descriptor
        ) AS demographic_key, 
        seoa.education_organization_id,
        seoa.student_unique_id
    FROM {{ ref('edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.cohort_years) AS cohort_years

    UNION ALL

    SELECT
        CONCAT('LanguageUse:', '-',
            uses.language_use_descriptor, '-', 
            seoa.student_unique_id, '-',
            seoa.education_organization_id
        ) AS student_school_demographic_bridge_key,
        CONCAT(seoa.student_unique_id, '-', seoa.education_organization_id) AS student_local_education_agency_key,
        CONCAT('LanguageUse:',
            uses.language_use_descriptor
        ) AS demographic_key,
        seoa.education_organization_id,
        seoa.student_unique_id
    FROM {{ ref('edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.languages) AS languages
    CROSS JOIN UNNEST(languages.uses) AS uses

    UNION ALL

    SELECT
        CONCAT('Language:', '-',
            languages.language_descriptor, '-', 
            seoa.student_unique_id, '-',
            seoa.education_organization_id
        ) AS student_school_demographic_bridge_key,
        CONCAT(seoa.student_unique_id, '-', seoa.education_organization_id) AS student_local_education_agency_key,
        CONCAT('Language:',
            languages.language_descriptor
        ) AS demographic_key,
        seoa.education_organization_id,
        seoa.student_unique_id
    FROM {{ ref('edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.languages) AS languages

    UNION ALL

    SELECT
        CONCAT('Race:', '-',
            races.race_descriptor, '-', 
            seoa.student_unique_id, '-',
            seoa.education_organization_id
        ) AS student_school_demographic_bridge_key,
        CONCAT(seoa.student_unique_id, '-', seoa.education_organization_id) AS student_local_education_agency_key,
        CONCAT('Race:',
            races.race_descriptor
        ) AS demographic_key,
        seoa.education_organization_id,
        seoa.student_unique_id
    FROM {{ ref('edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.races) AS races

)

SELECT
    student_school_demographic_bridge_key,
    student_local_education_agency_key,
    demographic_key
FROM demographics 
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('edfi_student_school_associations') }} ssa
    LEFT JOIN {{ ref('edfi_schools') }} schools ON ssa.school_reference.school_id = schools.school_id
    WHERE
        (ssa.exit_withdraw_date IS NULL OR ssa.exit_withdraw_date >= CURRENT_DATE)
        AND schools.local_education_agency_id = demographics.education_organization_id
        AND ssa.student_reference.student_unique_id = demographics.student_unique_id
)

