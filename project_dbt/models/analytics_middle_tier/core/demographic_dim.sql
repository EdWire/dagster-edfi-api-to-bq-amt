
SELECT
    CONCAT('CohortYear:',
        school_year_types.school_year, '-',
        descriptors.code_value
    ) AS demographic_key,
    'CohortYear' AS demographic_parent_key,
    CONCAT(school_year_types.school_year, '-', descriptors.code_value) AS demographic_label
FROM {{ ref('edfi_school_year_types') }} school_year_types
CROSS JOIN {{ ref('edfi_descriptors') }} descriptors
WHERE descriptors.namespace = 'uri://ed-fi.org/CohortTypeDescriptor'

UNION ALL

SELECT
    CONCAT('Language:', descriptors.code_value) AS demographic_key,
    'Language' AS demographic_parent_key,
    descriptors.code_value AS demographic_label
FROM {{ ref('edfi_descriptors') }} descriptors
WHERE descriptors.namespace = 'uri://ed-fi.org/LanguageDescriptor'

UNION ALL

SELECT
    CONCAT('LanguageUse:', descriptors.code_value) AS demographic_key,
    'LanguageUse' AS demographic_parent_key,
    descriptors.code_value AS demographic_label
FROM  {{ ref('edfi_descriptors') }} descriptors
WHERE descriptors.namespace = 'uri://ed-fi.org/LanguageUseDescriptor'

UNION ALL

SELECT
    CONCAT('Race:', descriptors.code_value) AS demographic_key,
    'Race' AS demographic_parent_key,
    descriptors.code_value AS demographic_label
FROM {{ ref('edfi_descriptors') }} descriptors
WHERE descriptors.namespace = 'uri://ed-fi.org/RaceDescriptor'
