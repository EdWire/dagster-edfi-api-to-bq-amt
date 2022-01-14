
SELECT DISTINCT
    CONCAT('CohortYear:',
        school_year_types.school_year, '-',
        descriptors.code_value
    ) AS demographic_key,
    'CohortYear' AS demographic_parent_key,
    CONCAT(school_year_types.school_year, '-', descriptors.code_value) AS demographic_label
FROM {{ ref('stg_edfi_school_year_types') }} school_year_types
CROSS JOIN {{ ref('stg_edfi_descriptors') }} descriptors
WHERE descriptors.namespace LIKE '%CohortTypeDescriptor'

UNION ALL

SELECT DISTINCT
    CONCAT('Language:', descriptors.code_value) AS demographic_key,
    'Language' AS demographic_parent_key,
    descriptors.code_value AS demographic_label
FROM {{ ref('stg_edfi_descriptors') }} descriptors
WHERE descriptors.namespace LIKE '%LanguageDescriptor'

UNION ALL

SELECT DISTINCT
    CONCAT('LanguageUse:', descriptors.code_value) AS demographic_key,
    'LanguageUse' AS demographic_parent_key,
    descriptors.code_value AS demographic_label
FROM  {{ ref('stg_edfi_descriptors') }} descriptors
WHERE descriptors.namespace LIKE '%LanguageUseDescriptor'

UNION ALL

SELECT DISTINCT
    CONCAT('Race:', descriptors.code_value) AS demographic_key,
    'Race' AS demographic_parent_key,
    descriptors.code_value AS demographic_label
FROM {{ ref('stg_edfi_descriptors') }} descriptors
WHERE descriptors.namespace LIKE '%RaceDescriptor'
