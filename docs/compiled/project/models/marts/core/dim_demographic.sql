


SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(school_year_types.school_year as 
    string
), '') || '-' || coalesce(cast(descriptors.code_value as 
    string
), '') as 
    string
)))                                                                     AS demographic_key,
    'CohortYear'                                                              AS demographic_parent,
    CONCAT(school_year_types.school_year, '-', descriptors.short_description) AS demographic_label
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_school_year_types` school_year_types
CROSS JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_descriptors` descriptors
WHERE descriptors.namespace LIKE '%CohortTypeDescriptor'

UNION ALL

SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(descriptors.code_value as 
    string
), '') as 
    string
)))                                       AS demographic_key,
    'Language'                                  AS demographic_parent,
    descriptors.short_description               AS demographic_label
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_descriptors` descriptors
WHERE descriptors.namespace LIKE '%LanguageDescriptor'

UNION ALL

SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(descriptors.code_value as 
    string
), '') as 
    string
)))                                     AS demographic_key,
    'LanguageUse'                             AS demographic_parent,
    descriptors.short_description             AS demographic_label
FROM  `gcp-proj-id`.`dev_staging`.`stg_edfi_descriptors` descriptors
WHERE descriptors.namespace LIKE '%LanguageUseDescriptor'

UNION ALL

SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(descriptors.code_value as 
    string
), '') as 
    string
)))                                   AS demographic_key,
    'Race'                                  AS demographic_parent,
    descriptors.short_description           AS demographic_label
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_descriptors` descriptors
WHERE descriptors.namespace LIKE '%RaceDescriptor'