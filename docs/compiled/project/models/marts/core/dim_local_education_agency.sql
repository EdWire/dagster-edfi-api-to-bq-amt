


SELECT
    to_hex(md5(cast(coalesce(cast(local_education_agency_id as 
    string
), '') || '-' || coalesce(cast(school_year as 
    string
), '') as 
    string
)))                               AS local_education_agency_key,
    school_year                         AS school_year,
    local_education_agency_id           AS local_education_agency_id,
    name_of_institution                 AS local_education_agency_name
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_local_education_agencies`