


SELECT
    to_hex(md5(cast(coalesce(cast(schools.school_id as 
    string
), '') || '-' || coalesce(cast(schools.school_year as 
    string
), '') as 
    string
)))                                   AS school_key,
    to_hex(md5(cast(coalesce(cast(leas.local_education_agency_id as 
    string
), '') || '-' || coalesce(cast(leas.school_year as 
    string
), '') as 
    string
)))                                   AS local_education_agency_key,
    schools.school_year                     AS school_year,
    schools.school_id                       AS school_id,
    schools.name_of_institution             AS school_name,
    schools.school_type_descriptor          AS school_type,
    leas.name_of_institution                AS local_education_agency_name
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_schools` schools
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_local_education_agencies` leas
    ON schools.school_year = leas.school_year
    AND leas.local_education_agency_id = schools.local_education_agency_id