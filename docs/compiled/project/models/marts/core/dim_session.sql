SELECT
    to_hex(md5(cast(coalesce(cast(sessions.school_reference.school_id as 
    string
), '') || '-' || coalesce(cast(sessions.school_year_type_reference.school_year as 
    string
), '') || '-' || coalesce(cast(sessions.session_name as 
    string
), '') as 
    string
)))                                                                                   AS session_key,
    to_hex(md5(cast(coalesce(cast(sessions.school_reference.school_id as 
    string
), '') || '-' || coalesce(cast(sessions.school_year_type_reference.school_year as 
    string
), '') as 
    string
)))                                                                                   AS school_key,
    sessions.school_year_type_reference.school_year                                         AS school_year,
    school_year_types.school_year_description                                               AS school_year_name,
    sessions.session_name                                                                   AS session_name,
    sessions.term_descriptor                                                                AS term_name,
    sessions.total_instructional_days                                                       AS total_instructional_days,
    sessions.begin_date                                                                     AS session_begin_date,
    sessions.end_date                                                                       AS session_end_date
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_sessions` sessions
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_school_year_types` school_year_types
    ON sessions.school_year_type_reference.school_year = school_year_types.school_year