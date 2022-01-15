{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT DISTINCT
    schools.school_id AS school_key,
    schools.name_of_institution AS school_name,
    schools.school_type_descriptor AS school_type,
    schools.local_education_agency_id AS local_education_agency_key,
    leas.name_of_institution AS local_education_agency_name
FROM {{ ref('stg_edfi_schools') }} schools
LEFT JOIN {{ ref('stg_edfi_local_education_agencies') }} leas
    ON schools.school_year = leas.school_year
    AND leas.local_education_agency_id = schools.local_education_agency_id
