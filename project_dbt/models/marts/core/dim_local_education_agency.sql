{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'local_education_agency_id'
    ]) }}                               AS local_education_agency_key,
    name_of_institution                 AS local_education_agency_name
FROM {{ ref('stg_edfi_local_education_agencies') }}
