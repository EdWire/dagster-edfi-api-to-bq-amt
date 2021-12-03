{{ config(
        materialized='table',
        schema='google_forms',
    )
}}


SELECT
    id AS form_id,
    JSON_VALUE(data, '$.info.documentTitle') AS form_title
FROM {{ source('raw_sources', 'forms') }}
