{{ config(
        materialized='table',
        schema='edfi',
    )
}}


SELECT
    JSON_VALUE(data, '$.studentUniqueId') AS student_unique_id,
    JSON_VALUE(data, '$.lastSurname') AS last_surname,
    JSON_VALUE(data, '$.middleName') AS middle_name,
    JSON_VALUE(data, '$.firstName') AS first_name,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.birthDate')) AS birth_date, 
FROM {{ source('raw_sources', 'edfi_students') }}
