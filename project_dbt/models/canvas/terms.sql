{{ config(
        materialized='table',
        schema='canvas',
    )
}}


SELECT
    CAST(JSON_EXTRACT_SCALAR(terms.data, '$.id') AS int64) AS id,
    JSON_EXTRACT_SCALAR(terms.data, '$.name') AS name,
    CAST(JSON_EXTRACT_SCALAR(terms.data, '$.start_at') AS TIMESTAMP) AS start_at,
    CAST(JSON_EXTRACT_SCALAR(terms.data, '$.end') AS TIMESTAMP) AS end_at,
    JSON_EXTRACT_SCALAR(terms.data, '$.workflow_state') AS workflow_state
FROM {{ source('raw_canvas', 'terms') }} 
