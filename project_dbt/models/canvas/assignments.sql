{{ config(
        materialized='table',
        schema='canvas',
    )
}}


SELECT
    CAST(JSON_EXTRACT_SCALAR(assignments.data, '$.id') AS int64) AS id,
    CAST(JSON_EXTRACT_SCALAR(assignments.data, '$.course_id') AS int64) AS course_id,
    JSON_EXTRACT_SCALAR(assignments.data, '$.name') AS name,
    JSON_EXTRACT_SCALAR(assignments.data, '$.description') AS description,
    CAST(JSON_EXTRACT_SCALAR(assignments.data, '$.due_at') AS TIMESTAMP) AS due_at,
    CAST(JSON_EXTRACT_SCALAR(assignments.data, '$.points_possible') AS float64) AS points_possible,
    JSON_EXTRACT_SCALAR(assignments.data, '$.workflow_state') AS workflow_state
FROM {{ source('raw_canvas', 'assignments') }} assignments
