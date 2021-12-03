{{ config(
        materialized='table',
        schema='reporting',
    )
}}


WITH questions AS (
    SELECT
        questions.form_id,
        questions.question_id,
        questions.question_title,
        question_value
    FROM {{ ref('questions') }} questions
    CROSS JOIN UNNEST(questions.question_values) AS question_value
),

responses AS (
    SELECT
        responses.form_id,
        responses.response_id,
        responses.last_submitted,
        responses.respondent_email,
        response.question_id,
        response.question_response
    FROM {{ ref('responses') }} responses
    CROSS JOIN UNNEST(responses.responses) AS response
)

SELECT
    forms.form_title,
    questions.question_id,
    questions.question_title,
    questions.question_value,
    responses.respondent_email,
    responses.last_submitted,
    responses.question_response
FROM {{ ref('forms') }} forms
LEFT JOIN questions ON questions.form_id = forms.form_id
LEFT JOIN responses 
    ON responses.form_id = forms.form_id
    AND responses.question_id = questions.question_id
    AND (responses.question_response = questions.question_value OR questions.question_value = '')
