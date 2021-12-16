
{%
    set tables = [{"table": "edfi_cohort_type_descriptors", "descriptorId": "cohortTypeDescriptorId"},
    {"table": "edfi_disability_descriptors", "descriptorId": "disabilityDescriptorId" },
    {"table": "edfi_language_descriptors", "descriptorId": "languageDescriptorId" }, 
    {"table": "edfi_language_use_descriptors", "descriptorId": "languageUseDescriptorId" }, 
    {"table": "edfi_race_descriptors", "descriptorId": "raceDescriptorId" }, 
    {"table": "edfi_grading_period_descriptors", "descriptorId": "gradingPeriodDescriptorId" }]
%}


WITH parsed_data AS (

    {% for table in tables %}
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '{{ "$." ~ table["descriptorId"] }}') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM {{ source('raw_sources', table['table']) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY namespace, code_value
            ORDER BY extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('edfi_deletes') }}
    )
