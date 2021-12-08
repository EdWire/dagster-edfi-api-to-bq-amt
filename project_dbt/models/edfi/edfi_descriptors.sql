

{% set tables = ["edfi_cohort_type_descriptors", "edfi_disability_descriptors", "edfi_language_descriptors", 
                 "edfi_language_use_descriptors", "edfi_race_descriptors"]
%}


{% for table in tables %}
    SELECT
        JSON_VALUE(data, '$.codeValue') AS code_value,
        JSON_VALUE(data, '$.description') AS description,
        JSON_VALUE(data, '$.namespace') AS namespace,
        JSON_VALUE(data, '$.shortDescription') AS short_description
    FROM {{ source('raw_sources', table) }}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
