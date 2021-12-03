{% macro normalize_school_name(school_name) %}
    CASE
        WHEN {{ school_name }} LIKE '%Middle%' THEN 'Belmont MS'
        WHEN {{ school_name }} LIKE '%Downtown%' THEN 'Downtown HS'
        WHEN {{ school_name }} LIKE '%Belmont%' THEN 'Belmont HS'
        ELSE {{ school_name }} 
    END
{% endmacro %}
