
{% macro get_unweighted_gpa_point(letter_grade) %}
    CASE
        WHEN {{ letter_grade }} = 'A+' THEN 4.4
        WHEN {{ letter_grade }} = 'A' THEN 4
        WHEN {{ letter_grade }} = 'A-' THEN 3.7
        WHEN {{ letter_grade }} = 'B+' THEN 3.4
        WHEN {{ letter_grade }} = 'B' THEN 3
        WHEN {{ letter_grade }} = 'B-' THEN 2.7
        WHEN {{ letter_grade }} = 'C+' THEN 2.4
        WHEN {{ letter_grade }} = 'C' THEN 2
        WHEN {{ letter_grade }} = 'C-' THEN 1.7
        WHEN {{ letter_grade }} = 'D+' THEN 1.4
        WHEN {{ letter_grade }} = 'D' THEN 1.4
        WHEN {{ letter_grade }} = 'D-' THEN 0.6
        WHEN {{ letter_grade }} = 'F' THEN 0
        ELSE NULL
    END
{% endmacro %}
