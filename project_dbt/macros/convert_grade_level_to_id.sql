-- this macro converts grade level descriptors
-- to a numeric value for use in sorting in
-- a bi tool
{% macro convert_grade_level_to_id(grade_level) %}
    CASE
        WHEN {{ grade_level }} = 'Infant/toddler' THEN -3
        WHEN {{ grade_level }} = 'Preschool/Prekindergarten' THEN -2
        WHEN {{ grade_level }} = 'Kindergarten' THEN -1
        WHEN {{ grade_level }} = 'First grade' THEN 1
        WHEN {{ grade_level }} = 'Second grade' THEN 2
        WHEN {{ grade_level }} = 'Third grade' THEN 3
        WHEN {{ grade_level }} = 'Fourth grade' THEN 4
        WHEN {{ grade_level }} = 'Fifth grade' THEN 5
        WHEN {{ grade_level }} = 'Sixth grade' THEN 6
        WHEN {{ grade_level }} = 'Seventh grade' THEN 7
        WHEN {{ grade_level }} = 'Eighth grade' THEN 8
        WHEN {{ grade_level }} = 'Ninth grade' THEN 9
        WHEN {{ grade_level }} = 'Tenth grade' THEN 10
        WHEN {{ grade_level }} = 'Eleventh grade' THEN 11
        WHEN {{ grade_level }} = 'Twelfth grade' THEN 12
        ELSE 999999999
    END
{% endmacro %}
