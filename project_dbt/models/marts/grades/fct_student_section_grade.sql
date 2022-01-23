{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.school_id'
    ]) }}                                                                   AS school_key,
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.student_unique_id'
    ]) }}                                                                   AS student_key,
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.school_id',
        'descriptors.code_value',
        'grades.student_section_association_reference.begin_date'
    ]) }}                                                                   AS grading_period_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'grading_period_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier',
        'student_section_association_reference.student_unique_id',
        'student_section_association_reference.begin_date'
    ]) }}                                                                   AS student_section_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.school_year',
        'student_section_association_reference.section_identifier',
        'student_section_association_reference.session_name'
    ]) }}                                                                   AS section_key,
    grading_period_reference.school_year                                    AS school_year,
    numeric_grade_earned                                                    AS numeric_grade_earned,
    letter_grade_earned                                                     AS letter_grade_earned,
    grade_type_descriptor                                                   AS grade_type
FROM {{ ref('stg_edfi_grades') }} grades
LEFT JOIN {{ ref('stg_edfi_descriptors') }} descriptors
    ON grades.school_year = descriptors.school_year
    AND descriptors.code_value = grading_period_reference.grading_period_descriptor
