
SELECT
    fct_student_section_grade.school_year,
    dim_student.student_unique_id,
    dim_student.student_display_name,
    dim_student.grade_level,
    dim_student.is_enrolled_at_school,
    dim_student_section.local_course_code,
    dim_student_section.course_title,
    dim_student_section.academic_subject,
    dim_grading_period.grading_period_description,
    fct_student_section_grade.grade_type,
    fct_student_section_grade.numeric_grade_earned,
    fct_student_section_grade.letter_grade_earned,
    IF(
        CURRENT_DATE BETWEEN dim_student_section.student_section_start_date AND dim_student_section.student_section_end_date,
        "Yes",
        "No"
    ) AS is_currently_enrolled_in_section
FROM {{ ref('fct_student_section_grade') }} fct_student_section_grade
LEFT JOIN {{ ref('dim_student_section') }} dim_student_section
    ON fct_student_section_grade.student_section_key = dim_student_section.student_section_key
LEFT JOIN {{ ref('dim_grading_period') }} dim_grading_period
    ON fct_student_section_grade.grading_period_key = dim_grading_period.grading_period_key
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON fct_student_section_grade.school_key = dim_student.school_key
    AND fct_student_section_grade.student_key = dim_student.student_key
