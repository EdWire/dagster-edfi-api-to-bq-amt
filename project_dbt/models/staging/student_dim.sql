
WITH enrollments_ranked AS (

    SELECT
        student_school_dim.school_year                      AS school_year,
        school_dim.local_education_agency_name              AS local_education_agency_name,
        school_dim.school_key                               AS school_key,
        school_dim.school_name                              AS school_name,
        student_school_dim.student_key                      AS student_key,
        student_school_dim.student_last_surname             AS student_last_surname,
        student_school_dim.student_first_name               AS student_first_name,
        CONCAT(
            student_school_dim.student_last_surname, ", ",
            student_school_dim.student_first_name, " ",
            COALESCE(LEFT(student_school_dim.student_middle_name, 1), "")
        )                                                   AS student_display_name,
        enrollment_date.date                                AS enrollment_date,
        exit_date.date                                      AS exit_date,
        IF(
            student_school_dim.is_enrolled IS TRUE,
            "Yes",
            "No"
        )                                                   AS is_enrolled,
        {{ convert_grade_level('student_school_dim.grade_level') }} AS grade_level,
        student_school_dim.sex                              AS gender,
        student_school_dim.limited_english_proficiency      AS limited_english_proficiency,
        IF(
            student_school_dim.limited_english_proficiency = "Limited",
            "Yes",
            "No"
        )                                                   AS is_english_language_learner,
        IF(
            student_school_dim.is_hispanic IS TRUE,
            "Yes",
            "No"
        )                                                   AS is_hispanic,
        demographic_dim.demographic_label                   AS race,
        IF(
            student_school_dim.is_hispanic IS TRUE,
            "Hispanic or Latino",
            demographic_dim.demographic_label
        )                                                   AS race_and_ethnicity_roll_up,
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                school_dim.school_key
            ORDER BY school_year DESC, enrollment_date.date DESC
        ) AS rank,
    FROM {{ ref('student_school_dim') }} student_school_dim
    LEFT JOIN {{ ref('school_dim') }} school_dim
        ON student_school_dim.school_key = school_dim.school_key
    LEFT JOIN {{ ref('date_dim') }} enrollment_date
        ON student_school_dim.enrollment_date_key = enrollment_date.date_key
    LEFT JOIN {{ ref('date_dim') }} exit_date
        ON student_school_dim.exit_date_key = exit_date.date_key
    LEFT JOIN {{ ref('student_local_education_agency_dim') }} student_local_education_agency_dim
        ON student_school_dim.student_key = student_local_education_agency_dim.student_key
        AND school_dim.local_education_agency_key = student_local_education_agency_dim.local_education_agency_key
    LEFT JOIN {{ ref('student_local_education_agency_demographics_bridge') }} student_local_education_agency_demographics_bridge
        ON student_local_education_agency_dim.student_local_education_agency_key = student_local_education_agency_demographics_bridge.student_local_education_agency_key
    LEFT JOIN {{ ref('demographic_dim') }} demographic_dim
        ON student_local_education_agency_demographics_bridge.demographic_key = demographic_dim.demographic_key
        AND demographic_dim.demographic_parent_key = "Race"

)


SELECT *
FROM enrollments_ranked
WHERE rank = 1
