
WITH enrollments_ranked AS (

    SELECT
        dim_student_school.student_key                                  AS student_key,
        dim_student_local_education_agency.local_education_agency_key   AS local_education_agency_key,
        dim_school.school_key                                           AS school_key,
        dim_student_school.school_year                                  AS school_year,
        dim_student_school.student_unique_id                            As student_unique_id,
        dim_student_school.student_last_surname                         AS student_last_surname,
        dim_student_school.student_first_name                           AS student_first_name,
        CONCAT(
            dim_student_school.student_last_surname, ", ",
            dim_student_school.student_first_name, " ",
            COALESCE(LEFT(dim_student_school.student_middle_name, 1), "")
        )                                                               AS student_display_name,
        enrollment_date.date                                            AS school_enrollment_date,
        exit_date.date                                                  AS school_exit_date,
        dim_student_school.is_enrolled                                  AS is_enrolled_at_school,
        {{ convert_grade_level('dim_student_school.grade_level') }}     AS grade_level,
        dim_student_school.sex                                          AS gender,
        dim_student_school.limited_english_proficiency                  AS limited_english_proficiency,
        IF(
            dim_student_school.limited_english_proficiency = "Limited",
            "Yes",
            "No"
        )                                                               AS is_english_language_learner,
        IF (
            dim_student_program.program_name IS NOT NULL,
            "Yes",
            "No"
        )                                                               AS in_special_education_program,
        IF(
            dim_student_school.is_hispanic IS TRUE,
            "Yes",
            "No"
        )                                                               AS is_hispanic,
        dim_demographic.demographic_label                               AS race,
        IF(
            dim_student_school.is_hispanic IS TRUE,
            "Hispanic or Latino",
            dim_demographic.demographic_label
        )                                                               AS race_and_ethnicity_roll_up,
        ROW_NUMBER() OVER (
            PARTITION BY
                dim_school.school_key,
                dim_student_school.student_key
            ORDER BY dim_school.school_key DESC, dim_student_school.student_key DESC, enrollment_date.date DESC
        ) AS rank,
    FROM {{ ref('dim_student_school') }} dim_student_school
    LEFT JOIN {{ ref('dim_school') }} dim_school
        ON dim_student_school.school_key = dim_school.school_key
    LEFT JOIN {{ ref('dim_date') }} enrollment_date
        ON dim_student_school.enrollment_date = enrollment_date.date
    LEFT JOIN {{ ref('dim_date') }} exit_date
        ON dim_student_school.exit_date = exit_date.date
    LEFT JOIN {{ ref('dim_student_local_education_agency') }} dim_student_local_education_agency
        ON dim_student_school.student_key = dim_student_local_education_agency.student_key
        AND dim_school.local_education_agency_key = dim_student_local_education_agency.local_education_agency_key
    LEFT JOIN {{ ref('student_local_education_agency_demographics_bridge') }} student_local_education_agency_demographics_bridge
        ON dim_student_school.school_year = student_local_education_agency_demographics_bridge.school_year
        AND dim_student_local_education_agency.student_local_education_agency_key = student_local_education_agency_demographics_bridge.student_local_education_agency_key
    LEFT JOIN {{ ref('dim_demographic') }} dim_demographic
        ON student_local_education_agency_demographics_bridge.demographic_key = dim_demographic.demographic_key
        AND dim_demographic.demographic_parent = "Race"
    LEFT JOIN {{ ref('dim_student_program') }} dim_student_program
        ON dim_student_school.student_school_key = dim_student_program.student_school_key
        AND dim_student_program.program_name = "Special Education"

)


SELECT * EXCEPT(rank)
FROM enrollments_ranked
WHERE rank = 1
