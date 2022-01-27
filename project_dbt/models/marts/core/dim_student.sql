
WITH school_year_end_dates AS (

    SELECT
        school_year_type_reference.school_year,
        school_reference.school_id,
        MAX(end_date) AS school_year_end_date
    FROM {{ ref('stg_edfi_sessions') }}
    GROUP BY 1, 2


),

active_enrollments AS (

    SELECT
        student_school_associations.student_reference.student_unique_id            AS student_unique_id,
        student_school_associations.school_year_type_reference.school_year         AS school_year,
        'Yes'                                                                      AS is_actively_enrolled
    FROM {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    LEFT JOIN school_year_end_dates
        ON student_school_associations.school_year_type_reference.school_year = school_year_end_dates.school_year
        AND student_school_associations.school_reference.school_id = school_year_end_dates.school_id
    WHERE
        student_school_associations.exit_withdraw_date IS NULL
        OR (
            -- active enrollment for current year
            CURRENT_DATE >= student_school_associations.entry_date
            AND CURRENT_DATE < student_school_associations.exit_withdraw_date
        )
        -- if student exited a previous year on the final day of the session
        -- replace school_year_end_dates.school_year_end_date with DATE 'YYYY-MM-DD' if MAX session end date does
        -- not represent school year end date
        OR student_school_associations.exit_withdraw_date >= school_year_end_dates.school_year_end_date

),

student_grade_level_ranked AS (

    SELECT
        school_year_type_reference.school_year,
        student_reference.student_unique_id,
        {{ convert_grade_level_to_short_label('entry_grade_level_descriptor') }}     AS grade_level,
        {{ convert_grade_level_to_id('entry_grade_level_descriptor') }}              AS grade_level_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                student_reference.student_unique_id,
                school_year_type_reference.school_year
            ORDER BY
                school_year_type_reference.school_year DESC,
                student_reference.student_unique_id,
                entry_date DESC
        ) AS rank,
    FROM {{ ref('stg_edfi_student_school_associations') }}

),

student_grade_level AS (

    SELECT * FROM student_grade_level_ranked WHERE rank = 1

)

SELECT
    {{ dbt_utils.surrogate_key([
            'students.student_unique_id',
            'students.school_year'
    ]) }}                                                           AS student_key,
    students.school_year                                            AS school_year,
    students.student_unique_id                                      AS student_unique_id,
    students.first_name                                             AS student_first_name,
    students.middle_name                                            AS student_middle_name,
    students.last_surname                                           AS student_last_surname,
    CONCAT(
        students.last_surname, ', ',
        students.first_name, ' ',
        COALESCE(LEFT(students.middle_name, 1), '')
    )                                                               AS student_display_name,
    IFNULL(active_enrollments.is_actively_enrolled, 'No')           AS is_actively_enrolled,
    student_grade_level.grade_level                                 AS grade_level,
    student_grade_level.grade_level_id                              AS grade_level_id,
    COALESCE(
        seoa.limited_english_proficiency_descriptor,
        'Not applicable'
    )                                                               AS limited_english_proficiency,
    IF(
        seoa.limited_english_proficiency_descriptor = "Limited",
        "Yes",
        "No"
    )                                                               AS is_english_language_learner,
    IF (
        edfi_programs.program_name IS NOT NULL,
        "Yes",
        "No"
    )                                                               AS in_special_education_program,
    IF(seoa.hispanic_latino_ethnicity IS TRUE, 'Yes', 'No')         AS is_hispanic,
    CASE
        WHEN seoa.hispanic_latino_ethnicity IS TRUE THEN 'Hispanic or Latino'
        WHEN ARRAY_LENGTH(seoa.races) > 1 THEN 'Two or more races'
        WHEN EXISTS(SELECT * FROM UNNEST(races) AS race WHERE race.race_descriptor = 'Asian') THEN 'Asian'
        WHEN EXISTS(SELECT * FROM UNNEST(races) AS race WHERE race.race_descriptor = 'White') THEN 'White'
        WHEN EXISTS(SELECT * FROM UNNEST(races) AS race WHERE race.race_descriptor = 'Choose Not to Respond') THEN 'Choose Not to Respond'
        WHEN EXISTS(SELECT * FROM UNNEST(races) AS race WHERE race.race_descriptor = 'American Indian - Alaska Native') THEN 'American Indian - Alaska Native'
        WHEN EXISTS(SELECT * FROM UNNEST(races) AS race WHERE race.race_descriptor = 'Black - African American') THEN 'Black - African American'
        WHEN EXISTS(SELECT * FROM UNNEST(races) AS race WHERE race.race_descriptor = 'Native Hawaiian - Pacific Islander') THEN 'Native Hawaiian - Pacific Islander'
        ELSE 'Unknown'
    END                                                             AS race_and_ethnicity_roll_up,
    seoa.sex_descriptor                                             AS gender,
    students.birth_date                                             AS birth_date
FROM {{ ref('stg_edfi_students') }} students
LEFT JOIN {{ ref('stg_edfi_student_education_organization_associations') }} seoa 
    ON students.student_unique_id = seoa.student_reference.student_unique_id
    AND students.school_year = seoa.school_year
LEFT JOIN {{ ref('stg_edfi_student_special_education_program_associations') }} edfi_student_sped_associations
    ON students.school_year = edfi_student_sped_associations.school_year
    AND seoa.education_organization_reference.education_organization_id = edfi_student_sped_associations.program_reference.education_organization_id
    AND students.student_unique_id = edfi_student_sped_associations.student_reference.student_unique_id
LEFT JOIN {{ ref('stg_edfi_programs') }} edfi_programs
    ON edfi_student_sped_associations.school_year = edfi_programs.school_year
    AND edfi_student_sped_associations.program_reference.program_type_descriptor = edfi_programs.program_type_descriptor
    AND edfi_student_sped_associations.program_reference.education_organization_id = edfi_programs.education_organization_reference.education_organization_id
    AND edfi_programs.program_name = "Special Education"
LEFT JOIN active_enrollments
    ON students.student_unique_id = active_enrollments.student_unique_id
    AND students.school_year = active_enrollments.school_year
LEFT JOIN student_grade_level
    ON students.student_unique_id = student_grade_level.student_unique_id
    AND students.school_year = student_grade_level.school_year
