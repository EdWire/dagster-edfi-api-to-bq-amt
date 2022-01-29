WITH student_attendance AS (

    SELECT
        student_key                                     AS student_key,
        school_key                                      AS school_key,
        school_year                                     AS school_year,
        SUM(event_duration)                             AS sum_of_absences,
         -- alternative if event duration is not used 
        COUNT(1)                                        AS number_of_days_enrolled,
        (COUNT(1) - SUM(event_duration)) / COUNT(1)     AS average_daily_attendance
    FROM `gcp-proj-id`.`dev_attendance`.`fct_student_attendance`
    GROUP BY 1, 2, 3

)

SELECT
    student_attendance.school_year                                                     AS school_year,
    student_attendance.school_key                                                      AS school_key,
    student_attendance.student_key                                                     AS student_key,
    IF(student_attendance.sum_of_absences >= 15, 1, 0)                                 AS is_chronically_absent,
    IF(student_attendance.average_daily_attendance < 0.92, 1, 0)                       AS early_warning,
    dim_local_education_agency.local_education_agency_name                             AS local_education_agency_name,
    dim_school.school_name                                      AS school_name,
    dim_student.student_last_surname                            AS student_last_surname,
    dim_student.student_first_name                              AS student_first_name,
    dim_student.student_display_name                            AS student_display_name,
    dim_student.is_actively_enrolled                            AS is_actively_enrolled,
    dim_student.grade_level                                     AS grade_level,
    dim_student.gender                                          AS gender,
    dim_student.limited_english_proficiency                     AS limited_english_proficiency,
    dim_student.is_english_language_learner                     AS is_english_language_learner,
    dim_student.in_special_education_program                    AS in_special_education_program,
    dim_student.is_hispanic                                     AS is_hispanic,
    dim_student.race_and_ethnicity_roll_up                      AS race_and_ethnicity_roll_up
FROM student_attendance
LEFT JOIN `gcp-proj-id`.`dev_core`.`dim_student` dim_student
    ON student_attendance.student_key = dim_student.student_key
LEFT JOIN `gcp-proj-id`.`dev_core`.`dim_school` dim_school
    ON student_attendance.school_key = dim_school.school_key
LEFT JOIN `gcp-proj-id`.`dev_core`.`dim_local_education_agency` dim_local_education_agency
    ON dim_school.local_education_agency_key = dim_local_education_agency.local_education_agency_key