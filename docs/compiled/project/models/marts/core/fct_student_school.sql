SELECT
    to_hex(md5(cast(coalesce(cast(ssa.student_reference.student_unique_id as 
    string
), '') || '-' || coalesce(cast(ssa.school_year_type_reference.school_year as 
    string
), '') as 
    string
)))                                                           AS student_key,
    to_hex(md5(cast(coalesce(cast(schools.local_education_agency_id as 
    string
), '') || '-' || coalesce(cast(ssa.school_year_type_reference.school_year as 
    string
), '') as 
    string
)))                                                           AS local_education_agency_key,
    to_hex(md5(cast(coalesce(cast(ssa.school_reference.school_id as 
    string
), '') || '-' || coalesce(cast(ssa.school_year_type_reference.school_year as 
    string
), '') as 
    string
)))                                                           AS school_key,
    ssa.school_year_type_reference.school_year                      AS school_year,
    
    CASE ssa.entry_grade_level_descriptor
        WHEN 'Infant/toddler'            THEN 'Infant'
        WHEN 'Preschool/Prekindergarten' THEN 'PreK'
        WHEN 'Kindergarten'              THEN 'K'
        WHEN 'First grade'               THEN '1'
        WHEN 'Second grade'              THEN '2'
        WHEN 'Third grade'               THEN '3'
        WHEN 'Fourth grade'              THEN '4'
        WHEN 'Fifth grade'               THEN '5'
        WHEN 'Sixth grade'               THEN '6'
        WHEN 'Seventh grade'             THEN '7'
        WHEN 'Eighth grade'              THEN '8'
        WHEN 'Ninth grade'               THEN '9'
        WHEN 'Tenth grade'               THEN '10'
        WHEN 'Eleventh grade'            THEN '11'
        WHEN 'Twelfth grade'             THEN '12'
        ELSE '999999999'
    END
     AS grade_level,
    
    CASE ssa.entry_grade_level_descriptor
        WHEN 'Infant/toddler'            THEN -3
        WHEN 'Preschool/Prekindergarten' THEN -2
        WHEN 'Kindergarten'              THEN -1
        WHEN 'First grade'               THEN 1
        WHEN 'Second grade'              THEN 2
        WHEN 'Third grade'               THEN 3
        WHEN 'Fourth grade'              THEN 4
        WHEN 'Fifth grade'               THEN 5
        WHEN 'Sixth grade'               THEN 6
        WHEN 'Seventh grade'             THEN 7
        WHEN 'Eighth grade'              THEN 8
        WHEN 'Ninth grade'               THEN 9
        WHEN 'Tenth grade'               THEN 10
        WHEN 'Eleventh grade'            THEN 11
        WHEN 'Twelfth grade'             THEN 12
        ELSE 999999999
    END
              AS grade_level_id,
    ssa.entry_date                                                  AS enrollment_date,
    ssa.entry_type_descriptor                                       AS enrollment_type,
    ssa.exit_withdraw_date                                          AS exit_date,
    ssa.exit_withdraw_type_descriptor                               AS exit_type,
    ssa.primary_school                                              AS is_primary_school,
    COUNT(calendar_dates.date)                                      AS count_days_enrolled,
    IF(
        ssa.exit_withdraw_date IS NULL
        OR (
            CURRENT_DATE >= ssa.entry_date
            AND CURRENT_DATE < ssa.exit_withdraw_date
        ),
        1, 0)                                                       AS is_actively_enrolled
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_student_school_associations` ssa
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_schools` schools
    ON ssa.school_reference.school_id = schools.school_id
    AND ssa.school_year_type_reference.school_year = schools.school_year
LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_calendar_dates` calendar_dates
    ON ssa.school_year = calendar_dates.school_year
    AND ssa.school_reference.school_id = calendar_dates.calendar_reference.school_id
    AND ssa.entry_date <= calendar_dates.date
    AND (
        ssa.exit_withdraw_date IS NULL
        OR ssa.exit_withdraw_date > calendar_dates.date
    )
CROSS JOIN UNNEST(calendar_dates.calendar_events) AS calendar_events
WHERE
    calendar_dates.date < CURRENT_DATE
    AND calendar_events.calendar_event_descriptor = 'Instructional day'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11