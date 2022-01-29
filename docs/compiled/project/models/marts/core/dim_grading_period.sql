


WITH unique_records AS (
    SELECT DISTINCT
        student_section_association_reference.session_name,
        grading_period_reference.school_id,
        grading_period_reference.school_year,
        grading_period_reference.grading_period_descriptor,
        grading_period_reference.period_sequence
    FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_grades` grades

),

grades_grading_periods_unioned AS (

    SELECT
        to_hex(md5(cast(coalesce(cast(unique_records.school_id as 
    string
), '') || '-' || coalesce(cast(unique_records.school_year as 
    string
), '') || '-' || coalesce(cast(unique_records.grading_period_descriptor as 
    string
), '') || '-' || coalesce(cast(unique_records.period_sequence as 
    string
), '') as 
    string
)))                                               AS grading_period_key,
        to_hex(md5(cast(coalesce(cast(unique_records.school_id as 
    string
), '') || '-' || coalesce(cast(unique_records.school_year as 
    string
), '') || '-' || coalesce(cast(unique_records.session_name as 
    string
), '') as 
    string
)))                                               AS session_key,
        to_hex(md5(cast(coalesce(cast(unique_records.school_id as 
    string
), '') || '-' || coalesce(cast(unique_records.school_year as 
    string
), '') as 
    string
)))                                               AS school_key,
        unique_records.school_year                          AS school_year,
        grading_periods.grading_period_descriptor           AS grading_period_description,
        grading_periods.period_sequence                     AS period_sequence,
        grading_periods.begin_date                          AS grading_period_begin_date,
        grading_periods.end_date                            AS grading_period_end_date,
        grading_periods.total_instructional_days            AS total_instructional_days
    FROM unique_records
    LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_grading_periods` grading_periods
        ON unique_records.school_id = grading_periods.school_reference.school_id
        AND unique_records.school_year = grading_periods.school_year_type_reference.school_year
        AND unique_records.grading_period_descriptor = grading_periods.grading_period_descriptor
        AND unique_records.period_sequence = grading_periods.period_sequence

    UNION ALL

    SELECT
        to_hex(md5(cast(coalesce(cast(grading_periods.school_reference.school_id as 
    string
), '') || '-' || coalesce(cast(grading_periods.school_year_type_reference.school_year as 
    string
), '') || '-' || coalesce(cast(grading_periods.grading_period_descriptor as 
    string
), '') || '-' || coalesce(cast(grading_periods.period_sequence as 
    string
), '') as 
    string
)))                                                                                   AS grading_period_key,
        to_hex(md5(cast(coalesce(cast(sessions.school_reference.school_id as 
    string
), '') || '-' || coalesce(cast(sessions.school_year_type_reference.school_year as 
    string
), '') || '-' || coalesce(cast(sessions.session_name as 
    string
), '') as 
    string
)))                                                                                   AS session_key,
        to_hex(md5(cast(coalesce(cast(sessions.school_reference.school_id as 
    string
), '') || '-' || coalesce(cast(sessions.school_year_type_reference.school_year as 
    string
), '') as 
    string
)))                                                                                   AS school_key,
        sessions.school_year_type_reference.school_year                                         AS school_year,
        grading_periods.grading_period_descriptor                                               AS grading_period_description,
        grading_periods.period_sequence                                                         AS period_sequence,
        grading_periods.begin_date                                                              AS grading_period_begin_date,
        grading_periods.end_date                                                                AS grading_period_end_date,
        grading_periods.total_instructional_days                                                AS total_instructional_day
    FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_sessions` sessions
    LEFT JOIN UNNEST(sessions.grading_periods) sessions_grading_periods
    LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_school_year_types` school_year_types
        ON sessions.school_year_type_reference.school_year = school_year_types.school_year
    LEFT JOIN `gcp-proj-id`.`dev_staging`.`stg_edfi_grading_periods` grading_periods
        ON sessions.school_year_type_reference.school_year = grading_periods.school_year
        AND sessions_grading_periods.grading_period_reference.grading_period_descriptor = grading_periods.grading_period_descriptor
        AND sessions_grading_periods.grading_period_reference.period_sequence = grading_periods.period_sequence
        AND sessions_grading_periods.grading_period_reference.school_id = grading_periods.school_reference.school_id
    WHERE sessions_grading_periods.grading_period_reference.grading_period_descriptor != ''

)

SELECT DISTINCT
    grading_period_key,
    session_key,
    school_key,
    school_year,
    grading_period_description,
    period_sequence,
    grading_period_begin_date,
    grading_period_end_date,
    total_instructional_days
FROM grades_grading_periods_unioned