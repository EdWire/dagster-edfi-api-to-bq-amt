{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id',
        'sessions.school_year_type_reference.school_year',
        'sessions.term_descriptor',
        'sessions_grading_periods.grading_period_reference.grading_period_descriptor',
        'grading_periods.begin_date'
    ]) }}                                                                                   AS academic_time_period_key,
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id',
        'sessions.school_year_type_reference.school_year',
        'sessions.session_name'
    ]) }}                                                                                   AS session_key,
    {{ dbt_utils.surrogate_key([
        'grading_periods.school_reference.school_id',
        'grading_periods.school_year_type_reference.school_year',
        'grading_periods.grading_period_descriptor',
        'grading_periods.period_sequence'
    ]) }}                                                                                   AS grading_period_key,
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id'
    ]) }}                                                                                   AS school_key,
    sessions.school_year_type_reference.school_year                                         AS school_year,
    school_year_types.school_year_description                                               AS school_year_name,
    school_year_types.current_school_year                                                   AS is_current_school_year,
    sessions.session_name                                                                   AS session_name,
    sessions.term_descriptor                                                                AS term_name,
    grading_periods.grading_period_descriptor                                               AS grading_period_name,
    sessions.total_instructional_days                                                       AS total_instructional_days,
    sessions.begin_date                                                                     AS session_begin_date,
    sessions.end_date                                                                       AS session_end_date
FROM {{ ref('stg_edfi_sessions') }} sessions
LEFT JOIN UNNEST(sessions.grading_periods) sessions_grading_periods
LEFT JOIN {{ ref('stg_edfi_school_year_types') }} school_year_types
    ON sessions.school_year_type_reference.school_year = school_year_types.school_year
LEFT JOIN {{ ref('stg_edfi_grading_periods') }} grading_periods
    ON sessions.school_year_type_reference.school_year = grading_periods.school_year
    AND sessions_grading_periods.grading_period_reference.grading_period_descriptor = grading_periods.grading_period_descriptor
    AND sessions_grading_periods.grading_period_reference.period_sequence = grading_periods.period_sequence
    AND sessions_grading_periods.grading_period_reference.school_id = grading_periods.school_reference.school_id
