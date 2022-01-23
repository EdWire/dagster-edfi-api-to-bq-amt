{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    {{ dbt_utils.surrogate_key([
        'school_reference.school_id',
        'grading_period_descriptor', 
        'begin_date'
    ]) }}                                      AS grading_period_key,
    {{ dbt_utils.surrogate_key([
        'school_reference.school_id'
    ]) }}                                      AS school_key,
    school_year_type_reference.school_year     AS school_year,
    grading_period_descriptor                  AS grading_period_description,
    period_sequence                            AS period_sequence,
    begin_date                                 AS grading_period_begin_date,
    end_date                                   AS grading_period_end_date,
    total_instructional_days                   AS total_instructional_days
FROM {{ ref('stg_edfi_grading_periods') }}
