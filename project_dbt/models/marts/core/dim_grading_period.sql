{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    CONCAT(grading_period_descriptor, '-',
        school_reference.school_id, '-',
        FORMAT_DATE('%Y%m%d', begin_date))     AS grading_period_key,
    school_reference.school_id                 AS school_key,
    school_year_type_reference.school_year     AS school_year,
    grading_period_descriptor                  AS grading_period_description,
    period_sequence                            AS period_sequence,
    begin_date                                 AS grading_period_begin_date_key,
    end_date                                   AS grading_period_end_date_key,
    total_instructional_days                   AS total_instructional_days
FROM {{ ref('stg_edfi_grading_periods') }}
