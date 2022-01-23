
SELECT
    {{ dbt_utils.surrogate_key([
        'school_reference.school_id',
        'school_year_type_reference.school_year',
        'session_name'
    ]) }}                                   AS session_key,
    session_name                            AS session_name,
    term_descriptor                         AS term,
    total_instructional_days                AS total_instructional_days,
    begin_date                              AS session_begin_date,
    end_date                                AS session_end_date
FROM {{ ref('stg_edfi_sessions') }} sessions
