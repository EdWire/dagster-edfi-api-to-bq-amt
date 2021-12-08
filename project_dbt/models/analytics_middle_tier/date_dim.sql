
WITH dates AS (
    SELECT DISTINCT
        date,
        calendar_reference.school_year
    FROM  {{ ref('edfi_calendar_dates') }}
)


SELECT
    FORMAT_DATE('%Y%m%d', date) AS date_key,
    date,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATETIME('%B', date) AS month,
    EXTRACT(QUARTER FROM date) AS calendar_quarter,
    CASE
        WHEN EXTRACT(QUARTER FROM date) = 1 THEN 'First'
        WHEN EXTRACT(QUARTER FROM date) = 2 THEN 'Second'
        WHEN EXTRACT(QUARTER FROM date) = 3 THEN 'Third'
        WHEN EXTRACT(QUARTER FROM date) = 4 THEN 'Fourth'
    END AS calendar_quarter_name,
    school_year AS calendar_year
FROM dates
