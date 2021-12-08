
SELECT
    STRUCT(
        JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
    ) AS school_reference,
    STRUCT(
        JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
    ) AS student_reference,
    STRUCT(
        JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS school_year
    ) AS school_year_type_reference,
    SPLIT(JSON_VALUE(data, '$.entryGradeLevelDescriptor'), '#')[OFFSET(1)] AS entry_grade_level_descriptor,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.entryDate')) AS entry_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.exitWithdrawDate')) AS exit_withdraw_date,
    SPLIT(JSON_VALUE(data, '$.exitWithdrawTypeDescriptor'), '#')[OFFSET(1)] AS exit_withdraw_type_descriptor
FROM {{ source('raw_sources', 'edfi_student_school_associations') }}
