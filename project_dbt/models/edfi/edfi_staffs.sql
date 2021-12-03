{{ config(
        materialized='table',
        schema='edfi',
    )
}}


SELECT
    JSON_VALUE(data, '$.staffUniqueId') AS staff_unique_id,
    JSON_VALUE(data, '$.lastSurname') AS last_surname,
    JSON_VALUE(data, '$.middleName') AS middle_name,
    JSON_VALUE(data, '$.firstName') AS first_name,
    STRUCT(
        JSON_VALUE(data, '$.personReference.personId') AS person_id,
        SPLIT(JSON_VALUE(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] AS source_system_descriptor
    ) AS person_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.birthDate')) AS birth_date,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(electronic_mails, '$.electronicMailTypeDescriptor'), '#')[OFFSET(1)] AS electronic_mail_type_descriptor,
            JSON_VALUE(electronic_mails, "$.electronicMailAddress") AS electronic_mail_address,
            CAST(JSON_VALUE(electronic_mails, "$.doNotPublishIndicator") AS BOOL) AS do_not_publish_indicator
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.electronicMails")) electronic_mails 
    ) AS electronic_mails,
FROM {{ source('raw_sources', 'edfi_staffs') }}

