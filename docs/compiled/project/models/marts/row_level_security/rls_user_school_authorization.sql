WITH emails AS (

    SELECT
        stg_edfi_staffs.staff_unique_id AS staff_unique_id,
        emails.electronic_mail_address  AS electronic_mail_address
    FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_staffs` stg_edfi_staffs
    CROSS JOIN UNNEST(electronic_mails) AS emails
    WHERE emails.electronic_mail_type_descriptor = 'Work'

)


SELECT
    school_year_type_reference.school_year          AS school_year,
    to_hex(md5(cast(coalesce(cast(staff_reference.staff_unique_id as 
    string
), '') as 
    string
)))                                           AS user_key,
    to_hex(md5(cast(coalesce(cast(school_reference.school_id as 
    string
), '') as 
    string
)))                                           AS school_key,
    emails.electronic_mail_address                  AS email
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_staff_school_associations` stg_edfi_staff_school_associations
LEFT JOIN emails
    ON stg_edfi_staff_school_associations.staff_reference.staff_unique_id = emails.staff_unique_id