


SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(staff.staff_unique_id as 
    string
), '') as 
    string
)))                          AS user_key,
    email.electronic_mail_address   AS user_email
FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_staffs` staff,
    UNNEST(staff.electronic_mails) AS email
WHERE email.electronic_mail_type_descriptor = 'Work'