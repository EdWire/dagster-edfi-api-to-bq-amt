
SELECT DISTINCT
    staff.staff_unique_id AS user_key,
    email.electronic_mail_address AS user_email
FROM {{ ref('edfi_staffs') }} staff,
    UNNEST(staff.electronic_mails) AS email
WHERE email.electronic_mail_type_descriptor = 'Work'
