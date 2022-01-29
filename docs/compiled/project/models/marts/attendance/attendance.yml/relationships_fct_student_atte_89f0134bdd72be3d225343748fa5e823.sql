
    
    

with child as (
    select school_key as from_field
    from `gcp-proj-id`.`dev_attendance`.`fct_student_attendance`
    where school_key is not null
),

parent as (
    select school_key as to_field
    from `gcp-proj-id`.`dev_core`.`dim_school`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


