
    
    

with child as (
    select student_key as from_field
    from `gcp-proj-id`.`dev_attendance`.`fct_student_attendance`
    where student_key is not null
),

parent as (
    select student_key as to_field
    from `gcp-proj-id`.`dev_core`.`dim_student`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


