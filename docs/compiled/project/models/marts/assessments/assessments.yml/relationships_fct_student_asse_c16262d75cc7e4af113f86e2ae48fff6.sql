
    
    

with child as (
    select assessment_key as from_field
    from `gcp-proj-id`.`dev_assessments`.`fct_student_assessment`
    where assessment_key is not null
),

parent as (
    select assessment_key as to_field
    from `gcp-proj-id`.`dev_assessments`.`dim_assessment`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


