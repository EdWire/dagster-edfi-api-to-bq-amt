
    
    

with child as (
    select student_section_key as from_field
    from `gcp-proj-id`.`dev_grades`.`fct_student_section_grade`
    where student_section_key is not null
),

parent as (
    select student_section_key as to_field
    from `gcp-proj-id`.`dev_core`.`dim_student_section`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


