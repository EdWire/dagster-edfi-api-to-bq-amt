
    
    

with all_values as (

    select
        in_special_education_program as value_field,
        count(*) as n_records

    from `gcp-proj-id`.`dev_core`.`dim_student`
    group by in_special_education_program

)

select *
from all_values
where value_field not in (
    'Yes','No'
)


