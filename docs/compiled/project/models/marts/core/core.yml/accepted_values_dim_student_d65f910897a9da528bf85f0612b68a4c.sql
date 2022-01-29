
    
    

with all_values as (

    select
        is_english_language_learner as value_field,
        count(*) as n_records

    from `gcp-proj-id`.`dev_core`.`dim_student`
    group by is_english_language_learner

)

select *
from all_values
where value_field not in (
    'Yes','No'
)


