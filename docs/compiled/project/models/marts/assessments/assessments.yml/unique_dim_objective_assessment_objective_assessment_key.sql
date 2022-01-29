
    
    

with dbt_test__target as (
  
  select objective_assessment_key as unique_field
  from `gcp-proj-id`.`dev_assessments`.`dim_objective_assessment`
  where objective_assessment_key is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


