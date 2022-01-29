
    
    

with dbt_test__target as (
  
  select school_id as unique_field
  from `gcp-proj-id`.`dev_core`.`dim_school`
  where school_id is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


