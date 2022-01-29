
    
    

with dbt_test__target as (
  
  select school_year || '-' || school_id as unique_field
  from `gcp-proj-id`.`dev_staging`.`stg_edfi_schools`
  where school_year || '-' || school_id is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


