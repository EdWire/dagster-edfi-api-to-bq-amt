
    
    

with dbt_test__target as (
  
  select local_education_agency_id as unique_field
  from `gcp-proj-id`.`dev_core`.`dim_local_education_agency`
  where local_education_agency_id is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


