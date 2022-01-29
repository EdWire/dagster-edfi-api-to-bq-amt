
    
    

with dbt_test__target as (
  
  select student_school_demographic_bridge_key as unique_field
  from `gcp-proj-id`.`dev_core`.`student_local_education_agency_demographics_bridge`
  where student_school_demographic_bridge_key is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


