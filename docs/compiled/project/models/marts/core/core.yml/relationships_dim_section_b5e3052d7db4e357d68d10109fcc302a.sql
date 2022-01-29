
    
    

with child as (
    select session_key as from_field
    from `gcp-proj-id`.`dev_core`.`dim_section`
    where session_key is not null
),

parent as (
    select session_key as to_field
    from `gcp-proj-id`.`dev_core`.`dim_session`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


