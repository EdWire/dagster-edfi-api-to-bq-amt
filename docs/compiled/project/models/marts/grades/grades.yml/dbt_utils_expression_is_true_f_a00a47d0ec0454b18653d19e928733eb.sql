

with meet_condition as (
    select * from `gcp-proj-id`.`dev_grades`.`fct_student_section_grade` where 1=1
)

select
    *
from meet_condition

where not(numeric_grade_earned >= 0)

