from dagster import repository, ScheduleDefinition

from jobs.edfi_api_to_amt import edfi_api_prod_job


edfi_api_schedule = ScheduleDefinition(
    job=edfi_api_prod_job,
    cron_schedule="0 6 * * *",
    execution_timezone="US/Central")

@repository
def dev_repo():
    return [ edfi_api_schedule ]
