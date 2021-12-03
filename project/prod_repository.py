from dagster import repository

from jobs.edfi_api_to_amt import edfi_api_prod_job


@repository
def prod_repo():
    return [
        edfi_api_prod_job
    ]
