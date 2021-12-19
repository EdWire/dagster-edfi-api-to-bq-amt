import os

from dagster import (
    fs_io_manager,
    graph,
    multiprocess_executor,
    RunRequest,
    schedule,
    ScheduleEvaluationContext
)
from dagster_dbt import dbt_cli_resource, dbt_test_op
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource

from ops.edfi import (
    api_endpoint_generator,
    get_newest_api_change_versions,
    get_previous_change_version,
    append_newest_change_version,
    get_data,
    load_data,
    run_edfi_models
)
from resources.edfi_api_resource import edfi_api_resource_client
from resources.dw_resource import bq_client


@graph(
    name="edfi_api_to_amt",
    description=(
        "Gets data from the Ed-Fi API and "
        "loads to BigQuery. Runs dbt models "
        "after data is loaded to transform data "
        "to the Analytics Middle Tier data model."
    )
)
def edfi_api_to_amt(school_year, use_change_queries):

    previous_change_version = get_previous_change_version(school_year, use_change_queries)
    newest_change_version = get_newest_api_change_versions(school_year, use_change_queries)

    result = api_endpoint_generator(use_change_queries=use_change_queries).map(
        lambda mapped_value: get_data(
            api_endpoint=mapped_value,
            school_year=school_year,
            previous_change_version=previous_change_version,
            newest_change_version=newest_change_version
        )
    ).map(
        lambda mapped_value: load_data(
            api_endpoint_records=mapped_value,
            school_year=school_year,
            use_change_queries=use_change_queries
        )
    ).collect()

    # append_newest_change_version(
    #     start_after=result, newest_change_version=newest_change_version)

    # dbt_run_result = run_edfi_models(start_after=result)
    # dbt_test_op(start_after=dbt_run_result)


edfi_api_dev_job = edfi_api_to_amt.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": fs_io_manager,
        "edfi_api_client": edfi_api_resource_client.configured({
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "api_page_limit": 5000,
            "api_mode": "YearSpecific" # DistrictSpecific, SharedInstance, YearSpecific
        }),
        "warehouse": bq_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_DEV"),
            "dataset": "dev_raw_sources",
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "dev"
        })
    },
    config={
        "inputs": {
            "school_year": { "value": 2021 },
            "use_change_queries": { "value": False }
        },
        "ops": {}
    },
)

edfi_api_prod_job = edfi_api_to_amt.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": gcs_pickle_io_manager.configured({
            "gcs_bucket": os.getenv("GCS_BUCKET_PROD"),
            "gcs_prefix": "dagster_io"
        }),
        "edfi_api_client": edfi_api_resource_client.configured({
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "school_years": [2021, 2022] # empty list tells job to not include year in URL
        }),
        "warehouse": bq_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_PROD"),
            "dataset": "prod_raw_sources",
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "prod"
        })
    }
)

@schedule(job=edfi_api_prod_job, cron_schedule="0 6 * * 7,1-5")
def change_query_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "inputs": {
                "use_change_queries": {
                    "value": True
                }
            },
            "ops": {}
        },
        tags={"date": scheduled_date},
    )

@schedule(job=edfi_api_prod_job, cron_schedule="0 6 * * 6")
def full_run_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "inputs": {
                "use_change_queries": {
                    "value": False
                }
            },
            "ops": {}
        },
        tags={"date": scheduled_date},
    )
