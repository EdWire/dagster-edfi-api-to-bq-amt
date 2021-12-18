import json

from datetime import datetime
from typing import List, Dict

from dagster import (
    AssetMaterialization,
    DynamicOut,
    DynamicOutput,
    ExpectationResult,
    op,
    Out,
    Output,
    RetryPolicy
)
from dagster_dbt.cli.types import DbtCliOutput
from dagster_dbt.utils import generate_materializations

import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions


@op(
    description="Dynamically outputs Ed-Fi API enpoints for parallelization",
    out=DynamicOut(Dict)
)
def api_endpoint_generator(context, api_endpoints: List[Dict], use_change_queries: bool) -> Dict:
    """
    Dynamically output each Ed-Fi API endpoint
    in the job's config. If job is configured to not
    use change queries, do not output the /deletes version
    of each endpoint.
    """
    for endpoint in api_endpoints:
        if "/deletes" in endpoint["endpoint"] and not use_change_queries:
            pass
        else:
            yield DynamicOutput(
                value=endpoint,
                mapping_key=endpoint["table_name"]
            )


@op(
    description="Retrieves newest change version from Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    out={ "newest_change_version": Out(int) },
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"},
)
def get_newest_api_change_versions(context, use_change_queries: bool) -> int:
    """
    If job is configured to use change queries, get
    the newest change version number from the target Ed-Fi API.
    """
    if use_change_queries:
        response = context.resources.edfi_api_client.get_available_change_versions()
        context.log.debug(response)
        yield Output(response["NewestChangeVersion"], "newest_change_version")
    else:
        context.log.debug("Will not use change queries")


@op(
    description="Appends newest change version to warehouse table",
    required_resource_keys={"warehouse"},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"}
)
def append_newest_change_version(context, start_after, newest_change_version: int):
    """
    Create a dataframe with two columns:
    timestamp column using job's run timestamp and
    newest change version. Call append_data() on
    warehouse resource to append new record to table.

    Args:
        newest_change_version (bool): The latest change
        version number returned from target Ed-Fi API.
    """
    df = pd.DataFrame(
        [[
            datetime.now().isoformat(),
            newest_change_version
        ]],
        columns = ['timestamp', 'newest_change_version']
    )
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("newest_change_version", "INTEGER", "REQUIRED")
    ]
    table_name = 'edfi_processed_change_versions'

    table = context.resources.warehouse.append_data(
        table_name, schema, df)

    yield ExpectationResult(
        success=table != None,description="ensure table was created without failures")

    yield AssetMaterialization(
        asset_key=f'{context.resources.warehouse.dataset}.{table_name}',
        description="Ed-Fi API data",
    )

    yield Output(table)


@op(
    description="Retrieves change version from last job run",
    required_resource_keys={"warehouse"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"}
)
def get_previous_change_version(context) -> int:
    """
    Run SQL query on table edfi_processed_change_versions
    to retrieve the change version number from the
    previous job run. If no data is returned, return -1.
    This will cause the extract step to pull all data from
    the target Ed-Fi API.
    """
    query = f"""
        SELECT newest_change_version
        FROM `{{project_id}}.{{dataset}}.edfi_processed_change_versions`
        WHERE timestamp < TIMESTAMP '{datetime.now().isoformat()}'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    try:
        for row in context.resources.warehouse.run_query(query):
            previous_change_version = row["newest_change_version"]
            context.log.debug(f"Latest processed change version: {previous_change_version}")
            return previous_change_version
    except exceptions.NotFound as err:
        context.log.debug(err)
        context.log.debug("Failed to query table. Table not found.")
        context.log.debug("Returning -1 as latest processed change version")

    return -1


@op(
    description="Retrieves data from the Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "extract"},
)
def get_data(context, api_endpoint: Dict, previous_change_version: int,
    newest_change_version=None) -> Dict:
    """
    Retrieve data from API endpoint. For each record, add the run
    timestamp to store when the data was extracted.
    """
    output = api_endpoint.copy()
    output["records"] = list()
    retrieved_data = context.resources.edfi_api_client.get_data(
        output["endpoint"], previous_change_version, newest_change_version)

    for response in retrieved_data:

        response["extractedTimestamp"] = datetime.now().isoformat()
        output["records"].append({
            "id": None,
            "data": json.dumps(response)
        })

    context.log.info(f"Received {len(output['records'])} records")
    return output


@op(
    description="Loads JSON strings to BigQuery",
    required_resource_keys={"warehouse"},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "load"}
)
def load_data(context, api_endpoint_records: Dict):
    """
    Load the passed in records retrieved from the
    specific API endpoint into the data warehouse.
    """
    table_name = api_endpoint_records['table_name']
    table = context.resources.warehouse.load_data(
        table_name, f"edfi_api/{table_name}", api_endpoint_records["records"])

    yield ExpectationResult(success=table is not None,
        description="ensure table was created without failures")

    yield AssetMaterialization(
        asset_key=f'{context.resources.warehouse.dataset}.{table_name}',
        description="Ed-Fi API data",
    )

    yield Output(table)


@op(
    description="Run all dbt models tagged with edfi and amt",
    required_resource_keys={"dbt"},
)
def run_edfi_models(context, start_after) -> DbtCliOutput:
    """
    Run all dbt models tagged with edfi
    and amt. Yield asset materializations
    """
    dbt_cli_edfi_output = context.resources.dbt.run(models=["tag:edfi"])
    for materialization in generate_materializations(
        dbt_cli_edfi_output, asset_key_prefix=["edfi"]):

        yield materialization
    
    dbt_cli_amt_output = context.resources.dbt.run(models=["tag:amt"])
    for materialization in generate_materializations(
        dbt_cli_amt_output, asset_key_prefix=["amt"]):

        yield materialization.asset_key

    yield Output(dbt_cli_edfi_output)
