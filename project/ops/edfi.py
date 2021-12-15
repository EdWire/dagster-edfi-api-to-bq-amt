import json

from datetime import datetime
from typing import List, Dict

from dagster import (
    AssetMaterialization,
    DynamicOut,
    DynamicOutput,
    ExpectationResult,
    In,
    List,
    op,
    Out,
    Output,
    RetryPolicy
)

import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions


@op(
    description="Dynamically outputs Ed-Fi API enpoints for parallelization",
    # ins={"api_endpoints": In(List[Dict])},
    out=DynamicOut(Dict)
)
def api_endpoint_generator(context, api_endpoints: List[Dict], use_change_queries=False) -> Dict:
    for endpoint in api_endpoints:
        # iterate through endpoints
        if "/deletes" in endpoint["endpoint"] \
            and not use_change_queries:
            # if /delete endpoint and config not using change queries, skip
        
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
    # retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "change_queries"},
)
def get_newest_api_change_versions(context, use_change_queries: bool):
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
    tags={"kind": "change_queries"}
)
def append_newest_change_version(context, newest_change_version: int):
    df = pd.DataFrame(
        [[
            datetime.fromisoformat(context.get_tag("run_timestamp")),
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
        context, table_name, schema, df
    )

    yield ExpectationResult(success=table != None, description="ensure table was created without failures")

    yield AssetMaterialization(
        asset_key=f'{context.resources.warehouse.dataset}.{table_name}',
        description="Ed-Fi API data",
    )

    yield Output(table)


@op(
    description="Retrieves change version from last job run",
    required_resource_keys={"warehouse"},
    tags={"kind": "change_queries"}
)
def get_previous_change_version(context) -> int:
    query = f"""
        SELECT newest_change_version
        FROM `{{project_id}}.{{dataset}}.edfi_processed_change_versions`
        WHERE timestamp < TIMESTAMP '{context.get_tag("run_timestamp")}'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    try:
        for row in context.resources.warehouse.run_query(context, query):
            previous_change_version = row["newest_change_version"]
            context.log.debug(f"Latest processed change version: {previous_change_version}")
            return previous_change_version
    except exceptions.NotFound as err:
        context.log.debug("Failed to query table. Table not found.")
        context.log.debug("Returning -1 as latest processed change version")
    
    return -1


@op(
    description="Retrieves data from the Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    # retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"},
)
def get_data(context, api_endpoint: Dict, previous_change_version, newest_change_version=None) -> Dict:
    output = api_endpoint.copy()
    output["records"] = list()
    
    for response in context.resources.edfi_api_client.get_data(
        output["endpoint"], previous_change_version, newest_change_version):

        response["extractedTimestamp"] = context.get_tag("run_timestamp")
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
    tags={"kind": "load"}
)
def load_data(context, api_endpoint: Dict):
    table_name = api_endpoint['table_name']
    table = context.resources.warehouse.load_data(context, table_name, f"edfi_api/{table_name}", api_endpoint["records"])

    yield ExpectationResult(success=table != None, description="ensure table was created without failures")

    yield AssetMaterialization(
        asset_key=f'{context.resources.warehouse.dataset}.{table_name}',
        description="Ed-Fi API data",
    )

    yield Output(table)
