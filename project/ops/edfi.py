import json
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


@op(
    description="Dynamically outputs Ed-Fi API enpoints for parallelization",
    ins={"api_endpoints": In(List[Dict])},
    out=DynamicOut(Dict)
)
def api_endpoint_generator(context, api_endpoints: List[Dict]) -> Dict:
    for endpoint in api_endpoints:
        yield DynamicOutput(
            value=endpoint,
            mapping_key=endpoint["table_name"]
        )


@op(
    description="Retrieves data from the Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    # retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"},
)
def get_data(context, api_endpoint: Dict) -> Dict:
    output = api_endpoint.copy()
    output["records"] = list()
    
    for response in context.resources.edfi_api_client.get_data(output["endpoint"]):
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
