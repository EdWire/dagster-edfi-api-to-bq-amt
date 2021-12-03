import json
import uuid
from typing import List, Dict

from dataclasses import dataclass

from dagster import resource
from google.cloud import bigquery
from google.cloud import storage


@dataclass
class BigQueryClient:
    '''Class for loading Google Forms data into BigQuery'''
    dataset: str
    staging_gcs_bucket: str


    def upload_to_gcs(self, context, gcs_path: str, records: List[Dict]) -> str:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.staging_gcs_bucket)
        gcs_paths = list()

        # delete existing files
        blobs = bucket.list_blobs(prefix=f'{gcs_path}/')
        for blob in blobs:
            blob.delete()

        # upload records into 10,000 record JSON chunks
        context.log.info(f'Splitting {len(records)} into 10,000 record chunks.')
        for i in range(0, len(records), 10000):
            gcs_file = f'{gcs_path}/{str(uuid.uuid4())}.json'
            output = ''
            for record in records[i:i+10000]:
                output = output + json.dumps(record) + '\r\n'

            bucket.blob(gcs_file).upload_from_string(
                output,
                content_type='application/json',
                num_retries=3
            )

            gcs_paths.append(f'gs://{self.staging_gcs_bucket}/{gcs_file}')

        context.log.debug(gcs_paths)
        return gcs_paths


    def load_data(self, context, table_name, gcs_path, records) -> str:
        gcs_paths = self.upload_to_gcs(context, gcs_path, records)
        client = bigquery.Client()
        project = client.project
    
        # create dataset if it doesn't already exist
        client.create_dataset(
            bigquery.Dataset(f"{project}.{self.dataset}"),
            exists_ok=True
        )

        schema = [
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("data", "STRING", "NULLABLE")
        ]

        dataset_ref = bigquery.DatasetReference(project, self.dataset)
        table_ref = bigquery.Table(dataset_ref.table(table_name), schema=schema)

        external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
        external_config.source_uris = [f'gs://{self.staging_gcs_bucket}/{gcs_path}/*.json']

        table_ref.external_data_configuration = external_config
        table = client.create_table(table_ref, exists_ok=True)
        client.close()

        return "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)


@resource(
    config_schema={
        "dataset": str,
        "staging_gcs_bucket": str,
    },
    description="BigQuery client used to load data.",
)
def bq_client(context):
    return BigQueryClient(
        context.resource_config["dataset"],
        context.resource_config["staging_gcs_bucket"]
    )
