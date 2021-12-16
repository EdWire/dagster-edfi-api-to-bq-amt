import json
import uuid
from typing import List, Dict

from dataclasses import dataclass

from dagster import resource
from google.cloud import bigquery
from google.cloud import storage


class BigQueryClient:
    '''Class for loading data into BigQuery'''

    def __init__(self, dataset, staging_gcs_bucket):
        self.dataset = dataset
        self.staging_gcs_bucket = staging_gcs_bucket
        self.client = bigquery.Client()
        self.project = self.client.project
        self._create_dataset()
        self.dataset_ref = bigquery.DatasetReference(self.project, self.dataset)


    def _create_dataset(self):
        # create dataset if it doesn't already exist
        self.client.create_dataset(
            bigquery.Dataset(f"{self.project}.{self.dataset}"),
            exists_ok=True
        )


    def upload_to_gcs(self, context, gcs_path: str, records: List[Dict]) -> str:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.staging_gcs_bucket)
        gcs_paths = list()

        # delete existing files
        # blobs = bucket.list_blobs(prefix=f'{gcs_path}/')
        # for blob in blobs:
        #     blob.delete()

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
            gcs_upload_path = f"gs://{self.staging_gcs_bucket}/{gcs_file}"
            context.log.debug(f"Uploaded JSON file to {gcs_upload_path}")
            gcs_paths.append(gcs_upload_path)

        return gcs_paths


    def load_data(self, context, table_name, gcs_path, records) -> str:
        gcs_paths = self.upload_to_gcs(context, gcs_path, records)
    
        schema = [
            bigquery.SchemaField("id", "STRING", "NULLABLE"),
            bigquery.SchemaField("data", "STRING", "NULLABLE")
        ]

        table_ref = bigquery.Table(self.dataset_ref.table(table_name), schema=schema)

        external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
        external_config.source_uris = [f'gs://{self.staging_gcs_bucket}/{gcs_path}/*.json']

        table_ref.external_data_configuration = external_config
        table = self.client.create_table(table_ref, exists_ok=True)
        self.client.close()

        return "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)


    def append_data(self, context, table_name: str, schema: List, df) -> str:
        """Appends data to bigquery table using schema specified
        
        Parameters
        ----------
        table_name : str
            The name of the bigquery table
        schema : List
            BigQuery schema for bigquery table
        data_to_append
            The actual records to append to bigquery table

        """
        table_ref = bigquery.Table(self.dataset_ref.table(table_name), schema=schema)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition='WRITE_APPEND',
        )

        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )

        job.result()  # waits for the job to complete.

        self.client.close()
        return "Created table {}.{}.{}".format(self.project, self.dataset, table_name)


    def run_query(self, context, query: str):
        return self.client.query(query.format(project_id=self.project, dataset=self.dataset))


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
