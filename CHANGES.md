# Changelog

# 0.3.0

### New

- [dagster] Moves Ed-Fi API page limit variable under `edfi_api_client` resource config.
- [dagster] Adds Ed-Fi API mode variable to `edfi_api_client` resource config.
- [dagster] Adds `school_year` as input variable to specify school year scope of data pull.
- [dagster] Updates Google Cloud Storage folder structure to store each school year in its own folder under the `edfi_api` folder.
- [dagster] Updates source URIs in BigQuery external tables to allow for querying multiple school years of data.
- [dbt] Updates dbt SQL to factor in multiple school years of data.
- [dbt] Adds documentation and tests.
