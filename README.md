# Ed-Fi API -> Analytics Middle Tier
This repository contains code that pulls data from a target Ed-Fi API and creates tables in BigQuery that conform to the Analytics Middle Tier specification.

More specifically, this repository is a [Dagster](https://dagster.io/) workspace that contains a job designed to:

1. Extract data from a set of Ed-Fi API endpoints
2. Store the data as JSON files in a cloud based blob storage service
3. Query the JSON files from a cloud data warehouse to produce:
    * Tables that represent the Ed-Fi API endpoint
    * Tables that meet the spec of the Analytics Middle Tier

This repository currently supports Google Cloud Storage, BigQuery, and the subset of the Analytics Middler Tier necessary to power the Chronic Absenteeism Starter Kit use case.

Dagster was chosen as the orchestration platform to run this job due to it being free and open-source, focused on workflows that are specifically data oriented, and finally due to its ability to abstract out intermediary IO and other resources. This means it would be possible to extend this job to support additional blob storage and cloud data warehouse services (ie. AWS S3 buckets and Snowflake).

## Local Testing
This repository is designed to be opened on a machine with [Docker](https://www.docker.com/) installed. When opened in [Visual Studio Code](https://code.visualstudio.com/) with the [Remote Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension installed, Visual Studio Code can open the code in its own container using all Python requirements specified in this repository's `requirements.txt` file.


### Google Cloud Configuration



```bash

dagit -w workspace.yaml;

```
