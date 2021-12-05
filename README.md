# Ed-Fi API -> Analytics Middle Tier
This repository contains code that pulls data from a target Ed-Fi API and creates tables in BigQuery that conform to the Analytics Middle Tier specification.

[YouTube demo video](https://youtu.be/A1a7C9pDVL4)

More specifically, this repository is a [Dagster](https://dagster.io/) workspace that contains a job designed to:

1. Extract data from a set of Ed-Fi API endpoints
2. Store the data as JSON files in a cloud based blob storage service
3. Query the JSON files from a cloud data warehouse to produce:
    * Tables that represent the Ed-Fi API endpoint
    * Tables that meet the spec of the Analytics Middle Tier

This repository currently supports Google Cloud Storage, BigQuery, and the subset of the Analytics Middler Tier necessary to power the Chronic Absenteeism Starter Kit use case.

Dagster was chosen as the orchestration platform to run this job due to it being free and open-source, focus on workflows that are specifically data oriented, and due to its ability to abstract out intermediary IO and other resources. This means it would be possible to extend this job to support additional blob storage and cloud data warehouse services (ie. AWS S3 buckets and Snowflake).

This repository is designed to be opened on a machine with [Docker](https://www.docker.com/) installed. When opened in [Visual Studio Code](https://code.visualstudio.com/) with the [Remote Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension installed, Visual Studio Code can open the repository in its own container using all Python requirements specified in this repository's `requirements.txt` file.

At the root of this repo is a `.env-sample` file. Copy the file to create a `.env` file. Complete the following missing values:
* EDFI_BASE_URL
* EDFI_API_KEY
* EDFI_API_SECRET

You will complete the missing values in the steps below.

## Google Cloud Configuration
Create a Google Cloud Platform (GCP) project and set the `GCP_PROJECT` variable to the Google Cloud project ID.

### Service Account
Authentication with the GCP project happens through a service account. In GCP, head to _IAM & Admin --> Service Accounts_ to create your service account.

* Click **Create Service Account**
* Choose a name (ie. dagster) and click **Create**
* Grant the service account the following roles
    * BigQuery Admin
    * Storage Admin
* Click **Done** 
* Select the actions menu and click **Create key**. Create a JSON key, rename to _service.json_ and store in the root of the repository.

### Google Cloud Storage
Create a Google Cloud Storage bucket that will be used to house the JSON data retrieved from the target Ed-Fi API. In GCP, head to _Cloud Storage_ and click **Create Bucket**. Once created, set the `GCS_BUCKET_DEV` variable to the newly created bucket's name (ie. dagster-dev-123).


## Launching Dev Job
In Visual Studio Code, with the repo opened in a container, run the command below:

```bash

dagit -w workspace.yaml;

```

The command above launches dagit, Dagster's web UI. The menu top-left will allow you to access the Ed-Fi related job.

* Click **Launchpad**
* Click **Launch Run**


## Deploying to Production
This repository has been successfully deployed to production. Future documentation will be written if community interest is expressed.
