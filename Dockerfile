FROM python:3.8-slim

COPY --from=gcr.io/berglas/berglas:latest /bin/berglas /bin/berglas

ENV PYTHONUNBUFFERED=True
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV DAGSTER_GRPC_MAX_RX_BYTES=20000000
ENV DBT_PROFILES_DIR=/opt/dagster/app
ENV DBT_PROJECT_DIR=/opt/dagster/app/project_dbt
ENV PYTHONPATH=/opt/dagster/app/project

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

COPY requirements-prod.txt /opt/dagster/app/requirements.txt
WORKDIR /opt/dagster/app
RUN pip install -r requirements.txt

COPY workspace.yaml /opt/dagster/app/
COPY project /opt/dagster/app/project
COPY project_dbt /opt/dagster/app/project_dbt
COPY profiles.yml /opt/dagster/app/
COPY prod_dagster.yaml /opt/dagster/dagster_home/dagster.yaml

COPY run-prod.sh /opt/dagster/app/
RUN ["chmod", "+x", "/opt/dagster/app/run-prod.sh"]

ENTRYPOINT exec /bin/berglas exec /opt/dagster/app/run-prod.sh