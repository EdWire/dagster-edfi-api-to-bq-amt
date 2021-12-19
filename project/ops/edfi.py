import json

from datetime import datetime
from typing import List, Dict, Optional, Union

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
def api_endpoint_generator(context, use_change_queries: bool) -> Dict:
    """
    Dynamically output each Ed-Fi API endpoint
    in the job's config. If job is configured to not
    use change queries, do not output the /deletes version
    of each endpoint.
    """
    edfi_api_endpoints = [
        {"endpoint": "/ed-fi/localEducationAgencies", "table_name": "edfi_local_education_agencies"},
        {"endpoint": "/ed-fi/localEducationAgencies/deletes", "table_name": "edfi_local_education_agencies_deletes"},
        {"endpoint": "/ed-fi/schools", "table_name": "edfi_schools" },
        {"endpoint": "/ed-fi/schools/deletes", "table_name": "edfi_schools_deletes" },
        {"endpoint": "/ed-fi/schoolYearTypes", "table_name": "edfi_school_year_types" },
        {"endpoint": "/ed-fi/students", "table_name": "edfi_students" },
        {"endpoint": "/ed-fi/students/deletes", "table_name": "edfi_students_deletes" },
        {"endpoint": "/ed-fi/studentEducationOrganizationAssociations", "table_name": "edfi_student_education_organization_associations" },
        {"endpoint": "/ed-fi/studentEducationOrganizationAssociations/deletes", "table_name": "edfi_student_education_organization_associations_deletes" },
        {"endpoint": "/ed-fi/studentSchoolAssociations", "table_name": "edfi_student_school_associations" },
        {"endpoint": "/ed-fi/studentSchoolAssociations/deletes", "table_name": "edfi_student_school_associations_deletes" },
        {"endpoint": "/ed-fi/calendars", "table_name": "edfi_calendars" },
        {"endpoint": "/ed-fi/calendars/deletes", "table_name": "edfi_calendars_deletes" },
        {"endpoint": "/ed-fi/calendarDates", "table_name": "edfi_calendar_dates" },
        {"endpoint": "/ed-fi/calendarDates/deletes", "table_name": "edfi_calendar_dates_deletes" },
        {"endpoint": "/ed-fi/courses", "table_name": "edfi_courses" },
        {"endpoint": "/ed-fi/courses/deletes", "table_name": "edfi_courses_deletes" },
        {"endpoint": "/ed-fi/courseOfferings", "table_name": "edfi_course_offerings" },
        {"endpoint": "/ed-fi/courseOfferings/deletes", "table_name": "edfi_course_offerings_deletes" },
        {"endpoint": "/ed-fi/disciplineActions", "table_name": "edfi_discipline_actions" },
        {"endpoint": "/ed-fi/disciplineActions/deletes", "table_name": "edfi_discipline_actions_deletes" },
        {"endpoint": "/ed-fi/disciplineIncidents", "table_name": "edfi_discipline_incidents" },
        {"endpoint": "/ed-fi/disciplineIncidents/deletes", "table_name": "edfi_discipline_incident_deletes" },
        {"endpoint": "/ed-fi/grades", "table_name": "edfi_grades" },
        {"endpoint": "/ed-fi/grades/deletes", "table_name": "edfi_grades_deletes" },
        {"endpoint": "/ed-fi/gradingPeriods", "table_name": "edfi_grading_periods" },
        {"endpoint": "/ed-fi/gradingPeriods/deletes", "table_name": "edfi_grading_periods_deletes" },
        {"endpoint": "/ed-fi/gradingPeriodDescriptors", "table_name": "edfi_grading_period_descriptors" },
        {"endpoint": "/ed-fi/gradingPeriodDescriptors/deletes", "table_name": "edfi_grading_period_descriptors_deletes" },
        {"endpoint": "/ed-fi/staffDisciplineIncidentAssociations", "table_name": "edfi_staff_discipline_incident_associations" },
        {"endpoint": "/ed-fi/staffDisciplineIncidentAssociations/deletes", "table_name": "edfi_staff_discipline_incident_associations_deletes" },
        {"endpoint": "/ed-fi/studentDisciplineIncidentAssociations", "table_name": "edfi_student_discipline_incident_associations" }, # deprecated
        {"endpoint": "/ed-fi/studentDisciplineIncidentAssociations/deletes", "table_name": "edfi_student_discipline_incident_associations_deletes" }, # deprecated
        # {"endpoint": "/ed-fi/studentDisciplineIncidentBehaviorAssociations", "table_name": "edfi_student_discipline_incident_behavior_associations" }, # implemented in v5.2
        # {"endpoint": "/ed-fi/studentDisciplineIncidentNonOffenderAssociations", "table_name": "edfi_student_discipline_incident_non_offender_associations" }, # implemented in v5.2
        {"endpoint": "/ed-fi/parents", "table_name": "edfi_parents" },
        {"endpoint": "/ed-fi/parents/deletes", "table_name": "edfi_parents_deletes" },
        {"endpoint": "/ed-fi/sections", "table_name": "edfi_sections" },
        {"endpoint": "/ed-fi/sections/deletes", "table_name": "edfi_sections_deletes" },
        {"endpoint": "/ed-fi/staffs", "table_name": "edfi_staffs" },
        {"endpoint": "/ed-fi/staffs/deletes", "table_name": "edfi_staffs_deletes" },
        {"endpoint": "/ed-fi/staffEducationOrganizationAssignmentAssociations", "table_name": "edfi_staff_education_organization_assignment_associations" },
        {"endpoint": "/ed-fi/staffEducationOrganizationAssignmentAssociations/deletes", "table_name": "edfi_staff_education_organization_assignment_associations_deletes" },
        {"endpoint": "/ed-fi/staffSectionAssociations", "table_name": "edfi_staff_section_associations" },
        {"endpoint": "/ed-fi/staffSectionAssociations/deletes", "table_name": "edfi_staff_section_associations_deletes" },
        {"endpoint": "/ed-fi/studentParentAssociations", "table_name": "edfi_student_parent_associations" },
        {"endpoint": "/ed-fi/studentParentAssociations/deletes", "table_name": "edfi_student_parent_associations_deletes" },
        {"endpoint": "/ed-fi/studentSchoolAttendanceEvents", "table_name": "edfi_student_school_attendance_events" },
        {"endpoint": "/ed-fi/studentSchoolAttendanceEvents/deletes", "table_name": "edfi_student_school_attendance_events_deletes" },
        {"endpoint": "/ed-fi/studentSectionAssociations", "table_name": "edfi_student_section_associations" },
        {"endpoint": "/ed-fi/studentSectionAssociations/deletes", "table_name": "edfi_student_section_associations_deletes" },
        {"endpoint": "/ed-fi/studentSectionAttendanceEvents", "table_name": "edfi_student_section_attendance_events" },
        {"endpoint": "/ed-fi/studentSectionAttendanceEvents/deletes", "table_name": "edfi_student_section_attendance_events_deletes" },
        {"endpoint": "/ed-fi/sessions", "table_name": "edfi_sessions" },
        {"endpoint": "/ed-fi/sessions/deletes", "table_name": "edfi_sessions_deletes" },
        {"endpoint": "/ed-fi/cohortTypeDescriptors", "table_name": "edfi_cohort_type_descriptors"},
        {"endpoint": "/ed-fi/cohortTypeDescriptors/deletes", "table_name": "edfi_cohort_type_descriptors_deletes"},
        {"endpoint": "/ed-fi/disabilityDescriptors", "table_name": "edfi_disability_descriptors"},
        {"endpoint": "/ed-fi/disabilityDescriptors/deletes", "table_name": "edfi_disability_descriptors_deletes"},
        {"endpoint": "/ed-fi/languageDescriptors", "table_name": "edfi_language_descriptors"},
        {"endpoint": "/ed-fi/languageDescriptors/deletes", "table_name": "edfi_language_descriptors_deletes"},
        {"endpoint": "/ed-fi/languageUseDescriptors", "table_name": "edfi_language_use_descriptors"},
        {"endpoint": "/ed-fi/languageUseDescriptors/deletes", "table_name": "edfi_language_use_descriptors_deletes"},
        {"endpoint": "/ed-fi/raceDescriptors", "table_name": "edfi_race_descriptors"},
        {"endpoint": "/ed-fi/raceDescriptors/deletes", "table_name": "edfi_race_descriptors_deletes"}
    ]

    for endpoint in edfi_api_endpoints:
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
    out=Out(Union[int, None]),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"},
)
def get_newest_api_change_versions(context, school_year: int, use_change_queries: bool):
    """
    If job is configured to use change queries, get
    the newest change version number from the target Ed-Fi API.
    """
    if use_change_queries:
        response = context.resources.edfi_api_client.get_available_change_versions(school_year)
        context.log.debug(response)
        return response["NewestChangeVersion"]
    else:
        context.log.info("Will not use change queries")
        return None


@op(
    description="Appends newest change version to warehouse table",
    required_resource_keys={"warehouse"},
    out=Out(str, is_required=False),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"}
)
def append_newest_change_version(context, start_after, newest_change_version:Optional[int]):
    """
    Create a dataframe with two columns:
    timestamp column using job's run timestamp and
    newest change version. Call append_data() on
    warehouse resource to append new record to table.

    Args:
        newest_change_version (bool): The latest change
        version number returned from target Ed-Fi API.
    """
    if newest_change_version is not None:
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
    out=Out(Union[int, None]),
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "change_queries"}
)
def get_previous_change_version(context, school_year: int, use_change_queries: bool):
    """
    Run SQL query on table edfi_processed_change_versions
    to retrieve the change version number from the
    previous job run. If no data is returned, return -1.
    This will cause the extract step to pull all data from
    the target Ed-Fi API.
    """
    context.log.info(f"Use change queries is set to {use_change_queries}")
    if use_change_queries:
        query = f"""
            SELECT newest_change_version
            FROM `{{project_id}}.{{dataset}}.edfi_processed_change_versions`
            WHERE
                SchoolYear = {school_year}
                AND timestamp < TIMESTAMP '{datetime.now().isoformat()}'
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
    else:
        return None


@op(
    description="Retrieves data from the Ed-Fi API",
    required_resource_keys={"edfi_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    tags={"kind": "extract"},
)
def get_data(context, api_endpoint: Dict, school_year: int,
    previous_change_version:Optional[int]=None, newest_change_version:Optional[int]=None) -> Dict:
    """
    Retrieve data from API endpoint. For each record, add the run
    timestamp to store when the data was extracted.
    """
    output = api_endpoint.copy()
    output["records"] = list()
    retrieved_data = context.resources.edfi_api_client.get_data(
        output["endpoint"], school_year,
        previous_change_version, newest_change_version)

    for response in retrieved_data:

        if 'schoolYear' not in response:
            response['schoolYear'] = school_year

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
def load_data(context, api_endpoint_records: Dict, 
    school_year: int, use_change_queries: bool):
    """
    Load the passed in records retrieved from the
    specific API endpoint into the data warehouse.
    If change queries is used, files in gcs should
    be retained.
    """
    table_name = api_endpoint_records['table_name']
    table = context.resources.warehouse.load_data(
        table_name=table_name,
        school_year=school_year,
        gcs_path="edfi_api/",
        records=api_endpoint_records["records"],
        retain_gcs_files=use_change_queries)

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

        yield materialization

    yield Output(dbt_cli_edfi_output)
