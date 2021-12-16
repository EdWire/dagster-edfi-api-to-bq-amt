import os
from datetime import datetime

from dagster import (
    fs_io_manager,
    graph,
    multiprocess_executor
)

from dagster_dbt import dbt_cli_resource, dbt_run_op, dbt_test_op
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource

from ops.edfi import *

from resources.edfi_api_resource import edfi_api_resource_client
from resources.dw_resource import bq_client


@graph(
    name="edfi_api_to_amt",
    description=(
        "Gets data from the Ed-Fi API and "
        "loads to BigQuery. Runs dbt models "
        "after data is loaded to transform data "
        "to the Analytics Middle Tier data model."
    )
)
def edfi_api_to_amt(use_change_queries):

    previous_change_version = get_previous_change_version()
    newest_change_version = get_newest_api_change_versions(use_change_queries)

    result = api_endpoint_generator.alias('api_endpoint_generator')(use_change_queries=use_change_queries).map(
        lambda mapped_value: get_data(
            api_endpoint=mapped_value,
            previous_change_version=previous_change_version,
            newest_change_version=newest_change_version
        )
    ).map(load_data).collect()

    append_newest_change_version(
        start_after=result, newest_change_version=newest_change_version)

    # result = api_endpoint_generator.alias('api_endpoint_generator')(api_endpoints, use_change_queries=do_not_use_change_queries).map(get_data).map(load_data).collect()
    
    dbt_run_result = dbt_run_op(start_after=result)
    dbt_test_op(start_after=dbt_run_result)


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


edfi_api_dev_job = edfi_api_to_amt.to_job(
    tags={ "run_timestamp": datetime.now().isoformat() },
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": fs_io_manager,
        "edfi_api_client": edfi_api_resource_client.configured({
            "base_url": os.getenv("EDFI_BASE_URL"),
            "api_key": os.getenv("EDFI_API_KEY"),
            "api_secret": os.getenv("EDFI_API_SECRET"),
            "school_year": 1901 # tells job to not include year in URL
        }),
        "warehouse": bq_client.configured({
            "staging_gcs_bucket": os.getenv("GCS_BUCKET_DEV", ""),
            "dataset": "dev_raw_sources",
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "dev",
            "models": ["+date_dim", "+date_dim", "+demographic_dim",
                       "+grading_period_dim", "+local_education_agency_dim", "+rls_student_data_authorization",
                       "+rls_user_authorization", "+rls_user_dim", "+rls_user_student_data_authorization",
                       "+school_dim", "+student_local_education_agency_demographics_bridge", "+student_local_education_agency_dim",
                       "+student_school_dim", "+student_section_dim"]
        })
    },
    config={
        "inputs": {
            "use_change_queries": {
                "value": True
            }
        },
        "ops": {
            "api_endpoint_generator": {
                "inputs": {
                    "api_endpoints": edfi_api_endpoints
                }
            }
        }
    },
)


# edfi_api_prod_job = edfi_api_to_amt.to_job(
#     executor_def=multiprocess_executor.configured({
#         "max_concurrent": 8
#     }),
#     resource_defs={
#         "gcs": gcs_resource,
#         "io_manager": gcs_pickle_io_manager.configured({
#             "gcs_bucket": os.getenv("GCS_BUCKET_PROD", ""),
#             "gcs_prefix": "edfi_api"
#         }),
#         "edfi_api_client": edfi_api_resource_client.configured({
#             "base_url": os.getenv("EDFI_BASE_URL"),
#             "api_key": os.getenv("EDFI_API_KEY"),
#             "api_secret": os.getenv("EDFI_API_SECRET"),
#             "school_year": 1901
#         }),
#         "warehouse": bq_client.configured({
#             "staging_gcs_bucket": os.getenv("GCS_BUCKET_PROD", ""),
#             "dataset": "prod_raw_sources",
#         }),
#         "dbt": dbt_cli_resource.configured({
#             "project_dir": os.getenv("DBT_PROJECT_DIR"),
#             "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
#             "target": "dev",
#             "models": ["edfi_local_education_agencies", "edfi_schools", 
#                        "edfi_students", "edfi_student_education_organization_associations",
#                        "edfi_student_school_associations"]
#         })
#     },
#     config={
#         "ops": {
#             "api_endpoint_generator": {
#                 "inputs": {
#                     "api_endpoints": edfi_api_endpoints
#                 }
#             }
#         }
#     },
# )
