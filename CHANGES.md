# Changelog

# v0.4.1

### New

- [dbt] Adds `month_sort_order` column to date dim
- [dbt] Updates student attendance fact table to show 'In Attendance' as the attendance event descriptor when no negative attendance exists
- [dbt] Adds user school authorization table for row level security

### Breaking Changes

- [dbt] Fixes bug that flagged all students as early warning for attendance
- [dbt] School year is now an int in all data models


# v0.4.0

### New

- [dbt] Adds labels noting which data models are a part of Ed-Fi's Analytics Middle Tier
- [dbt] Adds a new `stg_student_attendance` fact table that joins on various dims to add contextual information
- [dbt] Adds student attendance metric for chronically absent and early warning

### Breaking Changes

- [dagster] Adds `base_` prefix to all BigQuery tables created in Dagster job
- [dbt] Updates naming convention of data models to match [bootcamp article](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/elt_layers.md)
- [dbt] Renames `attendance_fact` table to `student_attendance_fact`


# v0.3.2

### New

- [dbt] Updates `stg_student_assessment_fact` to use nested, repeated fields
- [dbt] Refactors dbt structure to match [bootcamp article](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/elt_layers.md)
- [dagster] Updates BigQuery permissions to be more restrictive
- [dagster] Updates Dagster to v0.13.14


# v0.3.1

### New

- [dagster] Adds `/programs` to Ed-Fi API extract
- [dagster] Adds `/studentProgramAssociations` to Ed-Fi API extract
- [dagster] Adds `/studentSpecialEducationProgramAssociations` to Ed-Fi API extract
- [dbt] Adds Analytics Middle Tier Assessment and Student Assessment fact tables
- [dbt] Creates native BigQuery table for `/programs` API spec
- [dbt] Creates native BigQuery table for `/studentProgramAssociations` API spec
- [dbt] Creates native BigQuery table for `/studentSpecialEducationProgramAssociations` API spec
- [dbt] Creates `student_dim` view that pre-joins several tables into one combined student dimension
- [dbt] Creates `stg_student_assessment_fact` that joins `assessment_fact` and `student_dim` to provide an easy to use staging fact table


# v0.3.0

### New

- [dagster] Moves Ed-Fi API page limit variable under `edfi_api_client` resource config.
- [dagster] Adds Ed-Fi API mode variable to `edfi_api_client` resource config.
- [dagster] Adds `school_year` as input variable to specify school year scope of data pull.
- [dagster] Updates Google Cloud Storage folder structure to store each school year in its own folder under the `edfi_api` folder.
- [dagster] Updates source URIs in BigQuery external tables to allow for querying multiple school years of data.
- [dbt] Updates dbt SQL to factor in multiple school years of data.
- [dbt] Adds documentation and tests.
