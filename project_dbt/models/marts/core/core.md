
{% docs dim_date %}

# Date dim

A date dimension table can be found in almost every dimensional model and allows the analytics engineer to look at student performance across different time periods. An explicit date dimension table can also help store date attributes that are not supported by a SQL date function (ie. month sort order in the context of a school year).

This dimension table notably lacks the date key found in Ed-Fi's Analytics Middle Tier. This is also counter to what Kimball tells us to do. Instead, fact tables found in the marts that include a date, use the DATE type. Kimball argues that if a fact table does this, it will cause folks to use SQL functions on that date to extract items like month name and avoid the join to the date dimension when they need to retrieve such information. You should use the date if you need the date and join on the date dimension if you need more.

---------------------------
Differences from Ed-Fi AMT
* Removes `date_key`
* Adds `month_sort_order`

{% enddocs %}


{% docs dim_demographic %}

# Demographic dim

Grain: one row per descriptor parent key and descriptor

This table is usually joined to the student demographic bridge tables.

---------------------------
Differences from Ed-Fi AMT
* The descriptor's short description is used in place of code value for the demographic label
* Renames `demographic_parent_key` to `demographic_parent`

{% enddocs %}


{% docs dim_grading_period %}

# Grading period dim

Grain: one row per school per grading period


{% enddocs %}


{% docs dim_local_education_agency %}

# Local education agency dim

Grain: one row per local education agency


{% enddocs %}


{% docs dim_school %}

# School dim

Grain: one row per school per school year


{% enddocs %}


{% docs dim_section %}

# Section dim

Grain: one row per section per school

---------------------------
Differences from Ed-Fi AMT
* Removes `description`
* Adds `section_identifier` to allow for a natural key for section id


{% enddocs %}


{% docs dim_session %}

# Session dim

Grain: one row per school per session

---------------------------
Differences from Ed-Fi AMT
* Adds `total_instructional_days`
* Adds `session_begin_date`
* Adds `session_end_date`

{% enddocs %}


{% docs dim_student %}

# Student dim

Grain: one record per student

Ed-Fi's Analytics Middle Tier provides two student dims related to the student's LEA association and the student's school association. This dim combines those two dims to provide one student dim that can be used downstream. If a student has multiple school enrollments, only their most recent will show in this dim.

{% enddocs %}


{% docs dim_student_section %}

# Student section dim

Grain: `dim_student_section` has one record per student section per section enrollment. The `teacher_name` field is a concatenation of all teachers currently assigned to the class section.

---------------------------
Differences from Ed-Fi AMT
* Renames `subject` to `academic_subject`
* Adds `session_key` to enable join between student section dim and session dim


{% enddocs %}