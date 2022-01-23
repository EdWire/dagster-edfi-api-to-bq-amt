{% docs dim_date %}

# Date dim

A date dimension table can be found in almost every dimensional model and allows the analytics engineer to look at student performance across different time periods. An explicit date dimension table can also help store date attributes that are not supported by a SQL date function (ie. month sort order in the context of a school year).

This dimension table notably lacks the date key found in Ed-Fi's Analytics Middle Tier. This is also counter to what Kimball tells us to do. Instead, fact tables found in the marts that include a date, use the DATE type. Kimball argues that if a fact table does this, it will cause folks to use SQL functions on that date to extract items like month name and avoid the join to the date dimension when they need to retrieve such information. You should use the date if you need the date and join on the date dimension if you need more.

---------------------------
Differences from Ed-Fi AMT
* Removed `date_key`
* Added `month_sort_order`

{% enddocs %}


{% docs dim_demographic %}

# Demographic dim

This dimension table contains a row per descriptor parent key and descriptor. This table is usually joined to the student demographic bridge tables.

---------------------------
Differences from Ed-Fi AMT
* The descriptor's short description is used in place of code value for the demographic label

{% enddocs %}


{% docs dim_grading_period %}

# Grading period dim

This dimension table contains one row per school per grading period.

<!-- ---------------------------
Differences from Ed-Fi AMT
* Renames `description` -->

{% enddocs %}


{% docs dim_section %}

# Section dim

Grain: `dim_section` has one record per section per school.

---------------------------
Differences from Ed-Fi AMT
* Removed `description`
* Added `section_identifier` to allow for a natural key for section id


{% enddocs %}

{% docs dim_student_section %}

# Student section dim

Grain: `dim_student_section` has one record per student section per section enrollment. The `teacher_name` field is a concatenation of all teachers currently assigned to the class section.

---------------------------
Differences from Ed-Fi AMT
* Renames `subject` to `academic_subject`


{% enddocs %}