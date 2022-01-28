{% docs fct_student_attendance %}

# Student attendance fact

Grain: one row per student per instructional day they hold an enrollment

Used for looking at a student's attendance per day. This fact table provides a row for each instructional day the student holds an enrollment up to the previous date.

---------------------------
Differences from Ed-Fi AMT
* Adds `event_duration` columns
* Addes `` column

{% enddocs %}