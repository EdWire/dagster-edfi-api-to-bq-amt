
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.studentAssessmentIdentifier') AS student_assessment_identifier,
        EXTRACT(DATE FROM PARSE_TIMESTAMP('%Y-%m-%dT%TZ', JSON_VALUE(data, '$.administrationDate'))) AS administration_date,
        -- administrationEndDate
        SPLIT(JSON_VALUE(data, "$.administrationEnvironmentDescriptor"), '#')[OFFSET(1)] AS administration_environment_descriptor,
        SPLIT(JSON_VALUE(data, "$.administrationLanguageDescriptor"), '#')[OFFSET(1)] AS administration_language_descriptor,
        SPLIT(JSON_VALUE(data, "$.eventCircumstanceDescriptor"), '#')[OFFSET(1)] AS event_circumstance_descriptor,
        SPLIT(JSON_VALUE(data, "$.platformTypeDescriptor"), '#')[OFFSET(1)] AS platform_type_descriptor,
        SPLIT(JSON_VALUE(data, "$.reasonNotTestedDescriptor"), '#')[OFFSET(1)] AS reason_not_tested_descriptor,
        SPLIT(JSON_VALUE(data, "$.retestIndicatorDescriptor"), '#')[OFFSET(1)] AS retest_indicator_descriptor,
        SPLIT(JSON_VALUE(data, "$.whenAssessedGradeLevelDescriptor"), '#')[OFFSET(1)] AS when_assessed_grade_level_descriptor,
        JSON_VALUE(data, '$.eventDescription') AS event_description,
        JSON_VALUE(data, '$.serialNumber') AS serial_number,
        STRUCT(
            JSON_VALUE(data, '$.assessmentReference.assessmentIdentifier') AS assessment_identifier,
            JSON_VALUE(data, '$.assessmentReference.namespace') AS namespace
        ) AS assessment_reference,
        STRUCT(
            CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
        ) AS school_year_type_reference,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(score_results, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                SPLIT(JSON_VALUE(score_results, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
                JSON_VALUE(score_results, '$.result') AS result
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.scoreResults")) score_results 
        ) AS score_results,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(accommodations, "$.accommodationDescriptor"), '#')[OFFSET(1)] AS accommodation_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.accommodations")) accommodations 
        ) AS accommodations,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(items, "$.assessmentItemResultDescriptor"), '#')[OFFSET(1)] AS assessment_item_result_descriptor,
                SPLIT(JSON_VALUE(items, "$.responseIndicatorDescriptor"), '#')[OFFSET(1)] AS response_indicator_descriptor,
                JSON_VALUE(items, '$.assessmentResponse') AS assessment_response,
                JSON_VALUE(items, '$.descriptiveFeedback') AS descriptive_feedback,
                CAST(JSON_VALUE(items, '$.rawScoreResult') AS float64) AS raw_score_result,
                JSON_VALUE(items, '$.timeAssessed') AS time_assessed
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.items")) items 
        ) AS items,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                SPLIT(JSON_VALUE(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] AS performance_level_descriptor,
                CAST(JSON_VALUE(performance_levels, "$.performanceLevelMet") AS BOOL) AS performance_level_met
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.performanceLevels")) performance_levels 
        ) AS performance_levels,
        ARRAY(
                SELECT AS STRUCT
                    STRUCT(
                            JSON_VALUE(assessments, '$.objectiveAssessmentReference.assessmentIdentifier') AS assessment_identifier,
                            JSON_VALUE(assessments, '$.objectiveAssessmentReference.identificationCode') AS identification_code,
                            JSON_VALUE(assessments, '$.objectiveAssessmentReference.namespace') AS namespace
                    ) AS objective_assessment_reference,
                    ARRAY(
                        SELECT AS STRUCT 
                            SPLIT(JSON_VALUE(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                            SPLIT(JSON_VALUE(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] AS performance_level_descriptor,
                            CAST(JSON_VALUE(performance_levels, "$.performanceLevelMet") AS BOOL) AS performance_level_met
                        FROM UNNEST(JSON_QUERY_ARRAY(assessments, "$.performanceLevels")) performance_levels 
                    ) AS performance_levels,
                    ARRAY(
                        SELECT AS STRUCT 
                            SPLIT(JSON_VALUE(score_results, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                            SPLIT(JSON_VALUE(score_results, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
                            JSON_VALUE(score_results, '$.result') AS result
                        FROM UNNEST(JSON_QUERY_ARRAY(assessments, "$.scoreResults")) score_results 
                    ) AS score_results
                FROM UNNEST(JSON_QUERY_ARRAY(data, "$.studentObjectiveAssessments")) assessments
        ) AS student_objective_assessments,
    FROM {{ source('staging', 'base_edfi_student_assessments') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                assessment_reference.assessment_identifier,
                assessment_reference.namespace,
                student_assessment_identifier,
                student_reference.student_unique_id
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
