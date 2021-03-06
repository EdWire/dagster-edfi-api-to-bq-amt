
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.identificationCode') AS identification_code,
        SPLIT(JSON_VALUE(data, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
        JSON_VALUE(data, '$.description') AS description,
        CAST(JSON_VALUE(data, '$.maxRawScore') AS float64) AS max_raw_score,
        CAST(JSON_VALUE(data, '$.percentOfAssessment') AS float64) AS percent_of_assessment,
        JSON_VALUE(data, '$.nomenclature') AS nomenclature,
        STRUCT(
            JSON_VALUE(data, '$.assessmentReference.assessmentIdentifier') AS assessment_identifier,
            JSON_VALUE(data, '$.assessmentReference.namespace') AS namespace
        ) AS assessment_reference,
        STRUCT(
            JSON_VALUE(data, '$.parentObjectiveAssessmentReference.assessmentIdentifier') AS assessment_identifier,
            JSON_VALUE(data, '$.parentObjectiveAssessmentReference.identificationCode') AS identification_code,
            JSON_VALUE(data, '$.parentObjectiveAssessmentReference.namespace') AS namespace
        ) AS parent_objective_assessment_reference,
        ARRAY(
            SELECT AS STRUCT 
                JSON_VALUE(assessment_items, '$.assessmentItemReference.assessmentIdentifier') AS assessment_identifier,
                JSON_VALUE(assessment_items, '$.assessmentItemReference.identificationCode') AS identification_code,
                JSON_VALUE(assessment_items, '$.assessmentItemReference.namespace') AS namespace
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.assessmentItems")) assessment_items 
        ) AS assessment_items,
        ARRAY(
            SELECT AS STRUCT
                STRUCT(
                    JSON_VALUE(learning_objectives, '$.learningObjectiveReference.learningObjectiveId') AS learning_objective_id,
                    JSON_VALUE(learning_objectives, '$.learningObjectiveReference.namespace') AS namespace
                ) AS learning_objective_reference
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.learningObjectives")) learning_objectives 
        ) AS learning_objectives,
        ARRAY(
            SELECT AS STRUCT
                STRUCT(
                    JSON_VALUE(learning_standards, '$.learningStandardReference.learningStandardId') AS learning_standard_id
                ) AS learning_standard_reference 
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.learningStandards")) learning_standards
        ) AS learning_standards,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                SPLIT(JSON_VALUE(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] AS performance_level_descriptor,
                SPLIT(JSON_VALUE(performance_levels, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
                JSON_VALUE(performance_levels, "$.maximumScore") AS maximum_score,
                JSON_VALUE(performance_levels, "$.minimumScore") AS minimum_score
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.performanceLevels")) performance_levels 
        ) AS performance_levels,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(scores, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                SPLIT(JSON_VALUE(scores, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
                JSON_VALUE(scores, "$.maximumScore") AS maximum_score,
                JSON_VALUE(scores, "$.minimumScore") AS minimum_score
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.scores")) scores 
        ) AS scores
    FROM {{ source('staging', 'base_edfi_objective_assessments') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                identification_code,
                assessment_reference.assessment_identifier,
                assessment_reference.namespace
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
