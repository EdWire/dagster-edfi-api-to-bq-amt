


WITH parsed_data AS (

    
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '$.cohortTypeDescriptorId') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM `gcp-proj-id`.`dev_staging`.`base_edfi_cohort_type_descriptors`
         UNION ALL 
    
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '$.disabilityDescriptorId') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM `gcp-proj-id`.`dev_staging`.`base_edfi_disability_descriptors`
         UNION ALL 
    
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '$.languageDescriptorId') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM `gcp-proj-id`.`dev_staging`.`base_edfi_language_descriptors`
         UNION ALL 
    
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '$.languageUseDescriptorId') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM `gcp-proj-id`.`dev_staging`.`base_edfi_language_use_descriptors`
         UNION ALL 
    
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '$.raceDescriptorId') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM `gcp-proj-id`.`dev_staging`.`base_edfi_race_descriptors`
         UNION ALL 
    
        SELECT
            JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '$.gradingPeriodDescriptorId') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM `gcp-proj-id`.`dev_staging`.`base_edfi_grading_period_descriptors`
        
    

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                namespace,
                code_value
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT DISTINCT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM `gcp-proj-id`.`dev_staging`.`stg_edfi_deletes` edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )