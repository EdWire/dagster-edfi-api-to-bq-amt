
SELECT
    JSON_VALUE(data, '$.localEducationAgencyReference.localEducationAgencyId') AS local_education_agency_id,
    JSON_VALUE(data, '$.schoolId') AS school_id,
    JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution,
    SPLIT(JSON_VALUE(data, '$.schoolTypeDescriptor'), '#')[OFFSET(1)] AS school_type_descriptor,
FROM {{ source('raw_sources', 'edfi_schools') }}
