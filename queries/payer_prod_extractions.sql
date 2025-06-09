SELECT
    coverage.id,
    coverage.organization_name,
    TO_CHAR(coverage.normalizer_timestamp, 'MM/DD/YYYY') AS version_last_updated,
    coverage.organization_name,
    coverage.subject_id,
    SPLIT_PART(coverage.meta.source::text, ':', 4) AS source,
    SPLIT_PART(coverage.meta.source::text, ':', 9) AS section_extraction_id,
    coverage.extension[0].valuestring AS document_id,
    coverage.type."text" AS type,
    coverage.subscriberid AS subscriber_id,
    TO_CHAR(CAST(coverage.period.start AS TIMESTAMP), 'MM/DD/YYYY') AS effective_date_start,
    TO_CHAR(CAST(coverage.period.end AS TIMESTAMP), 'MM/DD/YYYY') AS effective_date_end,
    coverage.class[0].type.text AS insurance_type,
    coverage.class[0].value AS group_number,
    coverage.class[0].name AS plan_name,
    coverage.relationship."text" AS guarantor_relation_to_patient,
    organization.name AS payer_name,
    organization.address[0].line[0] AS address, 
    organization.telcom[0].line[0] AS phone
FROM fhir.coverage
LEFT JOIN fhir.organization ON SPLIT_PART(coverage.payor[0].reference::text, '/', 2) = organization.id
WHERE
    SPLIT_PART(coverage.meta.source::text, ':', 4) = 'llm-extraction'
    AND coverage.subject_id IN ({{SUBJECT_IDS}});
