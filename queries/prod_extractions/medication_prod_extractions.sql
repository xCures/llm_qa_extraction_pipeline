WITH extensions AS (
    SELECT
        m.id,
        e.url::text         AS url,
        e.valueString::text AS value
    FROM fhir.medication_statement m,
         m.extension e
)
SELECT
    m.id,
    m.subject_id,
    m.organization_name,
    m.project_name,
    m.subject_identifier,
    m.medication.code.coding[0].display::text           AS medication,
    TO_CHAR((m.effectiveperiod.start)::timestamp, 'MM/DD/YYYY') AS start_date,
    TO_CHAR((m.effectiveperiod.end)::timestamp,   'MM/DD/YYYY') AS end_date,
    m.status,
    m.reasoncode[0].text                                AS indication,
    m.reasoncode[0].coding[0].display                   AS coding_display,
    discontinuation_extension.value                     AS discontinuation_reason,
    m.dosage[0].patientinstruction                      AS dose_information,
    m.dosage[0].route.text                              AS route_text,
    m.dosage[0].route.coding[0].display                 AS route_display,
    o.medication_code,
    o.medication_coding_system,
    o.medication_concept_id,
    o.oncology_classification,
    SPLIT_PART(m.meta.source::text, ':', 6)             AS doc_section_code, 
    SPLIT_PART(m.meta.source::text, ':', 8)             AS document_id, 
    SPLIT_PART(m.meta.source::text, ':', 9)             AS section_extraction_id
FROM fhir.medication_statement m
LEFT JOIN extensions discontinuation_extension
       ON m.id = discontinuation_extension.id
      AND discontinuation_extension.url = 'urn:xcures:medicationDiscontinuationReason'
LEFT JOIN Clinical_Concepts.medications o
       ON m.id = o.id
WHERE SPLIT_PART(m.meta.source::text, ':', 4) = 'llm-extraction'
  AND m.subject_id IN ({{SUBJECT_IDS}});

