SELECT *,
  id AS section_extraction_id
FROM sandbox.section_extraction
WHERE subject_id IN ({{SUBJECT_IDS}})
  AND extraction_schema = '{{EXTRACTION_SCHEMA}}'
ORDER BY created DESC;
