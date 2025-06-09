# llm_qa_extraction_pipeline
Workflow that pulls sandbox LLM clinical extractions, retrieves downstream FHIR mappings, and produces side-by-side QA reports to measure comparisons

# Process
1. **Pull sandbox data** from `sandbox.section_extraction`  
2. **Pull production (FHIR) data** for the same subjects  
3. **Compare** field-by-field with a YAML mapping  
4. Write a side-by-side CSV + per-field match summary

It supports any extractor schema and runs locally in SageMaker (or anywhere with Redshift Data-API access).

## Table of Contents
