# LLM QA Extraction Pipeline
Workflow that pulls sandbox LLM clinical extractions, retrieves downstream FHIR mappings, and produces side-by-side QA reports to measure comparisons

# Process
1. Pull sandbox data from the sandbox for a set of subjects.
2. Pull production (FHIR) data for the same subjects.  
3. Compares field-by-field with a YAML mapping.  
4. Write a side-by-side CSV + per-field match summary.

It supports any extractor schema and runs locally in SageMaker (or anywhere with Redshift Data-API access).

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Usage](#usage)
3. [Comparison Configs](#comparison-configs)  
4. [Redshift Queries](#redshift-queries)  
5. [Makefile](#makefile)

## Prerequisites
- Python 3.9+
- Access to Redshift Data API and the appropriate SageMaker role (for FHIR extraction).
- A list of subject IDs (`subject_ids.csv`).
- YAML config defining comparison logic (see `configs/` folder).

## Usage
| Script                          | Purpose                                                                                          |
|----------------------------------|--------------------------------------------------------------------------------------------------|
| `scripts/run_sandbox.py`         | Pulls LLM-generated extractions from the sandbox Redshift table for a list of subject IDs. Optionally filtered by `--CREATED:` date. |
| `scripts/run_prod.py`            | Queries production (FHIR) Redshift data for the same subjects using a SQL template and outputs mapped values. Optionally filtered by `--CREATED:` date. |
| `scripts/run_compare.py`         | Performs field-by-field comparisons between sandbox and production data using a YAML config. Outputs side-by-side CSV + match summary. Optionally filtered by `--CREATED:` date. |
| `scripts/format_raw_extractions.py` | Accepts a CSV exported from Redshift and formats it to match the expected sandbox schema (with one row per extraction value). |

## Comparison Configs
Comparison logic is defined in YAML config files, located in the `configs/` directory. These configs specify which fields to compare between the sandbox extractions and production (FHIR) mappings.

Each config file defines:
- **labels**: List of field names to compare.
- **normalizers** *(optional)*: Preprocessing functions like lowercasing, date standardization, etc.
- **field mappings**: Aligns differently named fields in sandbox vs production.
- **matchers**: sandbox and production rows are compared using subject_id, document_id, and sandbox_extraction_id (i.e. the fingerprint id).

You can use `scripts/generate_yaml_config.py` to create a YAML configuration file for comparing specific fields between the sandbox and production extractions. 

Example Usage:
`python3 scripts/generate_yaml_config.py \
  --output configs/payer_compare.yaml \
  --fields payer_name plan_name group_number subscriber_id`

## Redshift Queries
The pipeline expects a production Redshift query to retrieve FHIR-mapped values for comparison. These queries live in: `queries/prod_extractions/{extractor_name}_prod_extractions.sql`. Each query should include:

- Placeholder for subject_ids: `WHERE subject_id IN ({{SUBJECT_IDS}})`
- Fingerprint Unnesting: `SPLIT_PART(m.meta.source::text, ':', 9) AS section_extraction_id`
- Any relevant fields to have the same name as what is in the sandbox extractions

## Makefile Usage
The `Makefile` provides these targets:

- **run**: Full pipeline (sandbox → FHIR → compare)  
- **raw**: Pull sandbox extractions only  
- **prod**: Pull production (FHIR) values only  
- **compare**: Compare sandbox vs production only
- **clean**: Delete today’s output for an extractor  

### Full pipeline
`make run \
  EXTRACTOR=diagnosis \
  SUBJECTS=subject_ids.csv \
  CONFIG=configs/diagnosis_compare.yaml \
  CREATED=YYYY-MM-DD #optional date filter`

### Sandbox
`make raw \
  EXTRACTOR=diagnosis \
  SUBJECTS=subject_ids.csv \
  CREATED=YYYY-MM-DD #optional date filter`

### Production
`make prod \
  EXTRACTOR=diagnosis \
  SUBJECTS=subject_ids.csv \
  CREATED=YYYY-MM-DD` #optional date filter`

### Comparison
`make compare \
  EXTRACTOR=diagnosis \
  CONFIG=configs/diagnosis_compare.yaml \
  CREATED=YYYY-MM-DD #optional date filter`

### Clean
`make clean \
  EXTRACTOR = diagnosis`

All outputs are saved under `output/YYYY-MM-DD/<extractor>/`
