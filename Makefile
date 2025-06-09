# Usage:
# make run EXTRACTOR=radiation-v3 SUBJECTS=subject_ids.csv CONFIG=configs/radiation-v3_compare.yaml CREATED=YYYY-MM-DD

SUBJECTS ?= subject_ids.csv
EXTRACTOR ?= extractor-not-set
CONFIG ?= configs/$(EXTRACTOR)_compare.yaml
FHIR_SQL ?= queries/prod_extractions/$(EXTRACTOR)_prod_extractions.sql
CREATED ?=

ifeq ($(strip $(CREATED)),)
DATE := $(shell date +%Y-%m-%d)
else
DATE := $(CREATED)
endif

RAW_OUT  := output/$(DATE)/$(EXTRACTOR)/raw_extractions.csv
PROD_OUT := output/$(DATE)/$(EXTRACTOR)/prod_extractions.csv
RAW_CREATED_ARG := $(if $(CREATED),--created $(CREATED),)

.PHONY: run raw prod compare clean check

run: check raw prod compare

check:
	@if [ "$(EXTRACTOR)" = "extractor-not-set" ]; then \
		echo " Please specify EXTRACTOR=..."; exit 1; fi

raw:
	python3 scripts/run_sandbox.py \
		--subject-csv $(SUBJECTS) \
		--extractor $(EXTRACTOR) \
		$(RAW_CREATED_ARG)

prod:
	python3 scripts/run_prod.py \
		--subject-csv $(SUBJECTS) \
		--query-file $(FHIR_SQL) \
		--extractor $(EXTRACTOR)

compare:
	python3 scripts/run_compare.py \
		--raw-csv $(RAW_OUT) \
		--prod-csv $(PROD_OUT) \
		--extractor $(EXTRACTOR) \
		--config $(CONFIG) \
		--summary

clean:
	rm -rf output/$(DATE)/$(EXTRACTOR)

