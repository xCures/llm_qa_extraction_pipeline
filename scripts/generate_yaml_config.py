import argparse
import yaml

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="YAML file to save")
    parser.add_argument("--match-keys", nargs="+", default=["subject_id", "document_id", "section", "extraction_schema", "section_extraction_id"], help="List of merge keys")
    parser.add_argument("--fields", nargs="+", required=True, help="Fields to compare, e.g. payer_name plan_name group_number")
    args = parser.parse_args()

    config = {
        "match_keys": args.match_keys,
        "fields": {field: field for field in args.fields}
    }

    with open(args.output, "w") as f:
        yaml.dump(config, f, sort_keys=False)

    print(f" Config saved to {args.output}")

if __name__ == "__main__":
    main()
