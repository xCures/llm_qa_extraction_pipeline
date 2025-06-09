import argparse
import os
import pandas as pd
import yaml
from datetime import datetime

def to_string(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")

def normalise(val):
    return str(val).strip().lower() if pd.notnull(val) else ""

def compare_cols(df, raw_col, prod_col, match_col):
    df[match_col] = df.apply(
        lambda r: "match" if normalise(r[raw_col]) == normalise(r[prod_col]) else "no_match",
        axis=1
    )

def summary_table(df, field_pairs):
    rows = []
    for raw_col, prod_col, match_col in field_pairs:
        rows.append({
            "field": raw_col.replace("raw_", ""),
            "matches":     (df[match_col] == "match").sum(),
            "mismatches":  (df[match_col] == "no_match").sum(),
            "null_in_raw":  df[raw_col].isna().sum(),
            "null_in_prod": df[prod_col].isna().sum(),
        })
    return pd.DataFrame(rows)

# --------- main logic ----------------------------------------------------------------(still testing)--#
def main(args):
    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    raw  = pd.read_csv(args.raw_csv)
    prod = pd.read_csv(args.prod_csv)

    # unify merge-key dtypes
    to_string(raw,  cfg["match_keys"])
    to_string(prod, cfg["match_keys"])

    # merge
    merged = pd.merge(
        raw, prod,
        on        = cfg["match_keys"],
        suffixes  = ("_raw", "_prod"),
        how       = "outer"
    )

    field_pairs = []

    for label, mapping in cfg["fields"].items():
        # mapping can be a str or a dict {raw: …, prod: …}
        if isinstance(mapping, dict):
            raw_col_name  = mapping["raw"]
            prod_col_name = mapping["prod"]
        else:
            raw_col_name = prod_col_name = mapping

        # columns after merge carry the suffixes
        raw_col_merged  = f"{raw_col_name}_raw"
        prod_col_merged = f"{prod_col_name}_prod"

        # rename to raw_<label>, prod_<label>
        merged.rename(
            columns = {
                raw_col_merged:  f"raw_{label}",
                prod_col_merged: f"prod_{label}"
            },
            inplace = True,
        )

        # add match column
        compare_cols(merged, f"raw_{label}", f"prod_{label}", f"{label}_match")
        field_pairs.append((f"raw_{label}", f"prod_{label}", f"{label}_match"))

    # ---- output & save-------------------------------------------------------#
    date_str = datetime.today().strftime("%Y-%m-%d")
    out_dir  = f"output/{date_str}/{args.extractor}"
    os.makedirs(out_dir, exist_ok=True)

    comp_path = f"{out_dir}/final_comparison.csv"
    merged.to_csv(comp_path, index=False)
    print(f" Saved comparison CSV → {comp_path}")

    if args.summary:
        summary_df = summary_table(merged, field_pairs)
        summary_path = f"{out_dir}/summary_counts.csv"
        summary_df.to_csv(summary_path, index=False)
        print(f"  Saved summary counts → {summary_path}")

# ------------------------ CLI ------------------------------------------------#
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--raw-csv",  required=True)
    p.add_argument("--prod-csv", required=True)
    p.add_argument("--extractor", required=True)
    p.add_argument("--config",  required=True)
    p.add_argument("--summary", action="store_true")
    args = p.parse_args()
    main(args)
