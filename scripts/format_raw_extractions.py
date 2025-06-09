import argparse, 
import json, 
import os, 
import pandas as pd
from datetime import datetime

def explode_response(df: pd.DataFrame, col: str = "response") -> pd.DataFrame:
    records = []
    for idx, raw in df[col].dropna().items():
        try:
            for item in json.loads(raw):
                rec = json.loads(item)
                rec["__row_id"] = idx
                records.append(rec)
        except Exception as e:
            print(f" Row {idx}: {e}")
    return pd.json_normalize(records)

# ---------- main logic ---------------------------------------------------------#
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",      required=True, help="Path to exported CSV")
    ap.add_argument("--extractor",  required=True, help="Schema name, e.g. medication")
    ap.add_argument("--output-dir", default="output", help="Top-level output folder")
    args = ap.parse_args()

    #robust CSV read
    raw_df = pd.read_csv(args.input, engine="python", on_bad_lines="skip")

    #explode JSON array
    expanded = explode_response(raw_df, "response")

    #metadata columns to use
    meta_cols_all = [
        "organization_id","project_id","subject_id","document_id",
        "section_type","section","created","model_id",
        "extraction_schema","id" #id is extraction_section_id
    ]
    meta_cols = [c for c in meta_cols_all if c in raw_df.columns]

    #merge + reorder
    meta  = raw_df[meta_cols].reset_index().rename(columns={"index": "__row_id"})
    final = expanded.merge(meta, on="__row_id", how="left").drop(columns="__row_id")
    final = final[meta_cols + [c for c in final.columns if c not in meta_cols]]

    #output path
    dated_outdir = os.path.join(
        args.output_dir,
        datetime.today().strftime("%Y-%m-%d"),
        args.extractor
    )
    os.makedirs(dated_outdir, exist_ok=True)
    out_path = os.path.join(dated_outdir, "raw_extractions.csv") 

    if final.empty:
        print(" No rows after exploding; nothing written.")
    else:
        final.to_csv(out_path, index=False)
        print(f" Saved → {out_path}")

if __name__ == "__main__":
    main()
