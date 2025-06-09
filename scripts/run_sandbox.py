import os
import time
import json
import argparse
import pandas as pd
import boto3
from dotenv import load_dotenv
from datetime import datetime

def assume_role(role_arn: str, session_name: str):
    sts = boto3.client("sts")
    return sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name)["Credentials"]

def create_rs_client(creds: dict, region: str):
    return boto3.client(
        "redshift-data",
        region_name      = region,
        aws_access_key_id= creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token   =creds["SessionToken"],
    )

def run_query(client, workgroup, database, sql, is_serverless: bool = True):
    if is_serverless:
        resp = client.execute_statement(WorkgroupName=workgroup, Database=database, Sql=sql)
    else:
        resp = client.execute_statement(ClusterIdentifier=workgroup, Database=database, Sql=sql)
    qid = resp["Id"]

    while True:
        status = client.describe_statement(Id=qid)["Status"]
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Query {qid} failed")
        if status == "FINISHED":
            break
        time.sleep(1)

    res   = client.get_statement_result(Id=qid)
    cols  = [c["name"] for c in res["ColumnMetadata"]]
    rows  = [[list(v.values())[0] if v else None for v in r] for r in res["Records"]]
    return pd.DataFrame(rows, columns=cols)

def explode_response(df: pd.DataFrame, col: str = "response") -> pd.DataFrame:
    records = []
    for idx, raw in df[col].dropna().items():
        try:
            for item in json.loads(raw):
                inner = json.loads(item)
                inner["__row_id"] = idx
                records.append(inner)
        except Exception as e:
            print(f"  Row {idx}: {e}")
    return pd.json_normalize(records)

# -------- main logic ------------------------------------------------------------------- #
load_dotenv()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subject-csv", required=True, help="CSV with column subject_id")
    ap.add_argument("--extractor",   required=True, help="Extraction schema, e.g. payer-v2")
    ap.add_argument("--created",  help ="YYYY-MM-DD → filter Created date")
    args = ap.parse_args()

    region        = os.getenv("AWS_REGION", "us-west-2")
    workgroup     = os.getenv("REDSHIFT_WORKGROUP")           
    database      = os.getenv("REDSHIFT_DATABASE")       
    role_arn      = os.getenv("REDSHIFT_ROLE_ARN")
    session_name  = os.getenv("REDSHIFT_SESSION_NAME", "sandbox-query")

    print(f"  Assuming role {role_arn} …")
    creds   = assume_role(role_arn, session_name)
    rs_cli  = create_rs_client(creds, region)
    print(f"  Region={region}  Workgroup={workgroup}")

    # --- SQL SCRIPT----------------------------------------------------------------#
    ids = pd.read_csv(args.subject_csv)["subject_id"].dropna().unique()
    id_clause = ",".join(f"'{i}'" for i in ids)

    date_filter = f"AND DATE(created) = DATE '{args.created}'" if args.created else ""
    sql = f"""
    SELECT *,
           id AS section_extraction_id,
           TO_CHAR(created, 'MM/DD/YYYY') AS created_fmt
    FROM   sandbox.section_extraction
    WHERE  subject_id IN ({id_clause})
      AND  extraction_schema = '{args.extractor}'
      {date_filter}
    ORDER  BY created DESC
    """

    print("  Running Redshift query …")
    raw_df = run_query(rs_cli, workgroup, database, sql, is_serverless=True)

    # --- FORMAT FIX -----------------------------------------------------------#
    expanded = explode_response(raw_df, "response")

    meta_cols = [
        "organization_id","project_id","subject_id","document_id",
        "section_type","section","created","model_id",
        "extraction_schema","section_extraction_id"
    ]
    meta = raw_df[meta_cols].reset_index().rename(columns={"index": "__row_id"})
    final = expanded.merge(meta, on="__row_id", how="left").drop(columns="__row_id")

    # reorder
    front = meta_cols
    final = final[front + [c for c in final.columns if c not in front]]

    # --- output & save ---------------------------------------------------------------#
    out_dir = f"output/{datetime.today():%Y-%m-%d}/{args.extractor}"
    os.makedirs(out_dir, exist_ok=True)

    if final.empty:
        print(" No rows matched given subject_ids / created date — nothing saved.")
    else:
        out_path = f"{out_dir}/raw_extractions.csv"
        final.to_csv(out_path, index=False)
        print(f" saved → {out_path}")

if __name__ == "__main__":
    main()
