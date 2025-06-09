import os
import time
import argparse 
import pandas as pd 
import boto3
from dotenv import load_dotenv
from datetime import datetime

def assume_role(role_arn: str, session_name: str):
    sts = boto3.client("sts")
    return sts.assume_role(RoleArn=role_arn,
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
        resp = client.execute_statement(WorkgroupName=workgroup,
                                        Database=database, Sql=sql)
    else:
        resp = client.execute_statement(ClusterIdentifier=workgroup,
                                        Database=database, Sql=sql)
    qid = resp["Id"]
    while True:
        desc   = client.describe_statement(Id=qid)
        status = desc["Status"]
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Query {qid} {status}\n"
                f"Redshift error: {desc.get('Error', 'no details')}"
            )
        if status == "FINISHED":
            break
        time.sleep(1)

    res   = client.get_statement_result(Id=qid)
    cols  = [c["name"] for c in res["ColumnMetadata"]]
    rows  = [[list(v.values())[0] if v else None for v in r] for r in res["Records"]]
    return pd.DataFrame(rows, columns=cols)

# ------------- main logic ----------------------------------------------------
load_dotenv()  # read .env alongside AWS creds chain

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subject-csv", required=True)
    ap.add_argument("--query-file",  required=True,
                    help="SQL file containing {{SUBJECT_IDS}} placeholder")
    ap.add_argument("--extractor",   required=True)
    args = ap.parse_args()

    # -- LOAD env ------------------------------------------------------------------
    region       = os.getenv("AWS_REGION", "us-west-2")
    workgroup    = os.getenv("REDSHIFT_WORKGROUP")          # set in .env file
    database     = os.getenv("REDSHIFT_DATABASE")           # set in .env file
    role_arn     = os.getenv("REDSHIFT_ROLE_ARN")
    session_name = os.getenv("REDSHIFT_SESSION_NAME", "fhir-query")

    creds  = assume_role(role_arn, session_name)
    rs_cli = create_rs_client(creds, region)

    # -- build IN list ----------------------------------------------------
    ids       = pd.read_csv(args.subject_csv)["subject_id"].dropna().unique()
    id_clause = ",".join(f"'{i}'" for i in ids)

    sql = open(args.query_file).read().replace("{{SUBJECT_IDS}}", id_clause)

    print("  Running Redshift FHIR query …")
    df = run_query(rs_cli, workgroup, database, sql, is_serverless=True)

    # -- output & save -----------------------------------------------------------------
    date_str = datetime.today().strftime("%Y-%m-%d")
    out_dir  = f"output/{date_str}/{args.extractor}"
    os.makedirs(out_dir, exist_ok=True)

    out_path = f"{out_dir}/prod_extractions.csv"
    df.to_csv(out_path, index=False)
    print(f"  Saved → {out_path}")

if __name__ == "__main__":
    main()
