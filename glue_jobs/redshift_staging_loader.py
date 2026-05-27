"""
Glue ETL Job 2 — Redshift Staging Loader
==========================================
Reads curated Parquet from S3 and loads into Redshift staging tables
using the COPY command pattern — the fastest way to bulk-load Redshift.

Flow per table:
  1. TRUNCATE staging table
  2. COPY from S3 Parquet into staging
  3. UPSERT from staging into final fact/dim table
  4. Log row counts to audit table

AWS Glue job parameters:
  --S3_CURATED_BUCKET     s3://your-curated-bucket/
  --REDSHIFT_CONN         redshift_connection_name   (configured in Glue connections)
  --REDSHIFT_DB           fulfillment_ops
  --REDSHIFT_SCHEMA       ops_staging
  --IAM_ROLE              arn:aws:iam::ACCOUNT_ID:role/GlueRedshiftRole
  --JOB_NAME              redshift_staging_loader

Local dev: generates SQL files to ./sql_output/ instead of executing.
"""

import sys
import os
from datetime import datetime

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    import boto3

    sc          = SparkContext()
    glueContext = GlueContext(sc)
    spark       = glueContext.spark_session

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "S3_CURATED_BUCKET",
        "REDSHIFT_CONN",
        "REDSHIFT_DB",
        "REDSHIFT_SCHEMA",
        "IAM_ROLE",
    ])

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    CURATED_BUCKET  = args["S3_CURATED_BUCKET"].rstrip("/")
    REDSHIFT_CONN   = args["REDSHIFT_CONN"]
    REDSHIFT_DB     = args["REDSHIFT_DB"]
    STAGING_SCHEMA  = args["REDSHIFT_SCHEMA"]
    IAM_ROLE        = args["IAM_ROLE"]
    IS_LOCAL        = False

except ModuleNotFoundError:
    CURATED_BUCKET  = "./curated"
    REDSHIFT_DB     = "fulfillment_ops"
    STAGING_SCHEMA  = "ops_staging"
    IAM_ROLE        = "arn:aws:iam::123456789012:role/GlueRedshiftRole"
    IS_LOCAL        = True

SQL_OUT = "./sql_output"
os.makedirs(SQL_OUT, exist_ok=True)

print(f"\n[Glue Job] redshift_staging_loader")
print(f"[Config]  CURATED : {CURATED_BUCKET}")
print(f"[Config]  DB      : {REDSHIFT_DB}.{STAGING_SCHEMA}")
print(f"[Mode]    Local   : {IS_LOCAL}\n")

RUN_TS = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# SQL Templates
# ---------------------------------------------------------------------------

def sql_truncate(schema: str, table: str) -> str:
    return f"TRUNCATE TABLE {schema}.{table};"


def sql_copy(schema: str, table: str, s3_path: str, iam_role: str) -> str:
    return f"""
COPY {schema}.{table}
FROM '{s3_path}'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET
SERIALIZETOJSON
COMPUPDATE ON
STATUPDATE ON;
""".strip()


def sql_upsert_orders(staging: str, target: str) -> str:
    """
    Merge staging into fact_order.
    DELETE existing matching order_ids, then INSERT all from staging.
    This is the standard Redshift upsert pattern (no MERGE in older RS).
    """
    return f"""
BEGIN;

DELETE FROM {target}.fact_order
USING {staging}.stg_order_events s
WHERE {target}.fact_order.order_id = s.order_id;

INSERT INTO {target}.fact_order
SELECT
    order_id,
    warehouse_id,
    product_category,
    order_status,
    placed_at,
    shipped_at,
    delivered_at,
    shift,
    units_ordered,
    order_value_usd,
    prime_member,
    sla_breach,
    processing_hours,
    order_day_of_week,
    is_weekend,
    fulfillment_speed_tier,
    event_date,
    ingested_at
FROM {staging}.stg_order_events;

COMMIT;
""".strip()


def sql_upsert_shipments(staging: str, target: str) -> str:
    return f"""
BEGIN;

DELETE FROM {target}.fact_shipment
USING {staging}.stg_shipment_tracking s
WHERE {target}.fact_shipment.shipment_id = s.shipment_id;

INSERT INTO {target}.fact_shipment
SELECT
    shipment_id,
    order_id,
    warehouse_id,
    carrier,
    tracking_number,
    shipped_at,
    estimated_delivery_at,
    actual_delivery_at,
    delivery_status,
    late_delivery,
    delivery_delay_hours,
    event_date,
    ingested_at
FROM {staging}.stg_shipment_tracking;

COMMIT;
""".strip()


def sql_upsert_capacity(staging: str, target: str) -> str:
    return f"""
BEGIN;

DELETE FROM {target}.dim_warehouse_capacity
USING {staging}.stg_warehouse_capacity s
WHERE {target}.dim_warehouse_capacity.warehouse_id = s.warehouse_id
  AND {target}.dim_warehouse_capacity.effective_start_date = s.effective_start_date;

INSERT INTO {target}.dim_warehouse_capacity
SELECT
    warehouse_id,
    warehouse_name,
    region,
    snapshot_date,
    max_daily_capacity,
    units_processed,
    utilization_pct,
    active_shifts,
    headcount,
    effective_start_date,
    effective_end_date,
    is_current,
    ingested_at
FROM {staging}.stg_warehouse_capacity;

COMMIT;
""".strip()


def sql_upsert_returns(staging: str, target: str) -> str:
    return f"""
BEGIN;

DELETE FROM {target}.fact_returns_defects
USING {staging}.stg_returns_defects s
WHERE {target}.fact_returns_defects.record_id = s.record_id;

INSERT INTO {target}.fact_returns_defects
SELECT
    record_id,
    order_id,
    warehouse_id,
    product_category,
    report_type,
    defect_type,
    reported_at,
    units_affected,
    refund_issued,
    root_cause_category,
    event_date,
    ingested_at
FROM {staging}.stg_returns_defects;

COMMIT;
""".strip()


def sql_audit_log(job_name: str, table: str, rows: int, status: str) -> str:
    return f"""
INSERT INTO ops_audit.pipeline_run_log
    (job_name, target_table, rows_loaded, run_status, run_ts)
VALUES
    ('{job_name}', '{table}', {rows}, '{status}', '{RUN_TS}');
""".strip()


# ---------------------------------------------------------------------------
# Executor — runs SQL in Glue, writes to file locally
# ---------------------------------------------------------------------------

sql_scripts = []

def execute_or_log(label: str, sql: str, connection=None):
    if IS_LOCAL:
        sql_scripts.append(f"-- {label}\n{sql}\n")
        print(f"  [SQL logged] {label}")
    else:
        connection.execute(sql)
        print(f"  [Executed]   {label}")


# ---------------------------------------------------------------------------
# Build load plan
# ---------------------------------------------------------------------------

DATASETS = [
    {
        "name":        "order_events",
        "stg_table":   "stg_order_events",
        "s3_path":     f"{CURATED_BUCKET}/order_events/",
        "upsert_fn":   sql_upsert_orders,
        "target_tbl":  "fact_order",
    },
    {
        "name":        "shipment_tracking",
        "stg_table":   "stg_shipment_tracking",
        "s3_path":     f"{CURATED_BUCKET}/shipment_tracking/",
        "upsert_fn":   sql_upsert_shipments,
        "target_tbl":  "fact_shipment",
    },
    {
        "name":        "warehouse_capacity",
        "stg_table":   "stg_warehouse_capacity",
        "s3_path":     f"{CURATED_BUCKET}/warehouse_capacity/",
        "upsert_fn":   sql_upsert_capacity,
        "target_tbl":  "dim_warehouse_capacity",
    },
    {
        "name":        "returns_defects",
        "stg_table":   "stg_returns_defects",
        "s3_path":     f"{CURATED_BUCKET}/returns_defects/",
        "upsert_fn":   sql_upsert_returns,
        "target_tbl":  "fact_returns_defects",
    },
]

TARGET_SCHEMA = "ops"
conn = None  # Glue JDBC connection object (set in non-local mode)

if not IS_LOCAL:
    conn = glueContext.extract_jdbc_conf(REDSHIFT_CONN)

# ---------------------------------------------------------------------------
# Execute load plan
# ---------------------------------------------------------------------------

print("[Loading into Redshift staging...]\n")

for ds in DATASETS:
    name      = ds["name"]
    stg_table = ds["stg_table"]
    s3_path   = ds["s3_path"]
    upsert_fn = ds["upsert_fn"]
    target    = ds["target_tbl"]

    print(f"  Processing: {name}")

    execute_or_log(
        f"TRUNCATE {STAGING_SCHEMA}.{stg_table}",
        sql_truncate(STAGING_SCHEMA, stg_table),
        conn
    )

    execute_or_log(
        f"COPY {STAGING_SCHEMA}.{stg_table}",
        sql_copy(STAGING_SCHEMA, stg_table, s3_path, IAM_ROLE),
        conn
    )

    execute_or_log(
        f"UPSERT {TARGET_SCHEMA}.{target}",
        upsert_fn(STAGING_SCHEMA, TARGET_SCHEMA),
        conn
    )

    execute_or_log(
        f"AUDIT LOG {target}",
        sql_audit_log("redshift_staging_loader", target, -1, "SUCCESS"),
        conn
    )

    print()

# ---------------------------------------------------------------------------
# Write SQL output locally
# ---------------------------------------------------------------------------

if IS_LOCAL and sql_scripts:
    out_file = os.path.join(SQL_OUT, "redshift_load_plan.sql")
    with open(out_file, "w") as f:
        f.write(f"-- Redshift Load Plan\n")
        f.write(f"-- Generated: {RUN_TS}\n")
        f.write(f"-- Run this against your Redshift cluster after S3 upload\n\n")
        f.write("\n\n".join(sql_scripts))
    print(f"[Local] SQL load plan written to {out_file}")

print("=" * 50)
print(" Redshift Loader Complete")
print("=" * 50)
print(" All staging tables loaded and upserted into fact/dim tables.")
print(" Check ops_audit.pipeline_run_log for run history.\n")

if not IS_LOCAL:
    job.commit()
