"""
Glue ETL Job 1 — Orders & Shipments Cleansing
===============================================
Reads raw CSVs from S3 landing zone, applies:
  - Deduplication on primary key
  - Null handling and type casting
  - SCD Type 2 for slowly changing fields
  - Watermark-based incremental loading (only new partitions)

Writes cleaned Parquet to S3 curated zone, partitioned by date.

AWS Glue job parameters (set in Glue console under Job Parameters):
  --S3_RAW_BUCKET       s3://your-raw-bucket/
  --S3_CURATED_BUCKET   s3://your-curated-bucket/
  --WATERMARK_DATE      2026-01-01   (moves forward each run)
  --JOB_NAME            orders_shipments_cleanse

Local dev: Run with --local True to use ./output as source.
"""

import sys
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Bootstrap — works both in Glue and local dev
# ---------------------------------------------------------------------------
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "S3_RAW_BUCKET",
        "S3_CURATED_BUCKET",
        "WATERMARK_DATE",
    ])

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    RAW_BUCKET      = args["S3_RAW_BUCKET"].rstrip("/")
    CURATED_BUCKET  = args["S3_CURATED_BUCKET"].rstrip("/")
    WATERMARK_DATE  = args["WATERMARK_DATE"]
    IS_LOCAL        = False

except ModuleNotFoundError:
    # Local dev mode — uses PySpark directly
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("orders_shipments_cleanse_local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    RAW_BUCKET     = "./output"
    CURATED_BUCKET = "./curated"
    WATERMARK_DATE = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    IS_LOCAL       = True

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

print(f"\n[Glue Job] orders_shipments_cleanse")
print(f"[Config]  RAW      : {RAW_BUCKET}")
print(f"[Config]  CURATED  : {CURATED_BUCKET}")
print(f"[Config]  Watermark: {WATERMARK_DATE}\n")

os.makedirs(CURATED_BUCKET, exist_ok=True)

# ---------------------------------------------------------------------------
# Helper: get partitions newer than watermark
# ---------------------------------------------------------------------------

def get_new_partitions(base_path: str, watermark: str) -> list:
    """
    Returns list of date partition paths that are >= watermark date.
    In Glue this reads S3 prefixes; locally it reads the filesystem.
    """
    partitions = []
    if IS_LOCAL:
        if not os.path.exists(base_path):
            return []
        for folder in os.listdir(base_path):
            if folder.startswith("date="):
                date_val = folder.replace("date=", "")
                if date_val >= watermark:
                    partitions.append(os.path.join(base_path, folder))
    else:
        # In Glue, Spark's partition pruning handles this via pushdown filter
        partitions = [base_path]
    return partitions


# ---------------------------------------------------------------------------
# CLEANSE: Order Events
# ---------------------------------------------------------------------------

def cleanse_orders():
    print("[Step 1] Loading raw order_events...")

    raw_path = os.path.join(RAW_BUCKET, "order_events")
    partitions = get_new_partitions(raw_path, WATERMARK_DATE)

    if not partitions:
        print("[Skip] No new order partitions since watermark.")
        return

    df = spark.read.option("header", True).option("inferSchema", True).csv(partitions)
    raw_count = df.count()
    print(f"[Step 1] Raw rows loaded: {raw_count:,}")

    # --- Type casting ---
    df = (df
        .withColumn("placed_at",    F.to_timestamp("placed_at"))
        .withColumn("shipped_at",   F.to_timestamp("shipped_at"))
        .withColumn("delivered_at", F.to_timestamp("delivered_at"))
        .withColumn("order_value_usd",   F.col("order_value_usd").cast(DoubleType()))
        .withColumn("units_ordered",     F.col("units_ordered").cast(IntegerType()))
        .withColumn("processing_hours",  F.col("processing_hours").cast(DoubleType()))
        .withColumn("prime_member",      F.col("prime_member").cast(BooleanType()))
        .withColumn("sla_breach",        F.col("sla_breach").cast(BooleanType()))
    )

    # --- Null handling ---
    # For cancelled orders, shipped_at and delivered_at being null is valid
    # Flag unexpected nulls in required fields
    required_fields = ["order_id", "warehouse_id", "product_category",
                        "order_status", "placed_at", "order_value_usd"]

    null_counts = {}
    for field in required_fields:
        n = df.filter(F.col(field).isNull()).count()
        null_counts[field] = n
        if n > 0:
            print(f"[WARN] Nulls in {field}: {n}")

    # Drop rows with null in hard-required fields
    df = df.dropna(subset=["order_id", "placed_at", "order_status"])

    # Fill optional nulls with sensible defaults
    df = df.fillna({
        "processing_hours": -1.0,
        "prime_member":     False,
        "sla_breach":       False,
    })

    # --- Deduplication ---
    # Keep the latest record per order_id (in case of duplicate ingestion)
    window = Window.partitionBy("order_id").orderBy(F.col("placed_at").desc())
    df = (df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    dedup_count = df.count()
    print(f"[Step 1] After dedup: {dedup_count:,} rows ({raw_count - dedup_count} dupes removed)")

    # --- Derived columns ---
    df = df.withColumn(
        "order_day_of_week",
        F.date_format(F.col("placed_at"), "EEEE")
    ).withColumn(
        "is_weekend",
        F.dayofweek(F.col("placed_at")).isin([1, 7])
    ).withColumn(
        "fulfillment_speed_tier",
        F.when(F.col("processing_hours") <= 12, "Fast")
         .when(F.col("processing_hours") <= 24, "Standard")
         .when(F.col("processing_hours") > 24,  "Slow")
         .otherwise("Unknown")
    ).withColumn(
        "ingested_at", F.current_timestamp()
    )

    # --- Write curated Parquet ---
    out_path = os.path.join(CURATED_BUCKET, "order_events")
    (df.repartition("event_date")
       .write
       .mode("overwrite")
       .partitionBy("event_date")
       .parquet(out_path))

    print(f"[Step 1] Wrote {dedup_count:,} rows to {out_path}\n")
    return dedup_count


# ---------------------------------------------------------------------------
# CLEANSE: Shipment Tracking
# ---------------------------------------------------------------------------

def cleanse_shipments():
    print("[Step 2] Loading raw shipment_tracking...")

    raw_path = os.path.join(RAW_BUCKET, "shipment_tracking")
    partitions = get_new_partitions(raw_path, WATERMARK_DATE)

    if not partitions:
        print("[Skip] No new shipment partitions since watermark.")
        return

    df = spark.read.option("header", True).option("inferSchema", True).csv(partitions)
    raw_count = df.count()
    print(f"[Step 2] Raw rows loaded: {raw_count:,}")

    # --- Type casting ---
    df = (df
        .withColumn("shipped_at",            F.to_timestamp("shipped_at"))
        .withColumn("estimated_delivery_at", F.to_timestamp("estimated_delivery_at"))
        .withColumn("actual_delivery_at",    F.to_timestamp("actual_delivery_at"))
        .withColumn("late_delivery",         F.col("late_delivery").cast(BooleanType()))
    )

    # --- Deduplication on shipment_id ---
    window = Window.partitionBy("shipment_id").orderBy(F.col("shipped_at").desc())
    df = (df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    df = df.dropna(subset=["shipment_id", "order_id", "shipped_at"])

    df = df.fillna({"late_delivery": False})

    # --- Derived: actual delivery delay in hours ---
    df = df.withColumn(
        "delivery_delay_hours",
        F.when(
            F.col("actual_delivery_at").isNotNull() & F.col("estimated_delivery_at").isNotNull(),
            F.round(
                (F.unix_timestamp("actual_delivery_at") - F.unix_timestamp("estimated_delivery_at")) / 3600,
                2
            )
        ).otherwise(None)
    ).withColumn(
        "ingested_at", F.current_timestamp()
    )

    dedup_count = df.count()
    print(f"[Step 2] After dedup: {dedup_count:,} rows")

    out_path = os.path.join(CURATED_BUCKET, "shipment_tracking")
    (df.repartition("event_date")
       .write
       .mode("overwrite")
       .partitionBy("event_date")
       .parquet(out_path))

    print(f"[Step 2] Wrote {dedup_count:,} rows to {out_path}\n")
    return dedup_count


# ---------------------------------------------------------------------------
# CLEANSE: Warehouse Capacity (SCD Type 2)
# ---------------------------------------------------------------------------

def cleanse_warehouse_capacity():
    print("[Step 3] Loading warehouse_capacity (SCD Type 2)...")

    raw_path = os.path.join(RAW_BUCKET, "warehouse_capacity", "warehouse_capacity.csv")
    df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

    df = (df
        .withColumn("snapshot_date",       F.to_date("snapshot_date"))
        .withColumn("max_daily_capacity",  F.col("max_daily_capacity").cast(IntegerType()))
        .withColumn("units_processed",     F.col("units_processed").cast(IntegerType()))
        .withColumn("utilization_pct",     F.col("utilization_pct").cast(DoubleType()))
        .withColumn("active_shifts",       F.col("active_shifts").cast(IntegerType()))
        .withColumn("headcount",           F.col("headcount").cast(IntegerType()))
    )

    # SCD Type 2: add effective_start, effective_end, is_current columns
    # For this pipeline, each snapshot_date row is a new version of warehouse state
    window = Window.partitionBy("warehouse_id").orderBy("snapshot_date")
    window_next = Window.partitionBy("warehouse_id").orderBy("snapshot_date").rowsBetween(1, 1)

    df = (df
        .withColumn("effective_start_date", F.col("snapshot_date"))
        .withColumn("effective_end_date",
            F.lead("snapshot_date", 1).over(window))
        .withColumn("is_current",
            F.col("effective_end_date").isNull())
        .withColumn("ingested_at", F.current_timestamp())
    )

    out_path = os.path.join(CURATED_BUCKET, "warehouse_capacity")
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .parquet(out_path))

    count = df.count()
    print(f"[Step 3] Wrote {count:,} SCD Type 2 rows to {out_path}\n")
    return count


# ---------------------------------------------------------------------------
# CLEANSE: Returns & Defects
# ---------------------------------------------------------------------------

def cleanse_returns_defects():
    print("[Step 4] Loading returns_defects...")

    raw_path = os.path.join(RAW_BUCKET, "returns_defects", "returns_defects.csv")
    df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

    df = (df
        .withColumn("reported_at",    F.to_timestamp("reported_at"))
        .withColumn("units_affected", F.col("units_affected").cast(IntegerType()))
        .withColumn("refund_issued",  F.col("refund_issued").cast(BooleanType()))
    )

    df = df.dropna(subset=["record_id", "order_id", "report_type"])

    # Dedup on record_id
    window = Window.partitionBy("record_id").orderBy(F.col("reported_at").desc())
    df = (df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    df = df.withColumn("ingested_at", F.current_timestamp())

    out_path = os.path.join(CURATED_BUCKET, "returns_defects")
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .parquet(out_path))

    count = df.count()
    print(f"[Step 4] Wrote {count:,} rows to {out_path}\n")
    return count


# ---------------------------------------------------------------------------
# Run all jobs
# ---------------------------------------------------------------------------

results = {}
results["orders"]    = cleanse_orders()
results["shipments"] = cleanse_shipments()
results["capacity"]  = cleanse_warehouse_capacity()
results["returns"]   = cleanse_returns_defects()

print("=" * 50)
print(" Glue Job Complete — Summary")
print("=" * 50)
for dataset, count in results.items():
    if count:
        print(f"  {dataset:<15} {count:>8,} rows written")

if not IS_LOCAL:
    job.commit()

print("\n[Done] Curated Parquet ready for Redshift COPY.\n")
