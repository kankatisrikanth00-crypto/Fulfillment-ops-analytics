"""
Local Test Runner — Glue ETL Jobs
===================================
Runs both Glue jobs locally using PySpark (no AWS needed).
Validates that the full ETL pipeline works before deploying to Glue.

Usage:
    pip install pyspark
    python glue_jobs/run_local.py

What it does:
  1. Runs orders_shipments_cleanse.py   → writes Parquet to ./curated/
  2. Runs redshift_staging_loader.py    → writes SQL plan to ./sql_output/
  3. Prints row counts and spot-checks each curated dataset
"""

import subprocess
import sys
import os
from datetime import datetime, timedelta

WATERMARK = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

print("\n" + "=" * 55)
print(" Local ETL Runner — Fulfillment Ops Analytics")
print("=" * 55)

# ---------------------------------------------------------------------------
# Step 1: Run cleanse job
# ---------------------------------------------------------------------------

print("\n[Job 1] orders_shipments_cleanse")
print("-" * 40)

result = subprocess.run(
    [sys.executable, "glue_jobs/orders_shipments_cleanse.py"],
    capture_output=False
)

if result.returncode != 0:
    print("\n[FAIL] Cleanse job failed. Check errors above.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Step 2: Validate curated output
# ---------------------------------------------------------------------------

print("\n[Validation] Checking curated Parquet output...")

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("local_validation") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    curated_datasets = {
        "order_events":      "./curated/order_events",
        "shipment_tracking": "./curated/shipment_tracking",
        "warehouse_capacity":"./curated/warehouse_capacity",
        "returns_defects":   "./curated/returns_defects",
    }

    all_passed = True
    for name, path in curated_datasets.items():
        if not os.path.exists(path):
            print(f"  [MISSING] {name} — path not found: {path}")
            all_passed = False
            continue

        df = spark.read.parquet(path)
        count = df.count()
        cols  = len(df.columns)

        # Check ingested_at column was added
        has_ingested = "ingested_at" in df.columns
        # Check no nulls on key ID column
        id_col = {"order_events": "order_id",
                   "shipment_tracking": "shipment_id",
                   "warehouse_capacity": "warehouse_id",
                   "returns_defects": "record_id"}.get(name)

        null_ids = df.filter(df[id_col].isNull()).count() if id_col else 0

        status = "PASS" if (count > 0 and has_ingested and null_ids == 0) else "FAIL"
        if status == "FAIL":
            all_passed = False

        print(f"  [{status}] {name:<22} {count:>7,} rows | {cols} cols | "
              f"ingested_at={'yes' if has_ingested else 'NO'} | "
              f"null_ids={null_ids}")

    spark.stop()

    if all_passed:
        print("\n  All curated datasets validated.")
    else:
        print("\n  Some validations failed — check above.")

except ImportError:
    print("  [SKIP] pyspark not installed — skipping Parquet validation.")
    print("         Run: pip install pyspark")

# ---------------------------------------------------------------------------
# Step 3: Run SQL loader (generates SQL plan locally)
# ---------------------------------------------------------------------------

print("\n[Job 2] redshift_staging_loader (local mode — generates SQL)")
print("-" * 40)

result2 = subprocess.run(
    [sys.executable, "glue_jobs/redshift_staging_loader.py"],
    capture_output=False
)

if result2.returncode != 0:
    print("\n[FAIL] Loader job failed.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------

print("\n" + "=" * 55)
print(" Local Run Complete")
print("=" * 55)
print("""
 What to do next:
 ─────────────────────────────────────────────────────
 1. Review ./curated/         — cleaned Parquet files
 2. Review ./sql_output/      — Redshift load plan SQL
 3. Upload ./output/ to your S3 raw bucket
 4. Upload ./curated/ to your S3 curated bucket
 5. Deploy glue_jobs/*.py to AWS Glue (see README)
 6. Run redshift/schema.sql against your Redshift cluster
""")
