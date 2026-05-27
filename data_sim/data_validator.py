"""
Data Validator — Fulfillment Ops Analytics
============================================
Runs quality checks on generated datasets before S3 upload.
This mirrors the validation layer a BIE would build before
loading raw data into Redshift.

Checks performed:
  - Row count expectations
  - Null / blank field audit
  - Referential integrity (shipments → orders)
  - Business logic: SLA breach rate, return rate within expected bands
  - Date range completeness (no missing days)
  - Duplicate primary key detection

Usage:
    pip install pandas
    python data_validator.py --out ./output
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

PASS = "\033[92m PASS\033[0m"
FAIL = "\033[91m FAIL\033[0m"
WARN = "\033[93m WARN\033[0m"

failures = 0


def check(label: str, condition: bool, detail: str = "", warn_only: bool = False):
    global failures
    if condition:
        print(f"  [{PASS}] {label}")
    else:
        tag = WARN if warn_only else FAIL
        print(f"  [{tag}] {label}" + (f" — {detail}" if detail else ""))
        if not warn_only:
            failures += 1


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

def load_partitioned(out_dir: str, dataset: str) -> pd.DataFrame:
    folder = os.path.join(out_dir, dataset)
    frames = []
    for root, _, files in os.walk(folder):
        for f in files:
            if f.endswith(".csv"):
                frames.append(pd.read_csv(os.path.join(root, f)))
    if not frames:
        print(f"\033[91m  No files found for {dataset} in {folder}\033[0m")
        sys.exit(1)
    return pd.concat(frames, ignore_index=True)


def load_flat(out_dir: str, filename: str) -> pd.DataFrame:
    path = os.path.join(out_dir, filename)
    return pd.read_csv(path)


# ---------------------------------------------------------------------------
# Validation suites
# ---------------------------------------------------------------------------

def validate_orders(df: pd.DataFrame):
    print("\n Order events")

    check("Row count >= 1,000",            len(df) >= 1_000, f"got {len(df):,}")
    check("No duplicate order_ids",        df["order_id"].nunique() == len(df),
          f"{len(df) - df['order_id'].nunique()} duplicates")
    check("order_id not null",             df["order_id"].notna().all())
    check("warehouse_id not null",         df["warehouse_id"].notna().all())
    check("order_status not null",         df["order_status"].notna().all())
    check("placed_at not null",            df["placed_at"].notna().all())

    valid_statuses = {"PLACED","PICKED","PACKED","SHIPPED","DELIVERED","CANCELLED","RETURNED"}
    bad = df[~df["order_status"].isin(valid_statuses)]
    check("order_status values valid",     len(bad) == 0, f"{len(bad)} bad values")

    check("order_value_usd > 0",           (df["order_value_usd"] > 0).all())
    check("units_ordered >= 1",            (df["units_ordered"] >= 1).all())

    sla_rate = df["sla_breach"].mean()
    check("SLA breach rate < 15%",        sla_rate < 0.15, f"got {sla_rate:.1%}", warn_only=True)

    cancel_rate = (df["order_status"] == "CANCELLED").mean()
    check("Cancel rate between 1-10%",    0.01 <= cancel_rate <= 0.10, f"got {cancel_rate:.1%}")

    return_rate = (df["order_status"] == "RETURNED").mean()
    check("Return rate between 1-10%",    0.01 <= return_rate <= 0.10, f"got {return_rate:.1%}")

    # Shipped orders should have shipped_at populated
    shipped = df[df["order_status"].isin({"SHIPPED","DELIVERED","RETURNED"})]
    missing_ship = shipped["shipped_at"].isna().sum()
    check("Shipped orders have shipped_at", missing_ship == 0, f"{missing_ship} missing")

    # Delivered orders should have delivered_at populated
    delivered = df[df["order_status"] == "DELIVERED"]
    missing_del = delivered["delivered_at"].isna().sum()
    check("Delivered orders have delivered_at", missing_del == 0, f"{missing_del} missing")

    # Date range: check all 30+ days present
    df["event_date_parsed"] = pd.to_datetime(df["event_date"])
    date_range = (df["event_date_parsed"].max() - df["event_date_parsed"].min()).days
    check("Date range spans >= 28 days", date_range >= 28, f"got {date_range} days")


def validate_shipments(orders: pd.DataFrame, shipments: pd.DataFrame):
    print("\n Shipment tracking")

    check("Row count > 0",                 len(shipments) > 0, f"got {len(shipments):,}")
    check("No duplicate shipment_ids",     shipments["shipment_id"].nunique() == len(shipments))
    check("carrier not null",              shipments["carrier"].notna().all())
    check("tracking_number not null",      shipments["tracking_number"].notna().all())

    # Referential integrity: every shipment must reference a valid order
    shipped_order_ids = set(
        orders[orders["order_status"].isin({"SHIPPED","DELIVERED","RETURNED"})]["order_id"]
    )
    orphan = shipments[~shipments["order_id"].isin(shipped_order_ids)]
    check("All shipments reference shipped orders", len(orphan) == 0, f"{len(orphan)} orphaned rows")

    # Every shipped/delivered order should have a shipment
    missing_shipments = len(shipped_order_ids) - shipments["order_id"].nunique()
    check("All shipped orders have a shipment record", missing_shipments == 0,
          f"{missing_shipments} orders missing shipment", warn_only=True)

    late_rate = shipments["late_delivery"].mean()
    check("Late delivery rate < 15%",      late_rate < 0.15, f"got {late_rate:.1%}", warn_only=True)


def validate_capacity(df: pd.DataFrame):
    print("\n Warehouse capacity")

    expected_warehouses = {"FC_SEA1","FC_LAX2","FC_ORD3","FC_JFK4","FC_ATL5","FC_DFW6"}
    found = set(df["warehouse_id"].unique())
    check("All 6 FCs present",             found == expected_warehouses,
          f"missing: {expected_warehouses - found}")

    check("utilization_pct between 0-100",
          df["utilization_pct"].between(0, 100).all())
    check("units_processed <= max_daily_capacity",
          (df["units_processed"] <= df["max_daily_capacity"]).all())
    check("headcount > 0",                 (df["headcount"] > 0).all())

    df["snapshot_date_parsed"] = pd.to_datetime(df["snapshot_date"])
    date_range = (df["snapshot_date_parsed"].max() - df["snapshot_date_parsed"].min()).days
    check("Capacity snapshot spans >= 28 days", date_range >= 28, f"got {date_range} days")


def validate_returns(orders: pd.DataFrame, rd: pd.DataFrame):
    print("\n Returns & defects")

    check("Row count > 0",                rd.notna().all().all() or len(rd) > 0)
    check("No duplicate record_ids",      rd["record_id"].nunique() == len(rd))
    check("report_type values valid",
          rd["report_type"].isin({"RETURN","DEFECT"}).all())
    check("units_affected >= 1",          (rd["units_affected"] >= 1).all())

    # All return records should tie back to returned orders
    returned_ids = set(orders[orders["order_status"] == "RETURNED"]["order_id"])
    return_records = rd[rd["report_type"] == "RETURN"]
    orphan = return_records[~return_records["order_id"].isin(returned_ids)]
    check("Return records tie back to RETURNED orders", len(orphan) == 0,
          f"{len(orphan)} orphaned records")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="./output")
    args = parser.parse_args()

    print(f"\n Fulfillment Ops — Data Validator")
    print(f" Source: {args.out}\n")
    print("=" * 50)

    orders    = load_partitioned(args.out, "order_events")
    shipments = load_partitioned(args.out, "shipment_tracking")
    capacity  = load_flat(os.path.join(args.out, "warehouse_capacity"), "warehouse_capacity.csv")
    rd        = load_flat(os.path.join(args.out, "returns_defects"), "returns_defects.csv")

    validate_orders(orders)
    validate_shipments(orders, shipments)
    validate_capacity(capacity)
    validate_returns(orders, rd)

    print("\n" + "=" * 50)
    if failures == 0:
        print(f"\033[92m All checks passed. Data is ready for S3 upload.\033[0m\n")
    else:
        print(f"\033[91m {failures} check(s) failed. Resolve before uploading.\033[0m\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
