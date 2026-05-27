"""
Amazon Fulfillment Ops Analytics — Data Simulator
===================================================
Generates four realistic datasets that mimic what a BIE would work with
inside Amazon's fulfillment network:

  1. order_events.csv         — 12,000 order lifecycle events
  2. shipment_tracking.csv    — one shipment record per fulfilled order
  3. warehouse_capacity.csv   — static daily capacity snapshot per FC
  4. returns_defects.csv      — return and defect records

Usage:
    python generate_data.py
    python generate_data.py --rows 5000 --seed 99 --out ./output

Output lands in ./output/ by default, partitioned for S3 upload:
    output/
      order_events/date=YYYY-MM-DD/order_events.csv
      shipment_tracking/date=YYYY-MM-DD/shipment_tracking.csv
      warehouse_capacity/warehouse_capacity.csv
      returns_defects/returns_defects.csv
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SEED = 42
NUM_ORDERS = 12_000
SIM_DAYS = 30           # window: last 30 days
OUTPUT_DIR = "./output"

WAREHOUSES = [
    {"id": "FC_SEA1", "name": "Seattle FC 1",   "region": "West",     "capacity": 4000},
    {"id": "FC_LAX2", "name": "Los Angeles FC 2","region": "West",     "capacity": 5500},
    {"id": "FC_ORD3", "name": "Chicago FC 3",    "region": "Midwest",  "capacity": 6000},
    {"id": "FC_JFK4", "name": "New York FC 4",   "region": "East",     "capacity": 7000},
    {"id": "FC_ATL5", "name": "Atlanta FC 5",    "region": "South",    "capacity": 5000},
    {"id": "FC_DFW6", "name": "Dallas FC 6",     "region": "South",    "capacity": 4500},
]

CARRIERS = ["UPS", "FedEx", "USPS", "Amazon Logistics", "OnTrac"]

PRODUCT_CATEGORIES = [
    "Electronics", "Books", "Apparel", "Home & Kitchen",
    "Sports", "Toys", "Beauty", "Grocery", "Tools", "Pet Supplies",
]

SHIFTS = ["Morning", "Afternoon", "Night"]

ORDER_STATUSES = ["PLACED", "PICKED", "PACKED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]

DEFECT_TYPES = ["Damaged in transit", "Wrong item", "Missing item", "Late delivery", "Packaging defect"]

SLA_HOURS = {
    "Electronics":     24,
    "Books":           48,
    "Apparel":         36,
    "Home & Kitchen":  48,
    "Sports":          48,
    "Toys":            36,
    "Beauty":          24,
    "Grocery":         12,
    "Tools":           48,
    "Pet Supplies":    36,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def shift_for_hour(hour: int) -> str:
    if 6 <= hour < 14:
        return "Morning"
    elif 14 <= hour < 22:
        return "Afternoon"
    else:
        return "Night"


def compute_sla_breach(category: str, placed_at: datetime, shipped_at) -> bool:
    if shipped_at is None:
        return True
    sla_limit = SLA_HOURS.get(category, 48)
    return (shipped_at - placed_at).total_seconds() / 3600 > sla_limit


# ---------------------------------------------------------------------------
# Generator: Orders
# ---------------------------------------------------------------------------

def generate_orders(n: int, sim_end: datetime) -> list[dict]:
    sim_start = sim_end - timedelta(days=SIM_DAYS)
    orders = []

    for i in range(1, n + 1):
        order_id = f"ORD-{i:07d}"
        warehouse = random.choice(WAREHOUSES)
        category = random.choice(PRODUCT_CATEGORIES)
        placed_at = random_date(sim_start, sim_end)

        # Simulate progression through statuses
        # ~3% cancel before shipping, ~5% return after delivery
        rand = random.random()
        if rand < 0.03:
            final_status = "CANCELLED"
            shipped_at = None
            delivered_at = None
        else:
            # Pick time step is 1-6 hours after placement
            pick_lag = timedelta(hours=random.uniform(0.5, 6))
            pack_lag = pick_lag + timedelta(hours=random.uniform(0.5, 3))
            ship_lag = pack_lag + timedelta(hours=random.uniform(1, 8))
            deliver_lag = ship_lag + timedelta(hours=random.uniform(18, 72))
            shipped_at = placed_at + ship_lag
            delivered_at = placed_at + deliver_lag

            if rand < 0.08:
                final_status = "RETURNED"
            elif delivered_at > sim_end:
                final_status = "SHIPPED"
                delivered_at = None
            else:
                final_status = "DELIVERED"

        sla_breach = compute_sla_breach(category, placed_at, shipped_at)

        orders.append({
            "order_id":          order_id,
            "warehouse_id":      warehouse["id"],
            "product_category":  category,
            "order_status":      final_status,
            "placed_at":         placed_at.isoformat(timespec="seconds"),
            "shipped_at":        shipped_at.isoformat(timespec="seconds") if shipped_at else "",
            "delivered_at":      delivered_at.isoformat(timespec="seconds") if delivered_at else "",
            "shift":             shift_for_hour(placed_at.hour),
            "units_ordered":     random.randint(1, 8),
            "order_value_usd":   round(random.uniform(5.99, 499.99), 2),
            "prime_member":      random.choice([True, False]),
            "sla_breach":        sla_breach,
            "processing_hours":  round((shipped_at - placed_at).total_seconds() / 3600, 2) if shipped_at else None,
            "event_date":        placed_at.strftime("%Y-%m-%d"),
        })

    return orders


# ---------------------------------------------------------------------------
# Generator: Shipments
# ---------------------------------------------------------------------------

def generate_shipments(orders: list[dict]) -> list[dict]:
    shipments = []
    for o in orders:
        if not o["shipped_at"]:
            continue

        carrier = random.choice(CARRIERS)
        shipped_dt = datetime.fromisoformat(o["shipped_at"])
        estimated_delivery = shipped_dt + timedelta(hours=random.uniform(20, 80))

        # ~8% late (actual arrives after estimated), ~92% on time (actual <= estimated)
        if o["delivered_at"]:
            if random.random() < 0.08:
                # Late: overshoot the estimate by 2-48 hours
                actual_delivery = estimated_delivery + timedelta(hours=random.uniform(2, 48))
            else:
                # On time: arrive 1-12 hours before estimated delivery
                actual_delivery = estimated_delivery - timedelta(hours=random.uniform(1, 12))
        else:
            actual_delivery = None

        shipments.append({
            "shipment_id":           f"SHP-{o['order_id'][4:]}",
            "order_id":              o["order_id"],
            "warehouse_id":          o["warehouse_id"],
            "carrier":               carrier,
            "tracking_number":       f"1Z{random.randint(100000000,999999999)}",
            "shipped_at":            o["shipped_at"],
            "estimated_delivery_at": estimated_delivery.isoformat(timespec="seconds"),
            "actual_delivery_at":    actual_delivery.isoformat(timespec="seconds") if actual_delivery else "",
            "delivery_status":       o["order_status"],
            "late_delivery":         (actual_delivery > estimated_delivery) if actual_delivery else False,
            "event_date":            o["event_date"],
        })

    return shipments


# ---------------------------------------------------------------------------
# Generator: Warehouse Capacity
# ---------------------------------------------------------------------------

def generate_warehouse_capacity(sim_end: datetime) -> list[dict]:
    rows = []
    sim_start = sim_end - timedelta(days=SIM_DAYS)
    current = sim_start

    while current <= sim_end:
        for wh in WAREHOUSES:
            utilization = round(random.uniform(0.55, 0.98), 4)
            rows.append({
                "snapshot_date":       current.strftime("%Y-%m-%d"),
                "warehouse_id":        wh["id"],
                "warehouse_name":      wh["name"],
                "region":              wh["region"],
                "max_daily_capacity":  wh["capacity"],
                "units_processed":     int(wh["capacity"] * utilization),
                "utilization_pct":     round(utilization * 100, 2),
                "active_shifts":       random.randint(2, 3),
                "headcount":           random.randint(180, 520),
            })
        current += timedelta(days=1)

    return rows


# ---------------------------------------------------------------------------
# Generator: Returns & Defects
# ---------------------------------------------------------------------------

def generate_returns_defects(orders: list[dict]) -> list[dict]:
    rows = []
    returned = [o for o in orders if o["order_status"] in ("RETURNED",)]

    # Also randomly add defect reports for ~4% of delivered orders
    delivered = [o for o in orders if o["order_status"] == "DELIVERED"]
    defect_sample = random.sample(delivered, k=int(len(delivered) * 0.04))
    pool = returned + defect_sample

    for o in pool:
        is_return = o["order_status"] == "RETURNED"
        report_dt = datetime.fromisoformat(o["delivered_at"]) + timedelta(days=random.randint(1, 14)) \
                    if o.get("delivered_at") else \
                    datetime.fromisoformat(o["shipped_at"]) + timedelta(days=random.randint(3, 10))

        rows.append({
            "record_id":           f"RD-{o['order_id'][4:]}",
            "order_id":            o["order_id"],
            "warehouse_id":        o["warehouse_id"],
            "product_category":    o["product_category"],
            "report_type":         "RETURN" if is_return else "DEFECT",
            "defect_type":         random.choice(DEFECT_TYPES),
            "reported_at":         report_dt.isoformat(timespec="seconds"),
            "units_affected":      random.randint(1, o["units_ordered"]),
            "refund_issued":       is_return or random.random() < 0.6,
            "root_cause_category": random.choice(["Carrier", "Warehouse", "Supplier", "Customer error"]),
            "event_date":          o["event_date"],
        })

    return rows


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_partitioned(rows: list[dict], dataset_name: str, out_dir: str, date_field: str = "event_date"):
    """Write rows partitioned by date into S3-style folder structure."""
    partitions: dict[str, list[dict]] = {}
    for row in rows:
        date_val = row.get(date_field, "unknown")
        partitions.setdefault(date_val, []).append(row)

    for date_val, partition_rows in sorted(partitions.items()):
        folder = os.path.join(out_dir, dataset_name, f"date={date_val}")
        os.makedirs(folder, exist_ok=True)
        fpath = os.path.join(folder, f"{dataset_name}.csv")
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=partition_rows[0].keys())
            writer.writeheader()
            writer.writerows(partition_rows)

    total_partitions = len(partitions)
    print(f"  {dataset_name}: {len(rows):,} rows across {total_partitions} date partitions")


def write_flat(rows: list[dict], filename: str, out_dir: str):
    """Write a single flat CSV (for slowly changing reference data)."""
    os.makedirs(out_dir, exist_ok=True)
    fpath = os.path.join(out_dir, filename)
    with open(fpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {filename}: {len(rows):,} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate fulfillment ops simulation data.")
    parser.add_argument("--rows",  type=int, default=NUM_ORDERS, help="Number of orders to generate")
    parser.add_argument("--seed",  type=int, default=SEED,       help="Random seed for reproducibility")
    parser.add_argument("--out",   type=str, default=OUTPUT_DIR,  help="Output directory")
    args = parser.parse_args()

    random.seed(args.seed)
    sim_end = datetime.now().replace(microsecond=0)

    print(f"\n Amazon Fulfillment Ops — Data Simulator")
    print(f" Seed: {args.seed} | Orders: {args.rows:,} | Window: last {SIM_DAYS} days")
    print(f" Output: {args.out}\n")

    print("Generating datasets...")
    orders    = generate_orders(args.rows, sim_end)
    shipments = generate_shipments(orders)
    capacity  = generate_warehouse_capacity(sim_end)
    rd        = generate_returns_defects(orders)

    print("\nWriting output files...")
    write_partitioned(orders,    "order_events",      args.out)
    write_partitioned(shipments, "shipment_tracking",  args.out)
    write_flat(capacity, "warehouse_capacity.csv", os.path.join(args.out, "warehouse_capacity"))
    write_flat(rd,       "returns_defects.csv",    os.path.join(args.out, "returns_defects"))

    # Summary stats
    cancelled  = sum(1 for o in orders if o["order_status"] == "CANCELLED")
    returned   = sum(1 for o in orders if o["order_status"] == "RETURNED")
    sla_breach = sum(1 for o in orders if o["sla_breach"])
    delivered  = sum(1 for o in orders if o["order_status"] == "DELIVERED")

    print(f"""
 Summary
 -------
 Total orders     : {len(orders):,}
 Delivered        : {delivered:,}  ({delivered/len(orders)*100:.1f}%)
 Cancelled        : {cancelled:,}   ({cancelled/len(orders)*100:.1f}%)
 Returned         : {returned:,}    ({returned/len(orders)*100:.1f}%)
 SLA breaches     : {sla_breach:,}  ({sla_breach/len(orders)*100:.1f}%)
 Shipments        : {len(shipments):,}
 Capacity rows    : {len(capacity):,}
 Returns/defects  : {len(rd):,}

 Done. Upload the output/ folder to your S3 raw landing bucket.
""")


if __name__ == "__main__":
    main()
