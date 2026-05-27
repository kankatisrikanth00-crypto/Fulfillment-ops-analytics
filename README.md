# data_sim — Fulfillment Ops Data Simulator

Generates four realistic datasets that simulate Amazon fulfillment center
operations across a 30-day window. Designed to feed the S3 raw landing zone
in the `fulfillment-ops-analytics` pipeline.

---

## Datasets produced

| Dataset | Rows | Format | Partitioning |
|---|---|---|---|
| `order_events` | ~12,000 | CSV | `date=YYYY-MM-DD` |
| `shipment_tracking` | ~11,600 | CSV | `date=YYYY-MM-DD` |
| `warehouse_capacity` | 186 | CSV | None (static) |
| `returns_defects` | ~980 | CSV | None |

---

## Simulated business logic

**Order events** models the full order lifecycle:
- Status progression: PLACED → PICKED → PACKED → SHIPPED → DELIVERED
- ~3% cancellation rate before shipment
- ~5% return rate post-delivery
- SLA breach detection per product category (e.g., Grocery = 12hr, Electronics = 24hr)
- Prime vs. non-Prime member flag
- Shift assignment (Morning / Afternoon / Night) based on placed_at hour

**Shipment tracking** captures carrier-level delivery data:
- One record per shipped order, linked by `order_id`
- Five carriers: UPS, FedEx, USPS, Amazon Logistics, OnTrac
- ~8% late delivery rate (actual delivery exceeds estimated)

**Warehouse capacity** provides a daily snapshot per fulfillment center:
- Six FCs across four regions (West, Midwest, East, South)
- Daily utilization %, headcount, active shifts

**Returns & defects** records post-delivery issues:
- All RETURNED orders generate a return record
- ~4% of DELIVERED orders generate a defect report
- Root cause categories: Carrier / Warehouse / Supplier / Customer error

---

## Quick start

```bash
# No pip installs needed for the generator (standard library only)
python generate_data.py

# With options
python generate_data.py --rows 5000 --seed 99 --out ./my_output

# Validate output before S3 upload
pip install pandas
python data_validator.py --out ./output
```

---

## Output structure (S3-ready partitioning)

```
output/
  order_events/
    date=2026-04-25/order_events.csv
    date=2026-04-26/order_events.csv
    ...
  shipment_tracking/
    date=2026-04-25/shipment_tracking.csv
    ...
  warehouse_capacity/
    warehouse_capacity.csv
  returns_defects/
    returns_defects.csv
```

This partitioning matches AWS Glue's native partition inference. When you
crawl the S3 bucket with a Glue Crawler, it automatically infers
`date` as a partition column — no schema changes needed.

---

## Key fields reference

### order_events

| Field | Type | Notes |
|---|---|---|
| `order_id` | string | PK, format ORD-NNNNNNN |
| `warehouse_id` | string | FK to dim_warehouse |
| `product_category` | string | 10 categories |
| `order_status` | string | PLACED / SHIPPED / DELIVERED / CANCELLED / RETURNED |
| `placed_at` | timestamp | ISO 8601 |
| `shipped_at` | timestamp | Null for cancelled orders |
| `delivered_at` | timestamp | Null for in-transit / cancelled |
| `shift` | string | Morning / Afternoon / Night |
| `sla_breach` | boolean | True if ship time exceeds category SLA |
| `processing_hours` | float | Hours from placed_at to shipped_at |

### shipment_tracking

| Field | Type | Notes |
|---|---|---|
| `shipment_id` | string | PK, format SHP-NNNNNNN |
| `order_id` | string | FK to order_events |
| `carrier` | string | One of 5 carriers |
| `late_delivery` | boolean | True if actual > estimated delivery |

---

## Adjustable parameters

| Parameter | Default | Description |
|---|---|---|
| `--rows` | 12,000 | Total orders to generate |
| `--seed` | 42 | RNG seed for reproducibility |
| `--out` | ./output | Output directory |

The seed guarantees identical data on any machine — useful for
consistent Redshift loads and repeatable dashboard screenshots.
