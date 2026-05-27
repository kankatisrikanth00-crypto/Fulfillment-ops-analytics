-- ============================================================
-- Redshift COPY Commands — Load S3 Parquet into Staging
-- ============================================================
-- Replace YOUR_ACCOUNT_ID with: 009846316315
-- Run after schema.sql has been executed
-- ============================================================

-- Step 1: Attach S3 access policy to Redshift Serverless
-- Run this in AWS CLI before executing COPY commands:
--
-- aws iam attach-role-policy \
--   --role-name FulfillmentGlueRole \
--   --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
--
-- Then associate the role with Redshift Serverless namespace:
-- aws redshift-serverless update-namespace \
--   --namespace-name fulfillment-ops-ns \
--   --iam-roles arn:aws:iam::009846316315:role/FulfillmentGlueRole \
--   --region us-east-1


-- ============================================================
-- TRUNCATE staging tables before each load
-- ============================================================

TRUNCATE TABLE ops_staging.stg_order_events;
TRUNCATE TABLE ops_staging.stg_shipment_tracking;
TRUNCATE TABLE ops_staging.stg_warehouse_capacity;
TRUNCATE TABLE ops_staging.stg_returns_defects;


-- ============================================================
-- COPY from S3 curated Parquet into staging
-- ============================================================

COPY ops_staging.stg_order_events
FROM 's3://fulfillment-ops-curated-srikanth-2026/order_events/'
IAM_ROLE 'arn:aws:iam::009846316315:role/FulfillmentGlueRole'
FORMAT AS PARQUET
SERIALIZETOJSON;

COPY ops_staging.stg_shipment_tracking
FROM 's3://fulfillment-ops-curated-srikanth-2026/shipment_tracking/'
IAM_ROLE 'arn:aws:iam::009846316315:role/FulfillmentGlueRole'
FORMAT AS PARQUET
SERIALIZETOJSON;

COPY ops_staging.stg_warehouse_capacity
FROM 's3://fulfillment-ops-curated-srikanth-2026/warehouse_capacity/'
IAM_ROLE 'arn:aws:iam::009846316315:role/FulfillmentGlueRole'
FORMAT AS PARQUET
SERIALIZETOJSON;

COPY ops_staging.stg_returns_defects
FROM 's3://fulfillment-ops-curated-srikanth-2026/returns_defects/'
IAM_ROLE 'arn:aws:iam::009846316315:role/FulfillmentGlueRole'
FORMAT AS PARQUET
SERIALIZETOJSON;


-- ============================================================
-- UPSERT from staging into final fact/dim tables
-- ============================================================

-- fact_order
BEGIN;
DELETE FROM ops.fact_order
USING ops_staging.stg_order_events s
WHERE ops.fact_order.order_id = s.order_id;

INSERT INTO ops.fact_order
SELECT
    order_id, warehouse_id, product_category, order_status,
    placed_at, shipped_at, delivered_at, shift,
    units_ordered, order_value_usd, prime_member, sla_breach,
    processing_hours, order_day_of_week, is_weekend,
    fulfillment_speed_tier, event_date, ingested_at
FROM ops_staging.stg_order_events;
COMMIT;


-- fact_shipment
BEGIN;
DELETE FROM ops.fact_shipment
USING ops_staging.stg_shipment_tracking s
WHERE ops.fact_shipment.shipment_id = s.shipment_id;

INSERT INTO ops.fact_shipment
SELECT
    shipment_id, order_id, warehouse_id, carrier, tracking_number,
    shipped_at, estimated_delivery_at, actual_delivery_at,
    delivery_status, late_delivery, delivery_delay_hours,
    event_date, ingested_at
FROM ops_staging.stg_shipment_tracking;
COMMIT;


-- dim_warehouse (from capacity snapshot)
BEGIN;
DELETE FROM ops.dim_warehouse
USING ops_staging.stg_warehouse_capacity s
WHERE ops.dim_warehouse.warehouse_id = s.warehouse_id
  AND ops.dim_warehouse.effective_start_date = s.effective_start_date;

INSERT INTO ops.dim_warehouse
SELECT
    warehouse_id, warehouse_name, region, max_daily_capacity,
    units_processed, utilization_pct, active_shifts, headcount,
    effective_start_date, effective_end_date, is_current, ingested_at
FROM ops_staging.stg_warehouse_capacity;
COMMIT;


-- fact_returns_defects
BEGIN;
DELETE FROM ops.fact_returns_defects
USING ops_staging.stg_returns_defects s
WHERE ops.fact_returns_defects.record_id = s.record_id;

INSERT INTO ops.fact_returns_defects
SELECT
    record_id, order_id, warehouse_id, product_category,
    report_type, defect_type, reported_at, units_affected,
    refund_issued, root_cause_category, event_date, ingested_at
FROM ops_staging.stg_returns_defects;
COMMIT;


-- ============================================================
-- Verify row counts after load
-- ============================================================

SELECT 'fact_order'           AS table_name, COUNT(*) AS row_count FROM ops.fact_order
UNION ALL
SELECT 'fact_shipment',                       COUNT(*) FROM ops.fact_shipment
UNION ALL
SELECT 'dim_warehouse',                       COUNT(*) FROM ops.dim_warehouse
UNION ALL
SELECT 'fact_returns_defects',                COUNT(*) FROM ops.fact_returns_defects
UNION ALL
SELECT 'dim_date',                            COUNT(*) FROM ops.dim_date
UNION ALL
SELECT 'dim_carrier',                         COUNT(*) FROM ops.dim_carrier
UNION ALL
SELECT 'dim_product_category',                COUNT(*) FROM ops.dim_product_category
ORDER BY table_name;
