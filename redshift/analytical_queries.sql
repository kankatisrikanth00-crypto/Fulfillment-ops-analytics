-- ============================================================
-- Fulfillment Ops Analytics — Analytical SQL Layer
-- ============================================================
-- These queries power the QuickSight dashboard datasets.
-- Run against fulfillment_ops database, ops schema.
-- ============================================================


-- ============================================================
-- 1. DAILY OPS SCORECARD
-- KPIs per warehouse per day — the main dashboard view
-- ============================================================

CREATE OR REPLACE VIEW ops.v_daily_ops_scorecard AS
SELECT
    o.event_date,
    o.warehouse_id,
    d.day_of_week,
    d.is_weekend,

    -- Volume
    COUNT(o.order_sk)                                           AS total_orders,
    SUM(o.units_ordered)                                        AS total_units,
    ROUND(SUM(o.order_value_usd), 2)                           AS total_revenue_usd,

    -- Fulfillment health
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN 1 ELSE 0 END)   AS delivered_orders,
    SUM(CASE WHEN o.order_status = 'CANCELLED' THEN 1 ELSE 0 END)   AS cancelled_orders,
    SUM(CASE WHEN o.order_status = 'RETURNED'  THEN 1 ELSE 0 END)   AS returned_orders,
    SUM(CASE WHEN o.sla_breach = TRUE          THEN 1 ELSE 0 END)   AS sla_breaches,

    -- Rates
    ROUND(100.0 * SUM(CASE WHEN o.order_status = 'DELIVERED' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(o.order_sk), 0), 2)                        AS delivery_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN o.sla_breach = TRUE THEN 1 ELSE 0 END)
          / NULLIF(COUNT(o.order_sk), 0), 2)                        AS sla_breach_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN o.order_status = 'CANCELLED' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(o.order_sk), 0), 2)                        AS cancellation_rate_pct,

    -- Speed
    ROUND(AVG(CASE WHEN o.processing_hours > 0
              THEN o.processing_hours END), 2)                      AS avg_processing_hours,

    -- Prime vs non-Prime
    SUM(CASE WHEN o.prime_member = TRUE THEN 1 ELSE 0 END)          AS prime_orders,
    SUM(CASE WHEN o.prime_member = FALSE THEN 1 ELSE 0 END)         AS non_prime_orders

FROM ops.fact_order o
LEFT JOIN ops.dim_date d ON o.event_date = d.full_date
GROUP BY
    o.event_date,
    o.warehouse_id,
    d.day_of_week,
    d.is_weekend
ORDER BY o.event_date DESC, o.warehouse_id;


-- ============================================================
-- 2. SHIFT PERFORMANCE
-- Throughput and SLA by shift within each warehouse
-- ============================================================

CREATE OR REPLACE VIEW ops.v_shift_performance AS
SELECT
    event_date,
    warehouse_id,
    shift,
    COUNT(*)                                                        AS orders_processed,
    SUM(units_ordered)                                             AS units_processed,
    ROUND(AVG(processing_hours), 2)                               AS avg_processing_hours,
    SUM(CASE WHEN sla_breach = TRUE THEN 1 ELSE 0 END)            AS sla_breaches,
    ROUND(100.0 * SUM(CASE WHEN sla_breach = TRUE THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                               AS sla_breach_rate_pct,
    ROUND(SUM(order_value_usd), 2)                                AS shift_revenue_usd,

    -- Rank shift within day+warehouse by order volume
    RANK() OVER (
        PARTITION BY event_date, warehouse_id
        ORDER BY COUNT(*) DESC
    )                                                              AS shift_rank_by_volume

FROM ops.fact_order
GROUP BY event_date, warehouse_id, shift
ORDER BY event_date DESC, warehouse_id, shift;


-- ============================================================
-- 3. CARRIER PERFORMANCE
-- On-time delivery rate and delay analysis by carrier
-- ============================================================

CREATE OR REPLACE VIEW ops.v_carrier_performance AS
SELECT
    s.event_date,
    s.carrier,
    COUNT(s.shipment_sk)                                           AS total_shipments,
    SUM(CASE WHEN s.late_delivery = TRUE  THEN 1 ELSE 0 END)      AS late_shipments,
    SUM(CASE WHEN s.late_delivery = FALSE THEN 1 ELSE 0 END)      AS on_time_shipments,
    ROUND(100.0 * SUM(CASE WHEN s.late_delivery = FALSE THEN 1 ELSE 0 END)
          / NULLIF(COUNT(s.shipment_sk), 0), 2)                   AS on_time_rate_pct,
    ROUND(AVG(CASE WHEN s.delivery_delay_hours > 0
              THEN s.delivery_delay_hours END), 2)                AS avg_delay_hours,
    ROUND(MAX(s.delivery_delay_hours), 2)                         AS max_delay_hours,

    -- 7-day rolling on-time rate
    ROUND(100.0 * SUM(SUM(CASE WHEN s.late_delivery = FALSE THEN 1 ELSE 0 END))
          OVER (PARTITION BY s.carrier
                ORDER BY s.event_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
          / NULLIF(SUM(COUNT(s.shipment_sk))
          OVER (PARTITION BY s.carrier
                ORDER BY s.event_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0), 2) AS rolling_7d_on_time_pct

FROM ops.fact_shipment s
GROUP BY s.event_date, s.carrier
ORDER BY s.event_date DESC, on_time_rate_pct ASC;


-- ============================================================
-- 4. DEFECT & RETURN ANALYSIS
-- Root cause breakdown by warehouse and category
-- ============================================================

CREATE OR REPLACE VIEW ops.v_defect_analysis AS
SELECT
    r.event_date,
    r.warehouse_id,
    r.product_category,
    r.report_type,
    r.defect_type,
    r.root_cause_category,
    COUNT(r.record_sk)                                             AS total_incidents,
    SUM(r.units_affected)                                         AS total_units_affected,
    SUM(CASE WHEN r.refund_issued = TRUE THEN 1 ELSE 0 END)       AS refunds_issued,
    ROUND(100.0 * SUM(CASE WHEN r.refund_issued = TRUE THEN 1 ELSE 0 END)
          / NULLIF(COUNT(r.record_sk), 0), 2)                    AS refund_rate_pct,

    -- Defect rate vs total orders for same warehouse+date
    ROUND(100.0 * COUNT(r.record_sk)
          / NULLIF((
              SELECT COUNT(*) FROM ops.fact_order o
              WHERE o.warehouse_id = r.warehouse_id
              AND o.event_date = r.event_date
          ), 0), 2)                                               AS defect_rate_pct

FROM ops.fact_returns_defects r
GROUP BY
    r.event_date,
    r.warehouse_id,
    r.product_category,
    r.report_type,
    r.defect_type,
    r.root_cause_category
ORDER BY r.event_date DESC, total_incidents DESC;


-- ============================================================
-- 5. WAREHOUSE UTILIZATION TREND
-- Capacity vs throughput over time
-- ============================================================

CREATE OR REPLACE VIEW ops.v_warehouse_utilization AS
SELECT
    w.snapshot_date,
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.max_daily_capacity,
    w.units_processed,
    w.utilization_pct,
    w.headcount,
    w.active_shifts,

    -- Week-over-week utilization change
    w.utilization_pct - LAG(w.utilization_pct, 7) OVER (
        PARTITION BY w.warehouse_id
        ORDER BY w.snapshot_date
    )                                                              AS wow_utilization_change,

    -- Flag over-capacity days (>95%)
    CASE WHEN w.utilization_pct > 95 THEN TRUE ELSE FALSE END     AS is_near_capacity,

    -- Rolling 7-day avg utilization
    ROUND(AVG(w.utilization_pct) OVER (
        PARTITION BY w.warehouse_id
        ORDER BY w.snapshot_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                          AS rolling_7d_avg_utilization

FROM ops.dim_warehouse w
WHERE w.is_current = TRUE
   OR w.effective_end_date IS NOT NULL
ORDER BY w.snapshot_date DESC, w.warehouse_id;


-- ============================================================
-- 6. EXECUTIVE SUMMARY — 30-DAY ROLLUP
-- Single-row KPI summary for scorecard tiles in QuickSight
-- ============================================================

CREATE OR REPLACE VIEW ops.v_executive_summary AS
SELECT
    MIN(event_date)                                                AS period_start,
    MAX(event_date)                                                AS period_end,
    COUNT(order_sk)                                                AS total_orders,
    SUM(units_ordered)                                             AS total_units,
    ROUND(SUM(order_value_usd), 2)                                AS total_revenue_usd,
    ROUND(AVG(order_value_usd), 2)                                AS avg_order_value_usd,
    ROUND(100.0 * SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(order_sk), 0), 2)                        AS overall_delivery_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN sla_breach = TRUE THEN 1 ELSE 0 END)
          / NULLIF(COUNT(order_sk), 0), 2)                        AS overall_sla_breach_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(order_sk), 0), 2)                        AS overall_cancellation_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN order_status = 'RETURNED' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(order_sk), 0), 2)                        AS overall_return_rate_pct,
    ROUND(AVG(CASE WHEN processing_hours > 0
              THEN processing_hours END), 2)                      AS avg_processing_hours,
    SUM(CASE WHEN prime_member = TRUE THEN 1 ELSE 0 END)          AS prime_orders,
    ROUND(100.0 * SUM(CASE WHEN prime_member = TRUE THEN 1 ELSE 0 END)
          / NULLIF(COUNT(order_sk), 0), 2)                        AS prime_order_pct
FROM ops.fact_order;
