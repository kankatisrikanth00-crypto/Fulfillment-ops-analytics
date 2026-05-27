-- ============================================================
-- Fulfillment Ops Analytics — Redshift Star Schema
-- ============================================================
-- Run this entire file against your Redshift Serverless instance
-- Database: fulfillment_ops
--
-- Schema layout:
--   ops_staging  — landing zone for Glue COPY loads
--   ops          — final fact and dimension tables
--   ops_audit    — pipeline run logs
-- ============================================================

-- ============================================================
-- SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS ops_staging;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS ops_audit;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- dim_warehouse
-- One row per fulfillment center, SCD Type 2
CREATE TABLE IF NOT EXISTS ops.dim_warehouse (
    warehouse_sk          BIGINT IDENTITY(1,1),   -- surrogate key
    warehouse_id          VARCHAR(20)   NOT NULL,
    warehouse_name        VARCHAR(100)  NOT NULL,
    region                VARCHAR(50)   NOT NULL,
    max_daily_capacity    INTEGER       NOT NULL,
    units_processed       INTEGER,
    utilization_pct       DECIMAL(5,2),
    active_shifts         INTEGER,
    headcount             INTEGER,
    effective_start_date  DATE          NOT NULL,
    effective_end_date    DATE,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE,
    ingested_at           TIMESTAMP     NOT NULL,
    PRIMARY KEY (warehouse_sk)
)
DISTSTYLE ALL
SORTKEY (warehouse_id, effective_start_date);


-- dim_date
-- Pre-populated date spine, 2024-01-01 through 2027-12-31
CREATE TABLE IF NOT EXISTS ops.dim_date (
    date_sk               INTEGER       NOT NULL,  -- YYYYMMDD
    full_date             DATE          NOT NULL,
    day_of_week           VARCHAR(10)   NOT NULL,
    day_of_week_num       INTEGER       NOT NULL,  -- 1=Sunday
    day_of_month          INTEGER       NOT NULL,
    week_of_year          INTEGER       NOT NULL,
    month_num             INTEGER       NOT NULL,
    month_name            VARCHAR(10)   NOT NULL,
    quarter               INTEGER       NOT NULL,
    year                  INTEGER       NOT NULL,
    is_weekend            BOOLEAN       NOT NULL,
    is_weekday            BOOLEAN       NOT NULL,
    PRIMARY KEY (date_sk)
)
DISTSTYLE ALL
SORTKEY (full_date);


-- dim_carrier
-- One row per carrier
CREATE TABLE IF NOT EXISTS ops.dim_carrier (
    carrier_sk            BIGINT IDENTITY(1,1),
    carrier_name          VARCHAR(100)  NOT NULL,
    carrier_type          VARCHAR(50),   -- National / Regional / Internal
    PRIMARY KEY (carrier_sk)
)
DISTSTYLE ALL;


-- dim_product_category
CREATE TABLE IF NOT EXISTS ops.dim_product_category (
    category_sk           BIGINT IDENTITY(1,1),
    category_name         VARCHAR(100)  NOT NULL,
    sla_hours             INTEGER       NOT NULL,  -- SLA threshold in hours
    PRIMARY KEY (category_sk)
)
DISTSTYLE ALL;


-- ============================================================
-- FACT TABLES
-- ============================================================

-- fact_order
-- Grain: one row per order
CREATE TABLE IF NOT EXISTS ops.fact_order (
    order_sk              BIGINT IDENTITY(1,1),
    order_id              VARCHAR(20)   NOT NULL,
    warehouse_id          VARCHAR(20)   NOT NULL,
    product_category      VARCHAR(100),
    order_status          VARCHAR(20)   NOT NULL,
    placed_at             TIMESTAMP     NOT NULL,
    shipped_at            TIMESTAMP,
    delivered_at          TIMESTAMP,
    shift                 VARCHAR(20),
    units_ordered         INTEGER,
    order_value_usd       DECIMAL(10,2),
    prime_member          BOOLEAN,
    sla_breach            BOOLEAN,
    processing_hours      DECIMAL(8,2),
    order_day_of_week     VARCHAR(10),
    is_weekend            BOOLEAN,
    fulfillment_speed_tier VARCHAR(20),
    event_date            DATE,
    ingested_at           TIMESTAMP     NOT NULL,
    PRIMARY KEY (order_sk)
)
DISTKEY (warehouse_id)
SORTKEY (event_date, warehouse_id);


-- fact_shipment
-- Grain: one row per shipment (one per shipped order)
CREATE TABLE IF NOT EXISTS ops.fact_shipment (
    shipment_sk           BIGINT IDENTITY(1,1),
    shipment_id           VARCHAR(20)   NOT NULL,
    order_id              VARCHAR(20)   NOT NULL,
    warehouse_id          VARCHAR(20)   NOT NULL,
    carrier               VARCHAR(100),
    tracking_number       VARCHAR(50),
    shipped_at            TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    actual_delivery_at    TIMESTAMP,
    delivery_status       VARCHAR(20),
    late_delivery         BOOLEAN,
    delivery_delay_hours  DECIMAL(8,2),
    event_date            DATE,
    ingested_at           TIMESTAMP     NOT NULL,
    PRIMARY KEY (shipment_sk)
)
DISTKEY (warehouse_id)
SORTKEY (event_date, warehouse_id);


-- fact_returns_defects
-- Grain: one row per return or defect report
CREATE TABLE IF NOT EXISTS ops.fact_returns_defects (
    record_sk             BIGINT IDENTITY(1,1),
    record_id             VARCHAR(20)   NOT NULL,
    order_id              VARCHAR(20)   NOT NULL,
    warehouse_id          VARCHAR(20)   NOT NULL,
    product_category      VARCHAR(100),
    report_type           VARCHAR(20)   NOT NULL,  -- RETURN / DEFECT
    defect_type           VARCHAR(100),
    reported_at           TIMESTAMP,
    units_affected        INTEGER,
    refund_issued         BOOLEAN,
    root_cause_category   VARCHAR(50),
    event_date            DATE,
    ingested_at           TIMESTAMP     NOT NULL,
    PRIMARY KEY (record_sk)
)
DISTKEY (warehouse_id)
SORTKEY (event_date);


-- ============================================================
-- STAGING TABLES (mirrors of fact/dim, no constraints)
-- ============================================================

CREATE TABLE IF NOT EXISTS ops_staging.stg_order_events (
    order_id              VARCHAR(20),
    warehouse_id          VARCHAR(20),
    product_category      VARCHAR(100),
    order_status          VARCHAR(20),
    placed_at             TIMESTAMP,
    shipped_at            TIMESTAMP,
    delivered_at          TIMESTAMP,
    shift                 VARCHAR(20),
    units_ordered         INTEGER,
    order_value_usd       DECIMAL(10,2),
    prime_member          BOOLEAN,
    sla_breach            BOOLEAN,
    processing_hours      DECIMAL(8,2),
    order_day_of_week     VARCHAR(10),
    is_weekend            BOOLEAN,
    fulfillment_speed_tier VARCHAR(20),
    event_date            DATE,
    ingested_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops_staging.stg_shipment_tracking (
    shipment_id           VARCHAR(20),
    order_id              VARCHAR(20),
    warehouse_id          VARCHAR(20),
    carrier               VARCHAR(100),
    tracking_number       VARCHAR(50),
    shipped_at            TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    actual_delivery_at    TIMESTAMP,
    delivery_status       VARCHAR(20),
    late_delivery         BOOLEAN,
    delivery_delay_hours  DECIMAL(8,2),
    event_date            DATE,
    ingested_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops_staging.stg_warehouse_capacity (
    warehouse_id          VARCHAR(20),
    warehouse_name        VARCHAR(100),
    region                VARCHAR(50),
    snapshot_date         DATE,
    max_daily_capacity    INTEGER,
    units_processed       INTEGER,
    utilization_pct       DECIMAL(5,2),
    active_shifts         INTEGER,
    headcount             INTEGER,
    effective_start_date  DATE,
    effective_end_date    DATE,
    is_current            BOOLEAN,
    ingested_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops_staging.stg_returns_defects (
    record_id             VARCHAR(20),
    order_id              VARCHAR(20),
    warehouse_id          VARCHAR(20),
    product_category      VARCHAR(100),
    report_type           VARCHAR(20),
    defect_type           VARCHAR(100),
    reported_at           TIMESTAMP,
    units_affected        INTEGER,
    refund_issued         BOOLEAN,
    root_cause_category   VARCHAR(50),
    event_date            DATE,
    ingested_at           TIMESTAMP
);


-- ============================================================
-- AUDIT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS ops_audit.pipeline_run_log (
    log_id                BIGINT IDENTITY(1,1),
    job_name              VARCHAR(100)  NOT NULL,
    target_table          VARCHAR(100)  NOT NULL,
    rows_loaded           INTEGER,
    run_status            VARCHAR(20)   NOT NULL,
    run_ts                TIMESTAMP     NOT NULL,
    PRIMARY KEY (log_id)
)
SORTKEY (run_ts);


-- ============================================================
-- POPULATE dim_date (2024-01-01 through 2027-12-31)
-- ============================================================

INSERT INTO ops.dim_date
WITH date_spine AS (
    SELECT
        DATEADD(day, seq, '2024-01-01'::DATE) AS full_date
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS seq
        FROM ops.fact_order LIMIT 1
        UNION ALL
        SELECT generate_series FROM generate_series(0, 1460)
    ) t
    WHERE DATEADD(day, seq, '2024-01-01'::DATE) <= '2027-12-31'
)
SELECT
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER)  AS date_sk,
    full_date,
    TO_CHAR(full_date, 'Day')                         AS day_of_week,
    EXTRACT(DOW FROM full_date) + 1                   AS day_of_week_num,
    EXTRACT(DAY FROM full_date)                       AS day_of_month,
    EXTRACT(WEEK FROM full_date)                      AS week_of_year,
    EXTRACT(MONTH FROM full_date)                     AS month_num,
    TO_CHAR(full_date, 'Month')                       AS month_name,
    EXTRACT(QUARTER FROM full_date)                   AS quarter,
    EXTRACT(YEAR FROM full_date)                      AS year,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0,6)
         THEN TRUE ELSE FALSE END                     AS is_weekend,
    CASE WHEN EXTRACT(DOW FROM full_date) NOT IN (0,6)
         THEN TRUE ELSE FALSE END                     AS is_weekday
FROM date_spine;


-- ============================================================
-- POPULATE dim_carrier
-- ============================================================

INSERT INTO ops.dim_carrier (carrier_name, carrier_type) VALUES
('UPS',               'National'),
('FedEx',             'National'),
('USPS',              'National'),
('Amazon Logistics',  'Internal'),
('OnTrac',            'Regional');


-- ============================================================
-- POPULATE dim_product_category
-- ============================================================

INSERT INTO ops.dim_product_category (category_name, sla_hours) VALUES
('Electronics',    24),
('Books',          48),
('Apparel',        36),
('Home & Kitchen', 48),
('Sports',         48),
('Toys',           36),
('Beauty',         24),
('Grocery',        12),
('Tools',          48),
('Pet Supplies',   36);
