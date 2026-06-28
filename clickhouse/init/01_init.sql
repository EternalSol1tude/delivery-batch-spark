CREATE DATABASE IF NOT EXISTS analytics;

-- Витрины, которые наполняет Spark-пайплайн. MergeTree + осмысленный ORDER BY
-- под типовые сортировки/фильтры в BI.

CREATE TABLE IF NOT EXISTS analytics.mart_department_kpis
(
    department       String,
    n_items          UInt64,
    n_orders         UInt64,
    n_products       UInt64,
    reorder_rate     Float64,
    avg_add_to_cart  Float64
) ENGINE = MergeTree ORDER BY department;

CREATE TABLE IF NOT EXISTS analytics.mart_aisle_reorder
(
    aisle         String,
    n_items       UInt64,
    reorder_rate  Float64,
    zscore        Float64,
    is_anomaly    UInt8
) ENGINE = MergeTree ORDER BY aisle;

CREATE TABLE IF NOT EXISTS analytics.mart_product_rank
(
    department    String,
    product_name  String,
    n_items       UInt64,
    rnk           UInt32
) ENGINE = MergeTree ORDER BY (department, rnk);

CREATE TABLE IF NOT EXISTS analytics.mart_hourly_demand
(
    order_dow         UInt8,
    order_hour        UInt8,
    n_orders          UInt64,
    avg_basket_size   Float64
) ENGINE = MergeTree ORDER BY (order_dow, order_hour);

CREATE TABLE IF NOT EXISTS analytics.mart_user_cohort
(
    order_number     UInt16,
    n_orders         UInt64,
    avg_basket_size  Float64,
    reorder_rate     Float64
) ENGINE = MergeTree ORDER BY order_number;
