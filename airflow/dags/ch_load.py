"""
Загрузка витрин из Parquet в ClickHouse (вызывается из DAG).
Это НЕ DAG-файл. Витрины маленькие (агрегаты), поэтому читаем pyarrow и
вставляем через clickhouse-connect; TRUNCATE перед вставкой -> идемпотентность.
"""
import os

import clickhouse_connect
import pyarrow.dataset as ds

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
MARTS_DIR = f"{DATA_DIR}/lake/marts"
DB = os.getenv("CLICKHOUSE_DB", "analytics")

# порядок колонок строго как в DDL (clickhouse/init/01_init.sql)
MARTS = {
    "mart_department_kpis": ["department", "n_items", "n_orders", "n_products",
                             "reorder_rate", "avg_add_to_cart"],
    "mart_aisle_reorder": ["aisle", "n_items", "reorder_rate", "zscore", "is_anomaly"],
    "mart_product_rank": ["department", "product_name", "n_items", "rnk"],
    "mart_hourly_demand": ["order_dow", "order_hour", "n_orders", "avg_basket_size"],
    "mart_user_cohort": ["order_number", "n_orders", "avg_basket_size", "reorder_rate"],
}


def client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def load_marts():
    ch = client()
    for table, cols in MARTS.items():
        df = ds.dataset(f"{MARTS_DIR}/{table}").to_table().to_pandas()[cols]
        ch.command(f"TRUNCATE TABLE {DB}.{table}")
        ch.insert_df(f"{DB}.{table}", df)
        print(f"[ch_load] {DB}.{table}: загружено {len(df)} строк")


if __name__ == "__main__":
    load_marts()
