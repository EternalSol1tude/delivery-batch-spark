"""Общие хелперы для Spark-джоб: пути и фабрика SparkSession (local-режим)."""
import os

from pyspark.sql import SparkSession

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
RAW = f"{DATA_DIR}/raw"
LAKE = f"{DATA_DIR}/lake"
MARTS = f"{LAKE}/marts"
METRICS = f"{DATA_DIR}/metrics"
EVENTS = os.getenv("SPARK_EVENTS_DIR", f"{DATA_DIR}/spark-events")


def get_spark(app_name: str, extra: dict | None = None) -> SparkSession:
    os.makedirs(EVENTS, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "5g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "64"))
        # event-логи → Spark History Server (контейнер spark-history, порт 18080)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{EVENTS}")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
