"""
Замер эффекта оптимизаций Spark «до/после» — главная метрика проекта в резюме.

Тест A. Join order_products (32M) × products: shuffle (sort-merge) join против
        broadcast join маленького справочника.
Тест B. Одна и та же агрегация поверх исходных CSV против Parquet — колоночный
        формат, сжатие и отсутствие парсинга дают кратный выигрыш на чтении.
Плюс честная метрика перекоса (skew) ключа product_id: max/median строк на ключ.

Действие-сток — `.write.format("noop")`: форсирует полное выполнение плана без
записи результата, поэтому замер времени честный. Результаты -> metrics/benchmark.json.
"""
import json
import os
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql.types import IntegerType, StructField, StructType  # noqa: E402

from spark_utils import LAKE, METRICS, RAW, get_spark  # noqa: E402

OP_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("add_to_cart_order", IntegerType()),
    StructField("reordered", IntegerType()),
])


def timed(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return round(time.perf_counter() - t0, 2)


def noop(df):
    df.write.format("noop").mode("overwrite").save()


def main():
    spark = get_spark("benchmark_join")
    op = spark.read.parquet(f"{LAKE}/order_products")
    products = spark.read.parquet(f"{LAKE}/products")
    departments = spark.read.parquet(f"{LAKE}/departments")

    # ---------- Тест A: shuffle join vs broadcast join ----------
    def shuffle_join():
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # запрещаем авто-broadcast
        j = op.join(products, "product_id").groupBy("department_id").agg(F.count("*"))
        noop(j)

    def broadcast_join():
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        j = op.join(broadcast(products), "product_id").groupBy("department_id").agg(F.count("*"))
        noop(j)

    shuffle_sec = timed(shuffle_join)
    broadcast_sec = timed(broadcast_join)

    # ---------- Тест B: чтение CSV vs Parquet (одинаковая агрегация) ----------
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    def agg_csv():
        df = (spark.read.option("header", True).schema(OP_SCHEMA)
              .csv([f"{RAW}/order_products__prior.csv", f"{RAW}/order_products__train.csv"]))
        noop(df.groupBy("reordered").agg(F.count("*")))

    def agg_parquet():
        df = spark.read.parquet(f"{LAKE}/order_products")
        noop(df.groupBy("reordered").agg(F.count("*")))

    csv_sec = timed(agg_csv)
    parquet_sec = timed(agg_parquet)

    # ---------- честная метрика перекоса ключа product_id ----------
    cnts = op.groupBy("product_id").count()
    sk = cnts.agg(
        F.max("count").alias("mx"),
        F.expr("percentile_approx(count, 0.5)").alias("med"),
    ).first()
    skew_ratio = round(sk["mx"] / sk["med"], 1) if sk["med"] else None

    metrics = {
        "rows_order_products": op.count(),
        "testA_shuffle_join_sec": shuffle_sec,
        "testA_broadcast_join_sec": broadcast_sec,
        "testA_speedup_x": round(shuffle_sec / broadcast_sec, 2) if broadcast_sec else None,
        "testB_csv_read_sec": csv_sec,
        "testB_parquet_read_sec": parquet_sec,
        "testB_speedup_x": round(csv_sec / parquet_sec, 2) if parquet_sec else None,
        "skew_product_id_max_rows": int(sk["mx"]),
        "skew_product_id_median_rows": int(sk["med"]),
        "skew_ratio_max_to_median": skew_ratio,
    }
    os.makedirs(METRICS, exist_ok=True)
    with open(f"{METRICS}/benchmark.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print("[benchmark]", json.dumps(metrics, ensure_ascii=False))

    spark.stop()


if __name__ == "__main__":
    main()
