"""
Шаг 1: сырые CSV (Instacart) -> очищенный Parquet (lake).

  * явные схемы (без inferSchema — на 32M строк это дорого и недетерминировано);
  * типизация, отбрасывание строк с NULL в ключах;
  * дедупликация по естественным ключам;
  * prior + train объединяются в одну таблицу order_products (с метой eval_set).

Колоночный Parquet с предсказуемой схемой — вход для всех витрин и бенчмарков.
"""
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, DoubleType)

sys.path.insert(0, "/opt/spark/jobs")
from spark_utils import LAKE, RAW, get_spark  # noqa: E402

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("eval_set", StringType()),
    StructField("order_number", IntegerType()),
    StructField("order_dow", IntegerType()),
    StructField("order_hour_of_day", IntegerType()),
    StructField("days_since_prior_order", DoubleType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("aisle_id", IntegerType()),
    StructField("department_id", IntegerType()),
])

AISLES_SCHEMA = StructType([
    StructField("aisle_id", IntegerType()),
    StructField("aisle", StringType()),
])

DEPARTMENTS_SCHEMA = StructType([
    StructField("department_id", IntegerType()),
    StructField("department", StringType()),
])

OP_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("add_to_cart_order", IntegerType()),
    StructField("reordered", IntegerType()),
])


def read_csv(spark, name, schema):
    return spark.read.option("header", True).schema(schema).csv(f"{RAW}/{name}")


def main():
    spark = get_spark("clean_to_parquet")

    # --- справочники: маленькие, дедуп по ключу, пишем одним файлом ---
    for name, schema, key in [
        ("products.csv", PRODUCTS_SCHEMA, "product_id"),
        ("aisles.csv", AISLES_SCHEMA, "aisle_id"),
        ("departments.csv", DEPARTMENTS_SCHEMA, "department_id"),
    ]:
        df = read_csv(spark, name, schema).where(F.col(key).isNotNull()).dropDuplicates([key])
        out = name.replace(".csv", "")
        df.coalesce(1).write.mode("overwrite").parquet(f"{LAKE}/{out}")
        print(f"[clean] {out}: {df.count()} строк")

    # --- orders ---
    orders = (
        read_csv(spark, "orders.csv", ORDERS_SCHEMA)
        .where(F.col("order_id").isNotNull() & F.col("user_id").isNotNull())
        .dropDuplicates(["order_id"])
    )
    orders.write.mode("overwrite").parquet(f"{LAKE}/orders")
    print(f"[clean] orders: {orders.count()} строк")

    # --- order_products: prior + train -> одна таблица ---
    prior = read_csv(spark, "order_products__prior.csv", OP_SCHEMA).withColumn("eval_set", F.lit("prior"))
    train = read_csv(spark, "order_products__train.csv", OP_SCHEMA).withColumn("eval_set", F.lit("train"))
    op = (
        prior.unionByName(train)
        .where(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
        .withColumn("reordered", F.coalesce(F.col("reordered"), F.lit(0)))
        .dropDuplicates(["order_id", "product_id"])
    )
    # перепартиционируем по числу шафл-партиций — равномерные файлы под чтение
    (op.repartition(int(spark.conf.get("spark.sql.shuffle.partitions")))
       .write.mode("overwrite").parquet(f"{LAKE}/order_products"))
    print(f"[clean] order_products: {op.count()} строк")

    spark.stop()


if __name__ == "__main__":
    main()
