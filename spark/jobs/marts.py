"""
Шаг 2: аналитические витрины из очищенного Parquet.

Применяем оптимизации:
  * broadcast(dim) для маленьких справочников products/aisles/departments —
    большая таблица order_products (32M) не шафлится;
  * cache() обогащённого датасета, он переиспользуется в нескольких витринах;
  * оконные функции для ранжирования товаров внутри департамента.

Витрины (адаптированы под Instacart — он без дат/времени доставки):
  * mart_department_kpis  — KPI по департаментам (объём, reorder-rate, корзина);
  * mart_aisle_reorder    — reorder-rate по полкам + детект аномалий (z-score);
  * mart_product_rank     — топ-товары внутри департамента (window rank);
  * mart_hourly_demand    — спрос по дню недели × часу + средняя корзина;
  * mart_user_cohort      — поведение по «зрелости» клиента (номер заказа).
"""
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import broadcast

sys.path.insert(0, "/opt/spark/jobs")
from spark_utils import LAKE, MARTS, get_spark  # noqa: E402


def write_mart(df, name):
    df.coalesce(1).write.mode("overwrite").parquet(f"{MARTS}/{name}")
    print(f"[marts] {name}: {df.count()} строк")


def main():
    spark = get_spark("marts")

    op = spark.read.parquet(f"{LAKE}/order_products")
    orders = spark.read.parquet(f"{LAKE}/orders")
    products = spark.read.parquet(f"{LAKE}/products")
    aisles = spark.read.parquet(f"{LAKE}/aisles")
    departments = spark.read.parquet(f"{LAKE}/departments")

    # обогащаем позиции заказов справочниками через broadcast (без шафла op)
    enriched = (
        op.join(broadcast(products), "product_id")
          .join(broadcast(departments), "department_id")
          .join(broadcast(aisles), "aisle_id")
    ).cache()

    # 1) KPI по департаментам
    dept = enriched.groupBy("department").agg(
        F.count("*").alias("n_items"),
        F.countDistinct("order_id").alias("n_orders"),
        F.countDistinct("product_id").alias("n_products"),
        F.round(F.avg("reordered"), 4).alias("reorder_rate"),
        F.round(F.avg("add_to_cart_order"), 2).alias("avg_add_to_cart"),
    )
    write_mart(dept, "mart_department_kpis")

    # 2) reorder-rate по полкам + аномалии (z-score по всем полкам)
    aisle = enriched.groupBy("aisle").agg(
        F.count("*").alias("n_items"),
        F.round(F.avg("reordered"), 4).alias("reorder_rate"),
    )
    stats = aisle.select(F.avg("reorder_rate").alias("m"), F.stddev("reorder_rate").alias("s")).first()
    aisle = (
        aisle.withColumn("zscore", F.round((F.col("reorder_rate") - F.lit(stats["m"])) / F.lit(stats["s"]), 3))
             .withColumn("is_anomaly", (F.abs(F.col("zscore")) > 2).cast("int"))
    )
    write_mart(aisle, "mart_aisle_reorder")

    # 3) топ-10 товаров в каждом департаменте — ОКОННАЯ ФУНКЦИЯ
    prod = enriched.groupBy("department", "product_name").agg(F.count("*").alias("n_items"))
    w = Window.partitionBy("department").orderBy(F.col("n_items").desc())
    prod_rank = (
        prod.withColumn("rnk", F.row_number().over(w))
            .where(F.col("rnk") <= 10)
    )
    write_mart(prod_rank, "mart_product_rank")

    # размер корзины и reorder-rate на уровне заказа (переиспользуем ниже)
    per_order = op.groupBy("order_id").agg(
        F.count("*").alias("basket_size"),
        F.avg("reordered").alias("order_reorder_rate"),
    ).cache()

    # 4) спрос по дню недели × часу
    od = orders.join(per_order, "order_id", "left")
    hourly = od.groupBy("order_dow", "order_hour_of_day").agg(
        F.countDistinct("order_id").alias("n_orders"),
        F.round(F.avg("basket_size"), 2).alias("avg_basket_size"),
    ).withColumnRenamed("order_hour_of_day", "order_hour")
    write_mart(hourly, "mart_hourly_demand")

    # 5) когорты по «зрелости» клиента (номер заказа), кап 30
    cohort = (
        od.withColumn("order_number", F.least(F.col("order_number"), F.lit(30)))
          .groupBy("order_number").agg(
              F.countDistinct("order_id").alias("n_orders"),
              F.round(F.avg("basket_size"), 2).alias("avg_basket_size"),
              F.round(F.avg("order_reorder_rate"), 4).alias("reorder_rate"),
          )
    )
    write_mart(cohort, "mart_user_cohort")

    spark.stop()


if __name__ == "__main__":
    main()
