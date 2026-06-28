"""
Data Quality слой. Проверяет очищенный lake и витрины; при нарушении контракта
данных падает с ненулевым кодом -> Airflow помечает таск failed и пайплайн стоит.

Проверки:
  * ключи без NULL и дублей (order_products, orders);
  * reordered ∈ {0,1};
  * ссылочная целостность: все product_id из order_products есть в products;
  * витрины непустые, reorder_rate ∈ [0,1].
"""
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark/jobs")
from spark_utils import LAKE, MARTS, get_spark  # noqa: E402

failures: list[str] = []


def check(name: str, ok: bool, detail: str = ""):
    status = "OK" if ok else "FAIL"
    print(f"[dq] {status}: {name} {detail}")
    if not ok:
        failures.append(name)


def main():
    spark = get_spark("dq_checks")
    op = spark.read.parquet(f"{LAKE}/order_products")
    orders = spark.read.parquet(f"{LAKE}/orders")
    products = spark.read.parquet(f"{LAKE}/products")

    # ключи
    check("order_products.keys_not_null",
          op.where(F.col("order_id").isNull() | F.col("product_id").isNull()).count() == 0)
    check("orders.order_id_unique",
          orders.count() == orders.select("order_id").distinct().count())
    check("order_products.no_dup_keys",
          op.count() == op.select("order_id", "product_id").distinct().count())

    # домен значений
    bad_reordered = op.where(~F.col("reordered").isin(0, 1)).count()
    check("order_products.reordered_in_{0,1}", bad_reordered == 0, f"(нарушений: {bad_reordered})")

    # ссылочная целостность order_products -> products
    orphans = op.join(products, "product_id", "left_anti").count()
    check("ref_integrity.product_id_in_products", orphans == 0, f"(сирот: {orphans})")

    # витрины
    for mart in ["mart_department_kpis", "mart_aisle_reorder", "mart_product_rank",
                 "mart_hourly_demand", "mart_user_cohort"]:
        cnt = spark.read.parquet(f"{MARTS}/{mart}").count()
        check(f"{mart}.not_empty", cnt > 0, f"(строк: {cnt})")

    for mart in ["mart_department_kpis", "mart_aisle_reorder", "mart_user_cohort"]:
        df = spark.read.parquet(f"{MARTS}/{mart}")
        bad = df.where((F.col("reorder_rate") < 0) | (F.col("reorder_rate") > 1)).count()
        check(f"{mart}.reorder_rate_in_[0,1]", bad == 0, f"(нарушений: {bad})")

    spark.stop()

    if failures:
        print(f"[dq] ПРОВАЛЕНО проверок: {len(failures)} -> {failures}")
        sys.exit(1)
    print("[dq] все проверки пройдены")


if __name__ == "__main__":
    main()
