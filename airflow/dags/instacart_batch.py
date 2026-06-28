"""
DAG: instacart_batch — батч-обработка Instacart на Spark.

Поток (event-driven через FileSensor):
  wait_for_file (ждёт data/landing/READY)
      └─ clean_to_parquet (CSV -> очищенный Parquet)
            ├─ build_marts (витрины: broadcast join + оконные функции)
            │     └─ dq_checks (контракты данных; падение -> стоп пайплайна)
            │           └─ load_clickhouse (Parquet витрин -> ClickHouse)
            └─ benchmark (замер оптимизаций; не блокирует загрузку)

Spark выполняется в local[*] прямо в задаче (pyspark в образе Airflow).
Чтобы запустить батч: создать файл-маркер  data/landing/READY
(см. scripts/trigger_batch.sh) — имитация «приехал новый файл».
"""
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from ch_load import load_marts

JOBS = "/opt/spark/jobs"
READY_FILE = "/opt/airflow/data/landing/READY"


def spark_task(dag, task_id, script):
    return BashOperator(
        task_id=task_id,
        bash_command=f"python {JOBS}/{script}",
        dag=dag,
    )


with DAG(
    dag_id="instacart_batch",
    description="Spark batch: Instacart CSV -> витрины -> ClickHouse",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule=None,            # запуск по появлению файла (FileSensor) / вручную
    catchup=False,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["spark", "batch", "instacart"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=READY_FILE,
        poke_interval=15,
        timeout=60 * 30,
        mode="reschedule",     # освобождает слот воркера, пока ждёт
    )

    clean = spark_task(dag, "clean_to_parquet", "clean_to_parquet.py")
    marts = spark_task(dag, "build_marts", "marts.py")
    benchmark = spark_task(dag, "benchmark", "benchmark_join.py")
    dq = spark_task(dag, "dq_checks", "dq_checks.py")

    load_clickhouse = PythonOperator(
        task_id="load_clickhouse",
        python_callable=load_marts,
    )

    wait_for_file >> clean
    clean >> marts >> dq >> load_clickhouse
    clean >> benchmark
