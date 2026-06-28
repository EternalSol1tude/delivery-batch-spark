FROM apache/airflow:2.9.3-python3.11

# Spark (pyspark) запускается в local[*] прямо в процессе Airflow-задачи,
# поэтому в образ нужен только JRE. Отдельный Spark-кластер не разворачиваем.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
