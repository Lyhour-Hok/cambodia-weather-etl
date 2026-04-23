FROM apache/airflow:2.7.0
USER root

RUN apt-get update -qq && \
    apt-get install -y -qq openjdk-11-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    mysql-connector-python \
    apache-airflow-providers-apache-spark==4.1.5 \
    pyspark==3.5.0