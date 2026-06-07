FROM apache/airflow:2.9.1-python3.11

# Java requis pour PySpark
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Dépendances Python pour les jobs ETL
USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-postgres==5.10.2
