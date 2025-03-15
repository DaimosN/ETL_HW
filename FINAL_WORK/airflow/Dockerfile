FROM apache/airflow:2.6.2

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*
USER airflow

# Устанавливаем совместимые версии пакетов
RUN pip install --no-cache-dir \
    pymongo==3.12.3 \
    psycopg2-binary==2.9.6 \
    Faker==18.11.2 \
    apache-airflow-providers-mongo==3.2.0 \
    apache-airflow-providers-postgres==5.5.0
