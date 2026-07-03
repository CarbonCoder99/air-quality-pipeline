FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /requirements.txt

# Removed --user flag — the base Airflow image uses a virtualenv
# pip installs directly into it without --user
RUN pip install --no-cache-dir -r /requirements.txt

