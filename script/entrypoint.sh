#!/bin/bash
set -e

pip install -r /opt/airflow/requirements.txt
airflow db upgrade

airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

exec airflow webserver
