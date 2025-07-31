#!/usr/bin/env bash
airflow db migrate

airflow users create \
    --username airflow \
    --password airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email airflow@example.com || true

exec airflow "$@"
