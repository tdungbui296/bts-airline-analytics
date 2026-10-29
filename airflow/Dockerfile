FROM apache/airflow:3.3.2-python3.12

# Isolate dbt dependencies from the Airflow runtime.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/python -m pip install \
       --no-cache-dir \
       "dbt-core==1.12.0" \
       "dbt-bigquery==1.12.0"