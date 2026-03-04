# Airflow DAG: daily ETL from MongoDB -> PostgreSQL -> CSV reports

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os
import sys

# Add plugins directory to path so we can import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../plugins')))

from insert_data_into_postgres import get_and_insert_data
from daily_report import generate_daily_report

logger = logging.getLogger('flights_dag')

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 5, 1),
}

dag = DAG(
    dag_id="flights_dag",
    default_args=default_args,
    description="Daily: MongoDB landed flights -> PostgreSQL -> CSV reports",
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flights", "etl"],
)


def start_job():
    logger.info("Pipeline started.")


def fetch_and_insert():
    try:
        logger.info("Fetching landed flights from MongoDB...")
        get_and_insert_data()
        logger.info("Data inserted into PostgreSQL.")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise


def create_reports():
    try:
        logger.info("Generating daily reports...")
        generate_daily_report()
        logger.info("Reports saved to /opt/airflow/outputs/")
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise


def end_job():
    logger.info("All tasks completed successfully.")


# Task definitions
start_task = PythonOperator(
    task_id='start',
    python_callable=start_job,
    dag=dag,
)

etl_task = PythonOperator(
    task_id='mongo_to_postgres',
    python_callable=fetch_and_insert,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_reports',
    python_callable=create_reports,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end',
    python_callable=end_job,
    dag=dag,
)

# Task order: start -> ETL -> reports -> end
start_task >> etl_task >> report_task >> end_task