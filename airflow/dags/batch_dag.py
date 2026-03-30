from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir) 
sys.path.insert(0, parent_dir) # Adds parent folder to path to import pipeline.tasks.* properly

from pipeline.tasks.pull_parquet_daily import pull_api_data
from pipeline.tasks.update_catalog import update_catalog
from pipeline.tasks.run_spark_job import run_spark_job
from pipeline.tasks.load_to_clickhouse import load_to_clickhouse
from pipeline.tasks.compute_portfolio_analytics import compute_portfolio_analytics

# DAG Configuration
default_args = {'owner': 'data-team', 'retries': 2}
dag = DAG(
    name = 'crypto_batch_update', 
    default_args=default_args, 
    schedule_interval='@daily', # Runs daily at midnight
    catchup=False)

# Tasks
daily_ingest = PythonOperator( # ingests min data for batch processing
    task_id='daily_ingest',
    python_callable=pull_api_data, 
    dag=dag
)
gluecatalog_update = PythonOperator( # updates glue catalog for possible QUick
    task_id='gluecatalog_update',
    python_callable=update_catalog,
    dag=dag
)
spark_process = PythonOperator( # processes data w Spark jobs
    task_id='spark_process',
    python_callable=run_spark_job,
    dag=dag
)
clickhouse_store = PythonOperator(
    task_id='clickhouse_store',
    python_callable=load_to_clickhouse,
    dag=dag
)
# Analytics Tasks
portfolio_analyse = PythonOperator(
    task_id='portfolio_analyse',
    python_callable=compute_portfolio_analytics,
    dag=dag
)

# Dependency Flow
daily_ingest >> gluecatalog_update >> spark_process >> clickhouse_store >> portfolio_analyse