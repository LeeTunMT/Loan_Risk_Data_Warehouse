"""
This DAG orchestrates the entire data pipeline (ELT) for the Loan Risk Data Warehouse project, including:
1. Extracting and Loading data from CSV files in Google Cloud Storage to the Bronze stage in BigQuery.
2. Transforming data from the Bronze stage to the Silver stage using PySpark.
3. Transforming data from the Silver stage to the Gold stage using Python.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dwh_pipeline_test",
    start_date=datetime(2026, 3, 18),
    schedule_interval= '@daily',
    catchup=False,
    description='Run full pipeline from GCS to BigQuery Bronze, Silver, and Gold for Home Credit data',
    tags=['home_credit', 'bronze', 'silver', 'gold', 'Data Warehouse'],
) as dag:

    extract = BashOperator(
        task_id="extract_data",
        bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/el_to_bronze.py"
    )

    transform = BashOperator(
        task_id="transform_data_to_silver",
        bash_command="spark-submit /home/minhthanh2004kid/airflow/dags/pipeline/transform_to_silver.py"
    )

    load = BashOperator(
        task_id="transform_data_to_gold",
        bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/transform_to_gold.py"
    )

    extract >> transform >> load