from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dwh_pipeline_test",
    start_date=datetime(2026, 3, 18),
    schedule_interval=None,
    catchup=False,
    description='Load Home Credit CSVs from GCS to BigQuery Bronze',
    tags=['home_credit', 'bronze', 'test'],
) as dag:

    extract = BashOperator(

        task_id="extract_data",
        bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/el_to_bronze.py"
    )
    transform_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command="python /home/minhthanh2004kid/airflow/dags/pipeline/transform_to_silver.py"
    ) 
    
    extract >> transform_silver