"""
This DAG orchestrates the entire data pipeline (ELT) for the Loan Risk Data Warehouse project using PySpark and spark-submit.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Change path if needed
BASE_PATH = '/home/minhthanh2004kid/airflow/dags/pipeline_using_pyspark'

def get_spark_submit_cmd(module_name, function_name):
    temp_file = f"/tmp/run_{module_name}_{function_name}.py"
    
    bq_jar = f"{BASE_PATH}/spark-bigquery.jar"
    gcs_jar = f"{BASE_PATH}/gcs-connector.jar"
    
    bash_script = f"""set -e
cat << 'EOF' > {temp_file}
import sys
sys.path.append('{BASE_PATH}')
from {module_name} import {function_name}

if __name__ == '__main__':
    {function_name}()
EOF

spark-submit \\
    --master local[2] \\
    --driver-memory 6g \\
    --executor-memory 6g \\
    --jars {bq_jar},{gcs_jar} \\
    {temp_file}

rm -f {temp_file}
"""
    return bash_script

with DAG(
    dag_id="dwh_pipeline_spark_submit_parallel",
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=3,
    description='Run full parallel PySpark pipeline for Home Credit data using spark-submit',
    tags=['home_credit', 'bronze', 'silver', 'gold', 'pyspark'],
) as dag:

    # STAGE 1: BRONZE (ELT Extract)
    extract = BashOperator(
        task_id="extract_load_data_to_bronze",
        bash_command=f"python {BASE_PATH}/el_to_bronze.py"
    )

    # STAGE 2: SILVER (Data Quality & Normalize)
    silver_app = BashOperator(
        task_id="transform_application_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_application_spark")
    )
    silver_bur = BashOperator(
        task_id="transform_bureau_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_bureau_spark")
    )
    silver_bur_bal = BashOperator(
        task_id="transform_bureau_balance_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_bureau_balance_spark")
    )
    silver_cc_bal = BashOperator(
        task_id="transform_cc_balance_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_credit_card_balance_spark")
    )
    silver_pos = BashOperator(
        task_id="transform_pos_cash_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_pos_cash_balance_spark")
    )
    silver_inst = BashOperator(
        task_id="transform_installments_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_installments_payments_spark")
    )
    silver_prev = BashOperator(
        task_id="transform_previous_app_silver",
        bash_command=get_spark_submit_cmd("transform_to_silver", "transform_previous_application_spark")
    )

    silver_tasks = [silver_app, silver_bur, silver_bur_bal, silver_cc_bal, silver_pos, silver_inst, silver_prev]

    # CHECKPOINT 1
    wait_for_silver_stage = EmptyOperator(task_id="WAIT_FOR_SILVER_STAGE")

    # STAGE 3: GOLD (Dimensions & Facts)
    gold_app = BashOperator(
        task_id="transform_application_gold",
        bash_command=get_spark_submit_cmd("transform_to_gold", "transform_application_spark")
    )
    gold_bur = BashOperator(
        task_id="transform_bureau_gold",
        bash_command=get_spark_submit_cmd("transform_to_gold", "transform_bureau_spark")
    )
    gold_cc_bal = BashOperator(
        task_id="transform_credit_card_balance_gold",
        bash_command=get_spark_submit_cmd("transform_to_gold", "transform_credit_card_balance_spark")
    )
    gold_inst = BashOperator(
        task_id="transform_installments_payments_gold",
        bash_command=get_spark_submit_cmd("transform_to_gold", "transform_installments_payments_spark")
    )
    gold_prev = BashOperator(
        task_id="transform_previous_applications_gold",
        bash_command=get_spark_submit_cmd("transform_to_gold", "transform_previous_applications_spark")
    )
    gold_pos = BashOperator(
        task_id="transform_pos_cash_balance_gold",
        bash_command=get_spark_submit_cmd("transform_to_gold", "transform_pos_cash_balance_spark")
    )

    gold_tasks = [gold_app, gold_bur, gold_cc_bal, gold_inst, gold_prev, gold_pos]

    # CHECKPOINT 2
    pipeline_complete = EmptyOperator(task_id="PIPELINE_COMPLETE")

    # RUN DEPENDENCIES
    extract >> silver_tasks >> wait_for_silver_stage >> gold_tasks >> pipeline_complete