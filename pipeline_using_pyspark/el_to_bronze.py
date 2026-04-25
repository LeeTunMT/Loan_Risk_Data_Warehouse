"""
Similar code

"""
from datetime import datetime
from google.cloud import bigquery

def extract_csv_to_bronze():
    bucket = 'homecreditdataset'
    dataset_bronze = 'bronze_stage_spark'
    project_id = 'loan-risk-home-credit'
    client = bigquery.Client(project=project_id)

    FILES_TO_LOAD = [
        'application_test.csv',
        'application_train.csv',
        'bureau.csv', 
        'bureau_balance.csv', 
        'credit_card_balance.csv', 
        'installments_payments.csv', 
        'previous_application.csv', 
        'POS_CASH_balance.csv'
    ]

    for file_name in FILES_TO_LOAD:
        try: 
            table_name = file_name.split(".")[0]
            table_id = f'{project_id}.{dataset_bronze}.{table_name}'
            print(f'Loading {file_name} into {table_id}...')
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                allow_jagged_rows=True,
                ignore_unknown_values=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            uri = f'gs://{bucket}/{file_name}'

            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()  # Waits for the job to complete.
            print(f'Loaded {file_name} into {table_id}')
        except Exception as e:
            print(f'Error loading {file_name}: {e}')

if __name__ == "__main__":
    extract_csv_to_bronze()
