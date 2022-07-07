from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import gcs_to_bq
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id="functions_call_test",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2022, 7, 6),
    ) as dag:

    # fucntionsをコールするタスク
    function_call = BashOperator(
        task_id='function_call',
        bash_command='gcloud functions call 関数名 --data {}'
    )

    # GCSからBigQueryにデータを送るタスク
    load_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_bq',
        bucket='バケット名',
        source_objects=['ファイル名'],
        destination_project_dataset_table='データセット名.テーブル名',
        write_disposition='WRITE_APPEND',
    )

function_call >> load_bq
