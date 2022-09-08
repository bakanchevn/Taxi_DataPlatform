from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.telegram.hooks.telegram import TelegramHook
CHAT_ID = "-1001708172657"
CONN_ID = 'TelegramBotMessage'

with DAG(
    dag_id="check_new_files",
    default_args={
        'depends_on_past': False,
        'email': 'ex@ex.org'
    },
    description='Get NY Taxi Data',
    schedule_interval='@daily',
    start_date=datetime(2022,9,1),
    catchup=False,
    tags=['NY Taxi Data'],
) as dag:
    t1 = GCSObjectExistenceSensor(
        task_id='check_new_files',
        bucket='taxi_project_data',
        google_cloud_conn_id='GCS_NY_Taxi',
        object='yellow_tripdata_2022-06.parquet'
    )

    @task
    def send_telegram_message(conn_id: str, chat_id: str) -> List[str]:
        tgHook = TelegramHook(telegram_conn_id=conn_id, chat_id=chat_id)
        tg_api_parms = {
            "text": f"File is here!"
        }
        tgHook.send_message(tg_api_parms)
        return ['True']
    t2 = send_telegram_message(conn_id=CONN_ID, chat_id=CHAT_ID)
    t1 >> t2