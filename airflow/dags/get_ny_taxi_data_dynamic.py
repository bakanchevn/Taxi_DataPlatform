from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any, Type

import requests
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup


CHAT_ID = "-1001708172657"
CONN_ID = 'TelegramBotMessage'
TRIP_DATA_FILES = ['yellow_tripdata', 'green_tripdata', 'fhv_tripdata']

@task
def get_NY_Taxi_data(url_name: str) -> str | None:
    """
    download the NY Taxi Data from the cloudfront URL
    """
    if not url_name:
        return None
    ret_file_name = 'None'
    gcs_hook = GCSHook('GCS_NY_Taxi')
    file_name = url_name.split('/')[-1]
    print(url_name)
    resp = requests.get(url_name, timeout=100)
    print(resp.status_code)
    if resp.status_code == 200:
        ret_file_name = file_name
        gcs_hook.upload('taxi_project_data', file_name, data=resp.content, timeout=400)
    return ret_file_name


@task
def checkGcsFiles(taxi_type: str, year: List[int], month: List[int]) -> List[str]:
    """
    Check if files are already in GCS
    """
    # 2021, 2022
    # 1, 2, ... , 12
    gcs_hook = GCSHook('GCS_NY_Taxi')

    missing_file_list = []
    for y in year:
        for m in month:
            file_name = f'{taxi_type}_{y}-{m:02d}.parquet'
            url_name = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_{y}-{m:02d}.parquet'
            if not gcs_hook.exists('taxi_project_data', file_name):
                missing_file_list.append(url_name)
    return missing_file_list



@dag(schedule_interval='@daily', dag_id='process_ny_taxi_dynamic', start_date=datetime(2022,9,1), catchup=False, tags=['NY Taxi Data'])
def task_flow():

    @task
    def send_telegram_message(data:List[str], taxi_type: str, conn_id: str, chat_id: str) -> List[str]:
        tgHook = TelegramHook(telegram_conn_id=conn_id, chat_id=chat_id)
        data_list = [x for x in data if x != 'None']
        tg_api_parms = {
            "text": f"Processed {data_list if data_list else '0'} files for {taxi_type}"
        }
        tgHook.send_message(tg_api_parms)
        return data_list

    @task.branch()
    def get_cnt_from_data_processing(**kwargs) -> str:
        cnt = []
        for file_type in TRIP_DATA_FILES:
            cnt_gather = kwargs['ti'].xcom_pull(task_ids=f'get_taxi_data.send_tg_{file_type}')
            print(cnt_gather)
            if cnt_gather:
                cnt.extend(cnt_gather)

        if len(cnt) == 0:
            return 'no_files_needed'
        else:
            kwargs['ti'].xcom_push(key='cnt', value=cnt)
            return f'push_data_to_bq'

    @task_group(group_id='get_taxi_data')
    def get_taxi_data() -> List[str]:
        sum_cnt = []

        for taxi_type in TRIP_DATA_FILES:
            get_missing_files = checkGcsFiles.override(task_id=f'get_missing_files_{taxi_type}')(taxi_type, [2022],
                                                                                    [x for x in range(1, 13)])
            # branch = get_files_if_needed(get_missing_files, taxi_type)
            process_task = get_NY_Taxi_data.override(task_id=f'process_{taxi_type}').partial().expand(
                url_name=get_missing_files)
            sum_cnt.append(send_telegram_message.override(task_id=f'send_tg_{taxi_type}')(process_task, taxi_type, conn_id=CONN_ID, chat_id=CHAT_ID))
        return sum_cnt
    #
    # bucket: Any,
    # source_objects: Any,
    # destination_project_dataset_table: Any,
    # source_format: str = 'CSV',
    @task
    def push_data_to_bq(**kwargs) -> List[str]:
        cnt = kwargs['ti'].xcom_pull(key='cnt')

        op = GCSToBigQueryOperator(
                          task_id='push_data_to_bq_2',
                          bucket = 'taxi_project_data',
                          source_objects=cnt,
                         schema_fields=[
                              {'name': 'VendorID', 'type': 'STRING', 'mode': 'NULLABLE'},
                          ],
                          destination_project_dataset_table='taxi_data.bq_taxi_data',
                          source_format='PARQUET',
                          write_disposition='WRITE_TRUNCATE',
                          dag=dag,
                          gcp_conn_id='GCS_NY_Taxi')
        op.execute(None)
        return cnt

    no_files_needed = EmptyOperator(task_id='no_files_needed')
    EmptyOperator(task_id = 'start') >> get_taxi_data() >> get_cnt_from_data_processing() >> [no_files_needed, push_data_to_bq()]
            # branch >> [send_telegram_message, EmptyOperator(task_id=f'no_files_needed_{taxi_type}')]
    #

    # process_yellow = get_NY_Taxi_data.partial(taxi_type='yellow_tripdata').expand(year=[2022], month=[x for x in range(1, 13)])
    # process_green = get_NY_Taxi_data.override(task_id='process_green').partial(taxi_type='green_tripdata').expand(year=[2022], month=[x for x in range(1, 13)])
    # print_processed_files.override(task_id='print_yellow')(process_yellow)
    # print_processed_files(process_green)

dag = task_flow()




# with DAG(
#     dag_id="get_NY_Taxi_data_dynamic",
#     default_args={
#         'depends_on_past': False,
#         'email': 'ex@ex.e',
#     },
#         description='Get NY Taxi Data',
#     schedule_interval='@daily',
#     start_date=datetime(2022,9,1),
#     catchup=False,
#     tags=['NY Taxi Data'],
# ) as dag:
#     t1 = get_NY_Taxi_data.partial(taxi_type = 'yellow_tripdata').expand(year=[2022], month=[x for x in range(1, 13)])
#     t2 = get_NY_Taxi_data.partial(taxi_type='green_tripdata').expand(year=[2022],
#                                                                       month=[x for x in range(1, 13)])
#
#     t3 = PythonOperator(
#         task_id='print_processed_files',
#         python_callable=print_processed_files,
#         provide_context=True
#     )
#
#     t1 >> t2 >> t3
