from datetime import datetime
from textwrap import dedent
from typing import List

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


with DAG(
    dag_id="get_NY_Taxi_data",
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



    def get_NY_Taxi_data(taxi_type: str, year_list: List[int], month_list: List[int]) -> List[str]:
        """
        Get list of files to download
        """
        file_list = []
        gcs_hook = GCSHook('GCS_NY_Taxi')
        for year in year_list:
            for month in month_list:
                url_name = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_{year}-{month:02d}.parquet'
                print(url_name)
                resp = requests.get(url_name, timeout=100)
                print(resp.status_code)
                if resp.status_code == 200:
                    file_list.append(f"{taxi_type}_{year}-{month:02d}.parquet")
                    gcs_hook.upload('taxi_project_data', f'{taxi_type}_{year}-{month:02d}.parquet', data=resp.content, timeout=200)
        return file_list


    t1 = PythonOperator(
        task_id='get_NY_Taxi_data',
        python_callable=get_NY_Taxi_data,
        op_kwargs={
            'taxi_type': 'yellow_tripdata',
            'year_list': [2022],
            'month_list': [x for x in range(1, 13)]
        }
    )

    t1.doc_md = dedent(
        """\
    ### Get NY Taxi Data
    This task downloads the NY Taxi Data from the cloudfront URL
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    Using the PythonOperator to download the NY Taxi Data
    """  # otherwise, type it like this

    t2 = EmptyOperator( task_id='dummy_task')

    t1 >> t2
