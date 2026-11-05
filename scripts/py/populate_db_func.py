from pathlib import Path
from airflow.sdk import dag, task, get_current_context
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from psycopg2.extras import execute_values

#setting up minio hook & postgre hook and retrieving latest id json packet
def init(MB_JSON_ENDPOINT, HEADER):
    minio_hook = S3Hook('minio_s3') #hook to our minio env
    postgre_hook = PostgresHook('postgres_localhost') #hook to our postgre db
    #we call get_current_context function from airflow.sdk to retrieve the latest id json packet oushed in the xcoms. This should have been retrieved by the first dag when extracting the data.
    ti = get_current_context()['ti']
    latest_id = ti.xcom_pull(
        task_ids='retrieve_data_per_cat',
        dag_id='sync_with_music_brainz',
        include_prior_dates=True,
    )
    if latest_id is None: #if no value in xcoms we retrieve the id directly from a get request to music brainz web site.
        try:
            response = requests.get(f'{MB_JSON_ENDPOINT}/LATEST', headers=HEADER)
            latest_id = response.text.strip()
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
                    print(e.args[0])

    return minio_hook, postgre_hook, latest_id

#function that will load the appropriate sql script based on the data category we want to pish in our postgre db.
def load_sql(SQL_DIR, cat: str) -> str:
    return (SQL_DIR / f'{cat}.sql').read_text()