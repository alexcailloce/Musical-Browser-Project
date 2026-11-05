import sys
sys.path.append('/opt/airflow/scripts/py') #path in the container
import populate_db_func

from pathlib import Path
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from psycopg2.extras import execute_values

load_dotenv()

def first(lst):
    return lst[0] if lst else None

#dag default ags
default_args = {
    'owner': 'music_discovery_project',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

#header sent when launching get request to music brainz
HEADER = {
    'User-Agent': 'MusicDiscovery/0.1 (musicdiscovery613@gmail.com)'
}

MB_JSON_ENDPOINT = 'https://data.metabrainz.org/pub/musicbrainz/data/json-dumps'

#listing data categories we wanna retrieve -> values must match exaclty with the names given by music brainz
CATEGORIES = ['area', 'instrument', 'label', 'artist', 'release-group', 'release']

#map categories with tuples we wanna retrieve in json files
MAPPERS = {
    'area': lambda r: (r['id'], r.get('name'), r.get('type'), first(r.get('iso-3166-1-codes') or [])),
    'artist': lambda r: (r['id'], r.get('name'), r.get('type'), (r.get('area') or {}).get('id')),
    'instrument': lambda r: (r['id'], r.get('name'), r.get('type')),
    'label': lambda r: (r['id'], r.get('name'), r.get('label-code'), (r.get('area') or {}).get('id')),
    'release-group': lambda r: (r['id'], r.get('title'), r.get('primary-type'), [ac['artist']['id'] for ac in r.get('artist-credit') or []]),
    'release': lambda r: (r['id'], r.get('title'), (r.get('release-group') or {}).get('id'), r.get('date'), r.get('barcode')),
}

SQL_DIR = Path('/opt/airflow/scripts/sql') #set up path for sql scripts

@dag(dag_id='populate_pg_db',default_args=default_args, start_date=datetime(2026, 10, 1), schedule='@weekly', catchup=False)
def populate_postgre_db():

    #basically a way to create a dag pipeline. instead of trigerring one dag then the other, we can set up a dependency between the two dags. This dag will run only if the last task of the 1st dag is successful.
    wait_for_data = ExternalTaskSensor(
        task_id='wait_for_data',
        external_dag_id='sync_with_music_brainz',      
        external_task_id='extract_data_per_cat', #will run after sucess of this task in the other dag. if none, it will run after completion of the first dag.
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],   
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode='reschedule', #frees the worker slot between checks
    )

    @task()
    def retrieve_dat_from_mino():
        minio_hook, postgre_hook, latest_id = populate_db_func.init(MB_JSON_ENDPOINT, HEADER)
        conn = postgre_hook.get_conn()
        BATCH_SIZE = 5000
        try:
            with conn.cursor() as cur:
                for cat in CATEGORIES:
                    mapper = MAPPERS[cat]
                    sql_script = populate_db_func.load_sql(SQL_DIR, cat)
                    key = f'{cat}-{latest_id}/mbdump/{cat}.json' #where the actual data is stored in minio. we retrieve the key name based on the category and the latest id we retrieved from the first dag.
                    body = minio_hook.get_key(key=key, bucket_name='unprocessed-data').get()['Body'] #return a boto3 s3 object. Will allow us to stream the file while retrieving it to avoid memory saturation.
                    batch = []
                    for line in body.iter_lines():
                        if not line:
                            continue
                        batch.append(mapper(json.loads(line)))
                        if len(batch) >= BATCH_SIZE: #we keep appending to the batch until we reach the batch size limit. We then execute the associated sql script.
                            execute_values(cur, sql_script, batch)
                            batch.clear()
                    if batch:
                        execute_values(cur, sql_script, batch)
                    conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
             conn.close()

    wait_for_data >> retrieve_dat_from_mino() #set up task dependency. The second task will run only after the first one is completed successfully. so basically we call this dag after the success of the first dag (i.e after the data has been retrieved and stored in minio).

populate_postgre_db()
        
