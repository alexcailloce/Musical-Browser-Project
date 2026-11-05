import sys
sys.path.append('/opt/airflow/scripts/py') #path in the container
import get_mb_dat_func
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

load_dotenv()

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

#endpoint we wanna reach to retrieve db data
MB_JSON_ENDPOINT = 'https://data.metabrainz.org/pub/musicbrainz/data/json-dumps'

#listing data categories we wanna retrieve -> values must match exaclty with the names given by music brainz
CATEGORIES = {
    'cat1': 'artist',
    'cat2': 'instrument',
    'cat3': 'area',
    'cat4': 'release',
    'cat5': 'label',
    'cat6': 'release-group'
}

@dag(dag_id='sync_with_music_brainz',default_args=default_args,start_date=datetime(2026, 10, 1), schedule='@weekly', catchup=False)
def mb_dat_push_to_minio():
    #first task of our dag -->  we retrieve the latest data dump for music brainz db
    @task()
    def retrieve_data_per_cat():
        latest_id = get_mb_dat_func.get_latest_mb_packet(endpoint=MB_JSON_ENDPOINT, header=HEADER) #get latest music brainz packet id
        for cat in CATEGORIES.values():
            get_mb_dat_func.save_tar_packet(cat, latest_id=latest_id, endpoint=MB_JSON_ENDPOINT, header=HEADER)
        return latest_id #pushed to xcoms

    #second task of our dag --> extract packets/tar files stored in our minio 'tar-dump' bucket and then store them in the unprocessed-data bucket
    @task()
    def extract_data_per_cat(latest_id):
        for cat in CATEGORIES.values():
            get_mb_dat_func.extract_tar_packet(cat, latest_id=latest_id)

    
    latest_id = retrieve_data_per_cat()
    extract_data_per_cat(latest_id)

mb_dat_push_to_minio()
