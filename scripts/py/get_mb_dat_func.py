import requests
import os
import tempfile
import tarfile
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#function to retrieve number of latest music brainz packet
def get_latest_mb_packet(endpoint, header):
    try:
        response = requests.get(f'{endpoint}/LATEST', headers=header)
        response.raise_for_status()
        return response.text.strip()
    except requests.exceptions.HTTPError as e:
        print(e.args[0])

#function called when extracting tar file retrieved from music brainz -> just a check to avoid opening potential malicious files
def is_safe_member(member):
    #Reject absolute paths and paths with '..'
    return (
        not member.name.startswith("/") and  #No absolute paths
        ".." not in member.name and          #No parent directory traversal
        member.isfile()                      #Only process regular files (skip symlinks/dirs)
    )

def save_tar_packet(cat, latest_id, endpoint, header): #cat is the data category we're pulling from music brainz and uploading in our tar dump bucket
        minio_hook = S3Hook('minio_s3') #hook to our minio env -> minio_s3 set up in docker-compose.yaml in airflow_init service
        try:
            with requests.get(
                f'{endpoint}/{latest_id}/{cat}.tar.xz', headers=header, stream=True
            ) as response:
                response.raise_for_status()

                with tempfile.NamedTemporaryFile(suffix='.tar.xz') as tmp: #write data in temp file. the tmp file will be deleted at the end of the with section.
                    for chunk in response.iter_content(chunk_size=1024 * 1024): #write data chunk by chunk to avoid memory saturation
                        tmp.write(chunk) #write in tmp file within the python file memory
                    tmp.flush() #flush file in the os for it to be accessible by load_file function

                    minio_hook.load_file(
                        filename=tmp.name,
                        key=f'{cat}/{cat}-{latest_id}.tar.xz',
                        bucket_name='tar-dump',
                        replace=True,
                    )

                print(f'Successfully saved {cat}') #just a print to keep track within the logs
        
        except requests.exceptions.HTTPError as e:
            print(e.args[0])

def extract_tar_packet(cat, latest_id):
        minio_hook = S3Hook('minio_s3')
        path = minio_hook.download_file(
            key=f'{cat}/{cat}-{latest_id}.tar.xz',
            bucket_name='tar-dump'
        )

        try:
            #tar file retrieved from music brainz can be pretty big (~2 to 10gb) so we extract its data as a stream to avoid memory/disk stockage issue.
            with tarfile.open(path, 'r:xz') as tar:
                for member in tar: #tar files are composed of 'members' (files, directories...) -> same as tar.getmembers(). Here we just go through all the members/files of the tar archive.
                    if is_safe_member(member=member): #sanity check, see function above
                        with tar.extractfile(member) as stream:  #stream for the member -> we only extract the member
                            if stream:  #Another sanity check, ensure stream is valid (i.e stream not null after calling tar.extractfile)
                                minio_hook.load_file_obj(
                                    file_obj=stream,
                                    key=f'{cat}-{latest_id}/{member.name}.json',
                                    bucket_name='unprocessed-data',
                                    replace=True
                                )
        except requests.exceptions.HTTPError as e:
            print(e.args[0])
        finally:
            os.remove(path)