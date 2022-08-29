import boto3
import datetime
import logging
import os
import requests
import time
import shutil
from datetime import timedelta
from google.cloud import storage


# 3600 seconds
GSQL_TIMEOUT = f'{90 * 90 * 1000}'
GSQL_THREAD_LIMIT = '10'
MAX_RETRY_COUNT = 5

logger = logging.getLogger(__name__)


def _form_tigergraph_request(tigergraph_ip,
                             chain,
                             loading_job) -> str:
    return f"http://{tigergraph_ip}:9000/ddl/{chain}?&" \
           f"tag={loading_job}&filename=f1&sep=,&eol=\n"


def _post_local_file_to_tg(filename,
                           tigergraph_ip,
                           chain,
                           loading_job,
                           stats,
                           size_hint = 2 << 22):
    file_handler = open(filename, 'r')
    data = file_handler.readlines(size_hint)
    while len(data) > 0:
        tigergraph_post_request(
            tigergraph_request=_form_tigergraph_request(tigergraph_ip=tigergraph_ip,
                                                        chain=chain,
                                                        loading_job=loading_job
                                                        ),
            data=''.join(data),
            statistic=stats
        )
        data = file_handler.readlines(size_hint)


def tigergraph_get_request(connection_string):
    """
        Make Tigergraph HTTP GET request
    :param connection_string:
    """
    logging.info(connection_string)
    response = requests.get(connection_string,
                            headers={"GSQL-TIMEOUT": GSQL_TIMEOUT,
                                     "GSQL-THREAD-LIMIT": GSQL_THREAD_LIMIT
                                     }
                            )

    logging.info(response.content)
    logging.info(response)
    if response.status_code == 200:
        logging.info("Successful")
    else:
        raise AirflowException("Error in processing")
    return response


def tigergraph_post_request(tigergraph_request,
                            data,
                            statistic):
    """
        Make Tigergraph HTTP POST request.
        TODO: naive Implementation of retry and backoff
    :param tigergraph_request:
    :param data:
    :param statistic:
    """
    logging.info(tigergraph_request)
    max_retries = MAX_RETRY_COUNT
    while max_retries > 0 :
        max_retries -= 1
        data = data.encode('utf-8')
        response = requests.post(tigergraph_request,
                                 data=data,
                                 headers={"GSQL-TIMEOUT": GSQL_TIMEOUT}
                                 )

        logging.info(f"Retry Count = {max_retries}/5")
        logging.info(response)
        logging.info(response.content)
        if response.status_code != 200 or response.json()['error']:
            time.sleep(5 * 60)
            continue
        else:
            stats = response.json()['results'][0]['statistics']
            if stats['validLine'] == 0 or \
                    stats['rejectLine'] > 0 or \
                    stats['failedConditionLine'] > 0 or \
                    stats['invalidJson'] > 0 or \
                    stats['oversizeToken'] > 0 or \
                    stats['notEnoughToken'] > 0 or \
                    (stats[statistic][0]['validObject'] - stats['validLine']) > 1 or \
                    stats[statistic][0]['invalidAttribute'] > 1:
                raise AirflowException('Error in request to Tigergraph')
        break
    if max_retries == 0:
        raise AirflowException('Error in request to Tigergraph')


def run_s3_loading_job_tigergraph(**kwargs):
    """
    Run file loading of S3 files to Tigergraph via HTTP POST
    :param kwargs:
    """
    s3_client = boto3.client(
        's3',
        region_name="us-east-2",
        aws_access_key_id=kwargs['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=kwargs['AWS_SECRET_ACCESS_KEY']
    )

    s3_client.download_file(Filename=kwargs['FILENAME'],
                            Bucket=kwargs['AWS_BUCKET'],
                            Key=kwargs['AWS_KEY'])

    _post_local_file_to_tg(filename=kwargs['FILENAME'],
                           tigergraph_ip=kwargs['TIGERGRAPH_HOST'],
                           chain=kwargs['CHAIN'],
                           loading_job=kwargs['JOB'],
                           stats=kwargs['STATS']
                           )


def run_multifile_gcs_loading_job_tigergraph(**kwargs):
    """
    Run multi file loading of GCS files to Tigergraph via HTTP POST
    :param kwargs: 
    """
    client = storage.Client()
    logging.info(kwargs)
    local_folder = f"{kwargs['CHAIN']}_{kwargs['RESOURCE']}_{kwargs['ds']}"
    prefix = f"{kwargs['CHAIN']}{kwargs['DEPLOY_LABEL']}/{kwargs['RESOURCE']}/{kwargs['ds']}/"

    if os.path.exists(local_folder):
        shutil.rmtree(local_folder)
    os.makedirs(local_folder)

    print(prefix)
    print(local_folder)

    blobs = list(client.list_blobs(kwargs['GCS_BUCKET'], prefix=prefix))

    if len(blobs) == 0:
        raise AirflowException('No files generated')

    for blob in blobs:
        print(blob)
        filename = blob.name.split('/')[blob.name.count('/')]
        local_full_filepath = os.path.join(local_folder, filename)
        blob.download_to_filename(local_full_filepath)

        _post_local_file_to_tg(filename=local_full_filepath,
                               tigergraph_ip=kwargs['TIGERGRAPH_HOST'],
                               chain=kwargs['CHAIN'],
                               loading_job=kwargs['JOB'],
                               stats=kwargs['STATS']
                               )

        os.remove(local_full_filepath)
