"""Google Cloud Storage related utilities"""

import os
import logging
import random
import sys
import time
from json import dumps as json_dumps

import google.auth
import google.auth.transport.requests as tr_requests
import httplib2
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from google.cloud import storage
from google.resumable_media.requests import RawChunkedDownload

from airflow.models import Variable
from decouple import config

from dataengineering.airflow.bigquery.utils import ServerEnv

MEGABYTE = 1024 * 1024

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed downloads.
NUM_RETRIES = 5

# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * MEGABYTE

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'


def handle_progressless_iter(error, progressless_iters):
    if progressless_iters > NUM_RETRIES:
        print('Failed to make progress for too many consecutive iterations.')
        raise error

    sleeptime = random.random() * (2 ** progressless_iters)
    print(('Caught exception (%s). Sleeping for %s seconds before retry #%d.'
           % (str(error), sleeptime, progressless_iters)))
    time.sleep(sleeptime)

def print_with_carriage_return(s):
    sys.stdout.write('\r' + s)
    sys.stdout.flush()

def upload_to_gcs(bucket, object_name, filename):
    cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")

    print('Building upload request...')
    response = cloud_storage_hook.upload(bucket=bucket,
                              object=object_name,
                              filename=filename,
                              multipart=True,
                              num_retries=5
    )


    print('\nUpload complete!')
    print(json_dumps(response, indent=2))

def upload_small_file_to_gcs(bucket, object_name, filename):
    print(bucket)
    print(object_name)
    print(filename)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(str(bucket))
    blob = bucket.blob(str(object_name))

    os.stat(str(filename))
    blob.upload_from_filename(str(filename))
    print(len(blob.download_as_string().decode()))

    print('File {} uploaded to {}.'.format(
        filename,
        object_name))

def upload_empty_file_to_gcs(bucket, object_name, filename):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(str(bucket))
    blob = bucket.blob(str(object_name))

    blob.upload_from_filename(str(filename))

    print('File {} uploaded to {}.'.format(
        filename,
        object_name))


def download_from_gcs(bucket, object, filename):
    ro_scope = u'https://www.googleapis.com/auth/storage.full_control'
    credentials, _ = google.auth.default(scopes=(ro_scope,))
    transport = tr_requests.AuthorizedSession(credentials)

    object = object.replace('/', '%2F')
    filename = open(filename, "wb")

    chunk_size = 10 * 1024 * 1024
    url_template = (u'https://www.googleapis.com/download/storage/v1/b/'u'{bucket}/o/{object}?alt=media')
    media_url = url_template.format(bucket=bucket, object=object)
    logging.info("Downloading file: " + str(media_url))
    download = RawChunkedDownload(media_url, chunk_size, filename)
    response = download.consume_next_chunk(transport)
    logging.info("Response: " + str(response))
    while not download.finished:
        logging.info("Bytes downloaded : " + str(download.bytes_downloaded) + " total of " + str(download.total_bytes))
        response = download.consume_next_chunk(transport)
        logging.info("Response: " + str(response))
    logging.info("Bytes downloaded : " + str(download.bytes_downloaded) + " total of " + str(download.total_bytes))
    logging.info("Response: " + str(response))

    if not download.bytes_downloaded == download.total_bytes:
        raise ValueError("File downloaded not of same size")

def build_gcs_bucket(bucket_id):
    """This function must be used every time a table is to be saved on Storage
    The intented use of this function is to route table creation statements to
    different datasets based on environment.
    """
    project_id = Variable.get("GCS_DESTINATION_PROJECT")
    dataset_id = Variable.get("GCS_DESTINATION_DATASET")

    server_env = config("SERVER_ENV", ServerEnv.LOCAL, cast=str)
    if server_env != ServerEnv.PRODUCTION:
        return f"{project_id}-{dataset_id}"
    return bucket_id
