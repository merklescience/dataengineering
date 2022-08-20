"""Google Cloud Storage related utilities"""

import logging
import os
import random
import sys
import time
from json import dumps as json_dumps

import google.auth
import google.auth.transport.requests as tr_requests
import httplib2
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from decouple import config
from google.cloud import storage
from google.resumable_media.requests import RawChunkedDownload

from dataengineering.airflow.bigquery.utils import ServerEnv

MEGABYTE = 1024 * 1024

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed downloads.
NUM_RETRIES = 5

# Number of bytes to send/receive in each request.
CHUNKSIZE = 10 * MEGABYTE

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = "application/octet-stream"


def upload_to_gcs(bucket, object_name, filename):
    cloud_storage_hook = GoogleCloudStorageHook(gcp_conn_id="google_cloud_default")

    print("Building upload request...")
    response = cloud_storage_hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=filename,
        num_max_attempts=5,
        mime_type=DEFAULT_MIMETYPE,
        chunk_size=CHUNKSIZE,
    )

    print("\nUpload complete!")
    print(json_dumps(response, indent=2))


def download_from_gcs(bucket, object, filename):
    ro_scope = "https://www.googleapis.com/auth/storage.full_control"
    credentials, _ = google.auth.default(scopes=(ro_scope,))
    transport = tr_requests.AuthorizedSession(credentials)

    object = object.replace("/", "%2F")
    filename = open(filename, "wb")

    chunk_size = 10 * 1024 * 1024
    url_template = (
        "https://www.googleapis.com/download/storage/v1/b/"
        "{bucket}/o/{object}?alt=media"
    )
    media_url = url_template.format(bucket=bucket, object=object)
    logging.info("Downloading file: " + str(media_url))
    download = RawChunkedDownload(media_url, chunk_size, filename)
    response = download.consume_next_chunk(transport)
    logging.info("Response: " + str(response))

    while not download.finished:
        logging.info(
            "Bytes downloaded : "
            + str(download.bytes_downloaded)
            + " total of "
            + str(download.total_bytes)
        )
        response = download.consume_next_chunk(transport)
        logging.info("Response: " + str(response))
    logging.info(
        "Bytes downloaded : "
        + str(download.bytes_downloaded)
        + " total of "
        + str(download.total_bytes)
    )
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
