"""Google Cloud Storage related utilities"""

from json import dumps as json_dumps

import httplib2
from decouple import config

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
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

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
    """
    This function was refactored in commit
    https://github.com/merklescience/dataengineering/pull/29/commits/6e5445274e3ad9baac1d579e66254fbc05e71e3b

    If you run into issues with this function while downloading large files,
    we need a more closer look at this function
    """
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

    cloud_storage_hook = GoogleCloudStorageHook(gcp_conn_id="google_cloud_default")

    print("Building upload request...")
    response = cloud_storage_hook.download(
        bucket_name=bucket,
        object_name=object,
        filename=filename,
        num_max_attempts=5,
        chunk_size=CHUNKSIZE,
    )

    print("\nDownload complete!")
    print(json_dumps(response, indent=2))


def build_gcs_bucket(bucket_id):
    """This function must be used every time a table is to be saved on Storage
    The intented use of this function is to route table creation statements to
    different datasets based on environment.
    """
    from airflow.models import Variable

    project_id = Variable.get("GCS_DESTINATION_PROJECT")
    dataset_id = Variable.get("GCS_DESTINATION_DATASET")

    server_env = config("SERVER_ENV", ServerEnv.LOCAL, cast=str)
    if server_env != ServerEnv.PRODUCTION:
        return f"{project_id}-{dataset_id}"
    return bucket_id
