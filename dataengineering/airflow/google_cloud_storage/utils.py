"""Google Cloud Storage related utilities"""

from airflow.models import Variable
from decouple import config

from dataengineering.airflow.bigquery.utils import ServerEnv


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
