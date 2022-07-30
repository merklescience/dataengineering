import os

import jinja2
from airflow.models import Variable
from decouple import config

from dataengineering.constants import ServerEnv


def build_bigquery_destination(dataset_id, table_id):
    """
    This function must be used every time a table is to be saved

    The intented use of this function is to route table creation
    statements to different datasets based on environment
    """
    project_id = Variable.get("BIGQUERY_DESTINATION_PROJECT")

    server_env = config("SERVER_ENV", ServerEnv.LOCAL, cast=str)
    if server_env != ServerEnv.PRODUCTION:
        dataset_id = Variable.get("BIGQUERY_DESTINATION_DATASET")

    table_name = f"{project_id}.{dataset_id}.{table_id}"
    return table_name


def get_query(query_path):
    """
    Returns the contents of file at a filepath
    """
    with open(query_path, "r") as f:
        query = f.read()
    return query


def apply_env_variables_on_blob(blob, environment):
    """
    Replaces all strings of style [[ `key` ]] with `value`
    To be used for templating SQL queries
    """
    # use the Jinja-native templating and use custom start and end strings
    # instead
    template = jinja2.Template(
        blob, variable_start_string="[[", variable_end_string="]]"
    )
    return template.render(**environment)


def join_bigquery_queries_in_folder(queries_folder, environment=None):
    """
    Finds all sql files in a folder and returns a string after adding up all queries
    This works on BigQuery queries
    """
    dags_folder = os.environ.get("DAGS_FOLDER", "/usr/local/airflow/dags")
    full_queries_folder = os.path.join(dags_folder, queries_folder)
    assert os.path.isdir(full_queries_folder), f"{full_queries_folder} doesn't exist"
    query_filenames = []
    for root, dirs, files in os.walk(full_queries_folder):
        for file in files:
            # append the file name to the list
            file_path = os.path.join(root, file)
            if os.path.splitext(file_path)[-1] == ".sql":
                query_filenames.append(os.path.join(root, file))

    queries = []
    for query_filename in query_filenames:
        query_path = os.path.join(full_queries_folder, query_filename)
        with open(query_path, "r") as f:
            query = f.read()
        queries.append(query)

    template_queries = "\n \n UNION ALL \n \n".join(queries)

    if environment:
        return apply_env_variables_on_blob(template_queries, environment)
    return template_queries
