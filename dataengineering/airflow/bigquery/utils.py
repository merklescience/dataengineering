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
    query = "".join(open(query_path, "r").readlines())
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


def join_bigquery_queries_in_folder(queries_folder, nested=False, environment=None):
    """
    Finds all sql files in a folder and returns a string after adding up all queries
    This works on BigQuery queries
    """
    dags_folder = os.environ.get("DAGS_FOLDER", "/usr/local/airflow/dags")
    full_queries_folder = os.path.join(dags_folder, queries_folder)
    query_filenames = []
    if not nested:
        query_filenames = filter(lambda x: "sql" in x, os.listdir(full_queries_folder))

    else:
        sub_directories = [
            sub_directory
            for sub_directory in os.listdir(full_queries_folder)
            if os.path.isdir(os.path.join(full_queries_folder, sub_directory))
        ]
        for sub_directory in sub_directories:
            sub_directory_path = os.path.join(full_queries_folder, sub_directory)
            query_filenames.extend(
                list(filter(lambda x: "sql" in x, os.listdir(sub_directory_path)))
            )

    queries = []
    for query_filename in query_filenames:
        query_path = os.path.join(full_queries_folder, query_filename)
        query = "".join(open(query_path, "r").readlines())
        queries.append(query)

    template_queries = "\n \n UNION ALL \n \n".join(queries)

    if environment:
        return apply_env_variables_on_blob(template_queries, environment)
    return template_queries
