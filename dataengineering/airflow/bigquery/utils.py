import logging
import os

import jinja2
from decouple import config
from google.cloud import bigquery

from dataengineering.constants import ServerEnv


def build_bigquery_destination(dataset_id, table_id):
    """
    This function must be used every time a table is to be saved

    The intented use of this function is to route table creation
    statements to different datasets based on environment
    """
    # TODO: this has a hard dependency on airflow, and might be better off written as something that's a pure function,
    # taking these as inputs. If necessary, wrap this with another function that injects airflow variables.
    # NOTE: This pattern occurs quite a bit, and can be automated.
    from airflow.models import Variable

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


def run_bigquery_sqls(
        sql: str, project_id: str = None, job_id_prefix: str = None, *args, **kwargs
) -> None:
    """
    This function is for running bigquery queries which don't return results like DDLs,DMLs,data exports
    :param sql: query, can also pass multiple queries separated via ';'
    :type sql: str
    :param project_id: GCP project id
    :type project_id: str
    :param job_id_prefix: job prefix, this is helpful in identifying specific bq jobs
    :type job_id_prefix: str
    :param args:
    :type args:
    :param kwargs:
    :type kwargs:
    :return:
    :rtype:
    """
    individual_queries = sql.split(";")
    client = bigquery.Client(project=project_id)
    for each_query in individual_queries:
        if each_query == "":
            continue
        logging.info("Running BQ query " + each_query)
        query_job = client.query(each_query, job_id_prefix=job_id_prefix)
        results = query_job.result()


def run_flush_sqls(
        partition_filter: str, fully_qualified_table: str, project_id: str = None, job_id_prefix: str = None, *args,
        **kwargs
) -> None:
    """

    """
    client = bigquery.Client(project=project_id)
    if client.get_table(fully_qualified_table):
        query_job = client.query(f"DELETE FROM {fully_qualified_table} WHERE {partition_filter}",
                                 job_id_prefix=job_id_prefix)
        results = query_job.result()
    else:
        return True
