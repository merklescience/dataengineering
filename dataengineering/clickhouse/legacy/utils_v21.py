import logging
import subprocess
import jinja2
import os

from airflow.models import Variable
from dataengineering.clickhouse.legacy.utils import format_sql_query, read_file


def read_and_render_file(file_path,
                         sql_variables: dict = None):
    """
    reads a file and renders the varaibles passed in sql_variables
    :rtype: rendered string
    """
    with open(file_path, 'r') as f:
        return jinja2.Template(f.read()).render(sql_variables)


def render_sql(sql,
               sql_variables: dict = None):
    """
    renders the varaibles passed in sql_variables
    :rtype: rendered string
    """
    return jinja2.Template(sql).render(sql_variables)


def execute_query(conn_details: dict, sql: str, sql_variables: dict = None, **kwargs):
    """
    Executes a sequence of semi-colon separated Clickhouse queries
    """
    logging.info('Executing SQL ' + sql)
    individual_queries = sql.split(';')
    response_file = kwargs["task_instance"].dag_id + kwargs["task_instance"].task_id + ".txt"
    for each_query in individual_queries:
        if each_query == '':
            continue
        logging.info('Executing SQL ' + each_query)
        echo_query = subprocess.Popen(["echo", each_query], stdout=subprocess.PIPE)
        echo_query.wait()
        curl_request = ["curl",
                        f"{conn_details['host']}:{conn_details['port']}/{conn_details['database']}",
                        "--data-binary",
                        "@-",
                        "-o",
                        response_file,
                        "-H",
                        f"X-ClickHouse-User: {conn_details['user']}",
                        "-H",
                        f"X-ClickHouse-Key: {conn_details['password']}",
                        ]
        logging.info(curl_request)
        result = subprocess.run(curl_request, stdin=echo_query.stdout)

        echo_query.terminate()
        if result.returncode != 0:
            logging.error(result)
            with open(response_file, 'r') as f:
                logging.error(f.read())
            raise Exception("Non 0 code returned from curl request " + " ".join(curl_request))
        _check_file_for_clickhouse_error(response_file)
        return response_file


def _check_file_for_clickhouse_error(filename):
    # This only checks if first/last 10 lines of file contains e.displayText() which is a way for checking
    # clickhouse errors
    def check_func(check_type):
        check_type_file = subprocess.Popen([check_type, filename], stdout=subprocess.PIPE)
        check_type_file.wait()
        grep_content = subprocess.run(["grep", "e.displayText()"], stdin=check_type_file.stdout)
        check_type_file.terminate()
        if grep_content.returncode == 0:
            raise Exception("Clickhouse query unsuccessfull :",
                            subprocess.run([check_type, filename], stdout=subprocess.PIPE).stdout)

    check_func("head")
    check_func("tail")


def _build_export_clickhouse_http_command_v21(
        parent_dir,
        resource,
        filename,
        environment,
):
    clickhouse_uri = Variable.get('CLICKHOUSE_URI_V21', '')

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
    query_path = os.path.join(dags_folder, parent_dir, f'{resource}.sql')
    query = format_sql_query(read_file(query_path), environment=environment)

    return f'echo "{query}" | curl -sS "{clickhouse_uri}" --data-binary @- > {filename} 2>&1 && if cat {filename} | grep -E \"Failed|Exception|Missing\"; then exit -1; fi'
