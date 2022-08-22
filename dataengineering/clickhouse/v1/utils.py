import ast
import os

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

CLICKHOUSE_URI = Variable.get('clickhouse_uri', '')
CH_USER = Variable.get('CH_USER', '')
CH_PASSWORD = Variable.get('CH_PASSWORD', '')

SETUP_COMMAND = \
    'set -o xtrace && ' + \
    'export LC_ALL=C.UTF-8 && ' \
    'export LANG=C.UTF-8 && ' \
    'export CLOUDSDK_PYTHON=/usr/bin/python2'


def read_file(file_path):
    with open(file_path, 'r') as f:
        return f.read()


def _build_clickhouse_http_command(parent_dir,
                                   resource,
                                   filename='-',
                                   clickhouse_uri=''):
    if clickhouse_uri == '':
        clickhouse_uri = Variable.get('clickhouse_uri', '')

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
    query_path = os.path.join(dags_folder, parent_dir, f'{resource}.sql')
    query = read_file(query_path)

    if filename == '-':
        return f'echo "{query}" | if curl "{clickhouse_uri}" --data-binary @{filename} 2>&1| grep -E \"Failed|Exception\"; then exit -1; fi'
    return f'[ -s {filename} ] || exit 0 &&  eval \'if curl {clickhouse_uri}/?query={query} --data-binary @{filename} 2>&1| grep -E \"Failed|Exception\"; then exit -1; fi\''


def _build_setup_table_operator(dag,
                                env,
                                table_type,
                                resource,
                                sql_folder,
                                task_id='',
                                params={},
                                clickhouse_uri=''):
    if task_id == '':
        task_id = f"setup_{resource}_table"

    command = ''
    if table_type == 'staging':
        command = _build_clickhouse_http_command(
            parent_dir=os.path.join(sql_folder, 'staging/drop'),
            resource=resource
        ) + ' && '

    command = command + _build_clickhouse_http_command(
        parent_dir=os.path.join(sql_folder, f'schemas/{table_type}'),
        resource=resource,
        clickhouse_uri=clickhouse_uri)

    operator = BashOperator(
        task_id=task_id,
        bash_command=command,
        execution_timeout=timedelta(hours=15),
        env=env,
        dag=dag,
        params=params
    )
    return operator


def _build_setup_streaming_table_operator(dag, env, table_type, resource, sql_folder, clickhouse_uri, task_id='',
                                          params={}):
    if task_id == '':
        task_id = f"setup_{resource}_table_{clickhouse_uri}"

    command = _build_clickhouse_http_command(parent_dir=os.path.join(sql_folder, f'schemas/{table_type}'),
                                             resource=resource, clickhouse_uri=clickhouse_uri)

    operator = BashOperator(task_id=task_id, bash_command=command, execution_timeout=timedelta(hours=15), env=env,
                            dag=dag, params=params)
    return operator


def flush_clickhouse_streaming_tables(dag, env, resource, sql_folder, task_id, params={},
                                      clickhouse_uri=''):
    command = _build_clickhouse_http_command(parent_dir=os.path.join(sql_folder, 'schemas/flush'),
                                             resource=resource, clickhouse_uri=clickhouse_uri)

    operator = BashOperator(task_id=task_id, bash_command=command, execution_timeout=timedelta(hours=15), env=env,
                            dag=dag, params=params)
    return operator


def _build_table_operator(dag,
                          env,
                          query,
                          sql_folder,
                          task_id='',
                          params={},
                          clickhouse_uri=''):
    operator = BashOperator(
        task_id=task_id,
        bash_command=_build_clickhouse_http_command(
            parent_dir=sql_folder,
            resource=query,
            clickhouse_uri=clickhouse_uri),
        execution_timeout=timedelta(hours=15),
        env=env,
        dag=dag,
        params=params
    )
    return operator


def _build_enrich_command(resource, chain):
    return ' && '.join([
        _build_clickhouse_http_command(parent_dir=link, resource=resource)
        for link in chain
    ])


def add_clickhouse_operator(dag,
                            env,
                            task_id,
                            bash_command,
                            dependencies=None):
    operator = BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        execution_timeout=timedelta(hours=15),
        env=env,
        dag=dag
    )
    if dependencies is not None and len(dependencies) > 0:
        for dependency in dependencies:
            if dependency is not None:
                dependency >> operator
    return operator


def _YYYY_MM(start_year):
    end_year = 2023
    partitions = []
    for year in range(start_year, end_year):
        for month in range(1, 13):
            if month < 10:
                month_str = '0' + str(month)
            else:
                month_str = str(month)
            partitions.append(str(year) + month_str)

    return partitions


def _build_clickhouse_optimize_http_command(resource, start_year):
    CLICKHOUSE_URIS = ast.literal_eval(Variable.get('CLICKHOUSE_URIS', ''))

    command_list = []
    for partition in _YYYY_MM(start_year):
        for each_clickhouse_instance_uri, shard_db in CLICKHOUSE_URIS:
            command_list.append(
                f'eval \' echo \'OPTIMIZE TABLE {shard_db}.{resource} PARTITION {partition} FINAL DEDUPLICATE\' ' \
                f'| curl http://{CH_USER}:{CH_PASSWORD}@{each_clickhouse_instance_uri}:8123?query= --data-binary @- \'')

    command = ' && '.join(command_list)
    return command


def _build_clickhouse_optimize_deduplicate_http_command(resource):
    clickhouse_uri = Variable.get('clickhouse_uri', '')
    return f'eval \' echo \'OPTIMIZE TABLE {resource} FINAL DEDUPLICATE\' ' \
           f'| curl {clickhouse_uri}:8123?query= --data-binary @- \''


def _build_load_clustering_updates_command(query_path):
    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
    query = read_file(os.path.join(dags_folder, query_path))
    CREATE_DIR = f'mkdir $EXECUTION_DATE'

    CP_COMMAND = f'gsutil cp -Z -r gs://$GCS_BUCKET/$CHAIN/$EXECUTION_DATE/* $EXECUTION_DATE/'

    LOAD_COMMAND = f'for filename in $EXECUTION_DATE/*; do ' \
                   f'eval \'if curl {CLICKHOUSE_URI}/?query={query} --data-binary @$filename 2>&1 | grep -E \"Failed|Exception\"; then exit -1; fi\'' \
                   f'; done'

    command = ' && '.join(
        [SETUP_COMMAND, CREATE_DIR, CP_COMMAND, LOAD_COMMAND]
    )
    return command


def format_sql_query(sql, environment):
    for key, value in environment.items():
        if isinstance(key, str) and isinstance(value, str):
            sql = sql.replace(f'[[ {key} ]]', value)
    return sql


def _build_export_clickhouse_http_command(
        parent_dir,
        resource,
        filename,
        environment,
):
    clickhouse_uri = Variable.get('clickhouse_uri', '')

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
    query_path = os.path.join(dags_folder, parent_dir, f'{resource}.sql')
    query = format_sql_query(read_file(query_path), environment=environment)

    return f'echo "{query}" | curl -sS "{clickhouse_uri}" --data-binary @- > {filename} 2>&1 && if cat {filename} | grep -E \"Failed|Exception|Missing\"; then exit -1; fi'
