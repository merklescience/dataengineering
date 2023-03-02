"""This module will be deprecated.

Clickhouse Bash Hook from the merklescience/airflow-dags repository
"""

import json
import os
import subprocess
import warnings
from importlib import util
from urllib.request import pathname2url

from dataengineering import logger
from decouple import config


class ClickHouseBashHook(object):
    """
    This is the connection hook for clickhouse operators. Function of this class use subprocess(bash) to
    interact with clickhouse via curl request.
    """

    def __init__(self, clickhouse_conn_id: str):
        """

        :param clickhouse_conn_id: clickhouse connection id
        :type clickhouse_conn_id: str
        """
        warnings.warn(
            "This method of communicating with Clickouse will be deprecated in later versions of these utilities.",
            DeprecationWarning,
        )
        self.clickhouse_conn_id = clickhouse_conn_id

    def _check_json_values(self, conn_details: str, key_type: str) -> (str, dict):
        """
        This function validates whether the credentials fetched via airflow variable or environment variable
        are valid json and contain host.
        :param conn_details: connections details as fetched from airflow/environment variable
        :type conn_details: str
        :param key_type: whether the variable was airflow or environment, this is for informative exception
        :type key_type: str
        :return: host & rest of connection details
        :rtype: (str, dict)
        """
        if conn_details is None:
            logger.error(f"{self.clickhouse_conn_id} not found in {key_type}.")
            raise Exception(f"{self.clickhouse_conn_id} not found in {key_type}.")
        conn_details = json.loads(conn_details)
        if conn_details.get("host") is None:
            logger.error(
                f"{self.clickhouse_conn_id} in {key_type} does not have host key"
            )
            raise Exception(
                f"{self.clickhouse_conn_id} in {key_type} does not have host key"
            )
        host = conn_details.pop("host")
        return host, conn_details

    def get_credentials(self) -> (str, dict):
        """
        This function will get the clickhouse connection details. It will first check in airflow variables and if not
        found then in environment variable. If not found anywhere then an exception will be raised. In future this
        will also be able to utilise airflow connections to find connection details, this is commented part of
        code as it requires airflow 2.0+ version.
        :return: host & rest of connection details
        :rtype: (str, dict)
        """
        airflow_present = util.find_spec("airflow") is not None
        if airflow_present:
            found = False
            # The below would work with airflow 2.0+ versions only
            # try:
            #     from airflow.models.connection import Connection
            #     from airflow.exceptions import AirflowNotFoundException
            #     conn = Connection.get_connection_from_secrets(self.clickhouse_conn_id)
            #     conn_details = {}
            #     host = conn.host
            #     if host is None:
            #         raise KeyError(f"{self.clickhouse_conn_id} in connections does not have host key")
            #     if conn.port:
            #         conn_details["port"] = int(conn.port)
            #     if conn.login:
            #         conn_details["user"] = conn.login
            #     if conn.password:
            #         conn_details["password"] = conn.password
            #     if self.database:
            #         conn_details["database"] = self.database
            #     elif conn.schema:
            #         conn_details["database"] = conn.schema
            #     return host, conn_details
            # except AirflowNotFoundException:
            #     found = False
            if not found:
                try:
                    from airflow.models.variable import Variable

                    conn = json.loads(config(self.clickhouse_conn_id))
                    return self._check_json_values(
                        conn_details=conn, key_type="Variables"
                    )
                except (KeyError, Exception) as e:
                    logger.error("Error with variable " + str(e))
                    found = False
            if not found:
                try:
                    conn = os.environ.get(self.clickhouse_conn_id)
                    return self._check_json_values(
                        conn_details=conn, key_type="Environment Variables"
                    )
                except KeyError:
                    raise KeyError(
                        f"{self.clickhouse_conn_id} not found in any of connections,"
                        f" variables and environment variable"
                    )

        if not airflow_present:
            # If airflow is not present we check only in environment variable, also we deliberately
            # don't want to catch error here
            conn = os.environ.get(self.clickhouse_conn_id)
            return self._check_json_values(
                conn_details=conn, key_type="Environment Variables"
            )

    @staticmethod
    def _check_file_for_clickhouse_error(filename) -> None:
        """
        This checks if first/last 10 lines of file contains e.displayText() which is a way for checking
        clickhouse errors. It also uses the python subprocess aka bash, use head & tail and piping the output to grep
        function.
        :param filename: file which needs to be checked
        :type filename: str
        :return: raises error if found
        :rtype: None
        """

        def check_func(check_type):
            check_type_file = subprocess.Popen(
                [check_type, filename], stdout=subprocess.PIPE
            )
            check_type_file.wait(timeout=10)
            grep_content = subprocess.run(
                ["grep", "e.displayText()"], stdin=check_type_file.stdout, timeout=60
            )
            check_type_file.terminate()
            if grep_content.returncode == 0:
                raise Exception(
                    "Clickhouse query unsuccessful :",
                    subprocess.run(
                        [check_type, filename], stdout=subprocess.PIPE, timeout=60
                    ).stdout.decode(),
                )

        check_func("head")
        logger.debug("Checked head ,checking tail")
        check_func("tail")
        logger.debug("Checked tail ,moving ahead")

    @staticmethod
    def _check_stdout(result: subprocess.CompletedProcess, curl_request) -> None:
        """
        This function checks the http response code and bash exit code returned from the bash curl request.
        :param result: result output of subprocess run
        :type result: CompletedProcess
        :param curl_request: the actual list of commands run via subprocess, this is just for making
        exception informative
        :type curl_request: list
        :return: raises exception if response code is anything other than 200 or bash exit code i
        :rtype: None
        """
        raw_response_code = result.stdout.decode()
        if not raw_response_code.isnumeric():
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            raise ValueError(f"Raw Response Code = {raw_response_code}")
        response_code = int(raw_response_code)
        logger.debug(f"Got response code: {response_code}")
        logger.debug
        if int(result.stdout.decode()) != 200:
            raise Exception(
                f"HTTP response code {response_code} received instead of 200"
            )
        logger.debug("Checked HTTP response ,checking bash return code")
        if result.returncode != 0:
            raise Exception(
                "Non 0 code returned from curl request " + " ".join(curl_request)
            )
        logger.debug("Checked bash return code ,checking file for errors")

    def get_results_file(
        self, sql: str, filename: str, file_format: str = "csv", **kwargs
    ) -> None:
        """
        This downloads sql query result from clickhouse to a local file. It uses python subprocess(bash) to fetch
        data via curl and downloads data directly to file.
        :param sql: sql query to fetch data
        :type sql: str
        :param filename: local file's name in which results will be downloaded to
        :type filename: str
        :param file_format: format of the data file, currently only csv and parquet supported
        :type file_format: str
        :param kwargs:
        :type kwargs:
        :return:
        :rtype:
        """
        logger.debug("Executing SQL " + sql)
        host, conn_details = self.get_credentials()
        if file_format == "csv":
            sql = sql + " FORMAT CSVWithNames"
        elif file_format == "parquet":
            sql = sql + " FORMAT Parquet"
        logger.debug("Echoing Query")
        echo_query = subprocess.Popen(["echo", sql], stdout=subprocess.PIPE)
        echo_query.wait(timeout=10)
        logger.debug("Echoed Query, running curl command")
        curl_request = [
            "curl",
            "-w",
            "%{http_code}",
            f"{host}:{conn_details['port']}/{conn_details['database']}"
            f"?user={conn_details['user']}&password={conn_details['password']}",
            "--data-binary",
            "@-",
            "-o",
            filename,
        ]
        # if compress_data:
        #     curl_request[1] = curl_request[1] + "&enable_http_compression=1"
        #     curl_request.extend(["-H", "'Accept-Encoding: gzip'"])
        result = subprocess.run(
            curl_request, stdin=echo_query.stdout, stdout=subprocess.PIPE, timeout=1800
        )
        logger.debug("Curl returned,terminating echo subprocess")
        echo_query.terminate()
        logger.debug("Echo process terminated ,checking http response code")
        self._check_stdout(result, curl_request)
        # if int(result.stdout.decode()) != 200:
        #     raise Exception(f"HTTP response code {int(result.stdout.decode())} received instead of 200")
        # logger.debug("Checked HTTP response ,checking bash return code")
        # if result.returncode != 0:
        #     raise Exception("Non 0 code returned from curl request " + " ".join(curl_request))
        # logger.debug("Checked bash return code ,checking file for errors")
        # self._check_file_for_clickhouse_error(filename)

    def run_insert_job(
        self,
        database: str,
        table: str,
        filename: str,
        file_format: str = "csv",
        **kwargs,
    ) -> None:
        """
        This will load a local file to clickhouse. It uses python subprocess(bash) to load data via curl request.
        :param database: target database
        :type database: str
        :param table: target table
        :type table: str
        :param filename: local filename to be loaded
        :type filename: str
        :param file_format: format of file, currently supports csv and parquet only
        :type file_format: str
        :param kwargs:
        :type kwargs:
        :return:
        :rtype:
        """
        host, conn_details = self.get_credentials()
        data_format = "CSVWithNames"
        if file_format == "parquet":
            data_format = "Parquet"
        elif file_format == "json":
            data_format = "JSONEachRow"
        sql = f"INSERT INTO {database}.{table} FORMAT {data_format}"
        response_file = (
            f"{kwargs['task_instance'].dag_id}_{kwargs['task_instance'].task_id}_"
            f"{str(kwargs['execution_date']).replace(' ', '').replace('-', '').replace(':', '')[:-5]}.txt"
        )
        if os.path.isfile(response_file):
            os.remove(response_file)
        logger.debug(
            f"Inserting into database: {database},table: {table} & data format: {file_format}"
        )
        curl_request = [
            "curl",
            "-w",
            "%{http_code}",
            f"{host}:{conn_details['port']}/{conn_details['database']}?"
            f"query={pathname2url(sql)}",
            "--data-binary",
            f"@{filename}",
            "-o",
            response_file,
            "-H",
            f"X-ClickHouse-User: {conn_details['user']}",
            "-H",
            f"X-ClickHouse-Key: {conn_details['password']}",
        ]
        result = subprocess.run(curl_request, stdout=subprocess.PIPE, timeout=3600)
        self._check_stdout(result, curl_request)
        self._check_file_for_clickhouse_error(response_file)
        os.remove(response_file)

    def execute_query(self, sql: str, **kwargs):
        """
        This is for executing sql queries which don't end up with results like DDLs and DMLs. It uses python
        subprocess(bash) for executing SQLs via curl request.
        :param sql: sql query to execute
        :type sql: str
        :param kwargs:
        :type kwargs:
        :return:
        :rtype:
        """
        host, conn_details = self.get_credentials()
        individual_queries = sql.split(";")
        response_file = (
            f"{kwargs['task_instance'].dag_id}_{kwargs['task_instance'].task_id}_"
            f"{str(kwargs['execution_date']).replace(' ', '').replace('-', '').replace(':', '')[:-5]}.txt"
        )
        if os.path.isfile(response_file):
            os.remove(response_file)
        for each_query in individual_queries:
            if each_query == "":
                continue
            logger.debug("Executing SQL " + each_query)
            echo_query = subprocess.Popen(["echo", each_query], stdout=subprocess.PIPE)
            echo_query.wait(timeout=10)
            curl_request = [
                "curl",
                "-w",
                "%{http_code}",
                f"{host}:{conn_details['port']}/{conn_details['database']}",
                "--data-binary",
                "@-",
                "-o",
                response_file,
                "-H",
                f"X-ClickHouse-User: {conn_details['user']}",
                "-H",
                f"X-ClickHouse-Key: {conn_details['password']}",
            ]
            result = subprocess.run(
                curl_request,
                stdin=echo_query.stdout,
                stdout=subprocess.PIPE,
                timeout=1800,
            )
            echo_query.terminate()
            self._check_stdout(result, curl_request)
            self._check_file_for_clickhouse_error(response_file)
        os.remove(response_file)
