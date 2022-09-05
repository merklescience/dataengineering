"""Generic Clickhouse Connector"""
import subprocess
import tempfile
import warnings
from typing import Callable, Optional, TypeVar, Union

import requests

from dataengineering import logger
from dataengineering.clickhouse import exceptions
from dataengineering.clickhouse.v1.bash_hook import ClickHouseBashHook

R = TypeVar("R")
"""Define a generic return type that will be returned from the `ClickhouseConnector.run` method."""


class ClickhouseConnector:
    """Generic Clickhouse Connector

    This connector allows users to run simple queries on a clickhouse server,
    while allowing them to abstract away the deserialization logic with the
    result. The raw response from the HTTP request is sent to the user,
    either as the result from the `Request.json()`, which is just the result of
    `json.loads`, or
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        use_https: bool = False,
        database: Optional[str] = None,
        port: Optional[int] = None,
        **_,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_https = use_https
        self.database = database or "default"

    def read(
        self,
        query: str,
        deserialize_with: Optional[Callable[[Union[dict, list]], R]] = None,
    ) -> Union[dict, list, R]:
        """Runs a READ query string and returns the result as either the result
        of the `json.loads` function, or it runs a user-provided callable that
        returns a type of its own.

        ```python
            def func(x=dict|list) -> str:
                return "hello"

            c = ClickhouseConnector()
            x = c.run("select * from table limit 1;", deserialize_with=func)
        ```

        """
        result = self.execute(query)
        result = result.json()
        if deserialize_with is None:
            return result
        else:
            return deserialize_with(result)

    def execute(self, query: str, mode="read") -> requests.Response:
        """Runs the query using the Clickhouse HTTP API"""
        # TODO: Validate the query to check if it is valid Clickhouse-compliant
        # SQL, or if it incorporates our best practices.
        url = self.__get_url()
        if mode == "read":
            params = dict(query=query, user=self.username, password=self.password)
            response = requests.get(url, params=params)
        else:
            params = dict(user=self.username, password=self.password)
            response = requests.post(url, params=params, data=query)

        response.raise_for_status()
        logger.debug(
            "Raw response content for mode=`{}`:=`{}`. ResponseCode: {}".format(
                mode, response.content, response.status_code
            )
        )
        return response

    def __get_url(self) -> str:
        """Builds the URL using the parameters passed in during __init__"""
        if self.use_https:
            prefix = "https://"
        else:
            prefix = "http://"
        if self.port is not None:
            url = f"{prefix}{self.host}:{self.port}/{self.database}"
        else:
            url = f"{prefix}{self.host}/{self.database}"
        logger.debug(f"Clickhouse URL={url}")
        return url

    def write_from_file(
        self,
        destination_database: str,
        destination_table: str,
        file_path: str,
        file_format: str = "csv",
    ):
        """Writes to a table"""
        from urllib.request import pathname2url

        data_format_dict = {
            "parquet": "Parquet",
            "json": "JSONEachRow",
            "csv": "CSVWithNames",
        }
        try:
            data_format = data_format_dict[file_format.lower()]
        except KeyError as e:
            error_message = f"`{file_format}` is not a valid file format. Choose from either `json`, `csv` or `parquet`"
            raise exceptions.ClickhouseFileFormatError(error_message) from e
        warnings.warn(
            "The cURL method of sending a file to clickhouse will be deprecated in later versions of this library.",
            DeprecationWarning,
        )
        sql = f"INSERT INTO {destination_database}.{destination_table} FORMAT {data_format}"

        with tempfile.NamedTemporaryFile() as tmp:
            url_escaped_query = pathname2url(sql)
            # TODO: Adapt the following cURL based logic into the requests
            # alternative.
            response_file = tmp.name
            curl_request = [
                "curl",
                "-w",
                # NOTE: `-w %{http_code}` will output the HTTP status code after a request is made.
                "%{http_code}",
                f"{self.__get_url()}?" f"query={url_escaped_query}",
                "--data-binary",
                f"@{file_path}",
                "-o",
                response_file,
                "-H",
                f"X-ClickHouse-User: {self.username}",
                "-H",
                f"X-ClickHouse-Key: {self.password}",
            ]
            result = subprocess.run(curl_request, stdout=subprocess.PIPE, timeout=3600)
            # NOTE: I'm using the previously written functions in the
            # merklescience/airflow-dags/dags/resources/clickhouse/ch_bash_hook.py file
            # so as to not replicate the logic.
            # However, I've replicated the cURL command here because I'd like to
            # replace it with requests eventually.
            try:
                ClickHouseBashHook._check_stdout(result, curl_request)
            except Exception as e:
                error_message = "Error returned from {}".format(" ".join(curl_request))
                logger.error(f"Result STDOUT: {result.stdout}")
                logger.error(error_message)
                raise exceptions.ClickhouseStdOutValidationError(error_message) from e
            try:
                ClickHouseBashHook._check_file_for_clickhouse_error(response_file)
            except Exception as e:
                error_message = "Error returned from {}".format(" ".join(curl_request))
                logger.error(result)
                logger.error(error_message)
                raise exceptions.ClickhouseOutputFileValidationError(
                    error_message
                ) from e
