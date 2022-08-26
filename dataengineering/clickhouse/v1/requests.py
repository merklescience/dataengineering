"""Utility functions to execute CH SQL queries"""

import logging
from typing import Dict, Union
import requests
from urllib.request import pathname2url
from itertools import islice

from requests import Response


class CHRpcFailedException(Exception):
    """Custom exception to throw any RPC failures"""

    pass


class CHRpcInsertException(Exception):
    """Custom exception to throw insert RPC failures"""

    pass


def execute_insert_sql(
    sql: str,
    payload_file_path: str,
    ch_conn: Dict[str, str],
    timeout=2700,
    batch_count=50000,
    data_format: str = "JSONEachRow",
) -> str:
    """
    Ingest 50k lines at a time from `payload_file_path` to CH as insert
    sql and returns the status msg throw CHRpcInsertException otherwise.


    :param `sql`
    :param `payload_file_path` the file to be imported in to table
    :param `ch_conn` map of CH co-ordinates & creds

    :return `str` status or CHRpcInsertException

    query --> CH

            |-200--> response

            |-otherwise--> CHRpcInsertException

    """
    ch_host = ch_conn["host"]
    ch_db = ch_conn["database"]
    ch_user = ch_conn["user"]
    ch_pwd = ch_conn["password"]
    ch_port = ch_conn.get("port", "8123")

    headers = {
        "cache-control": "no-cache",
        "X-ClickHouse-User": ch_user,
        "X-ClickHouse-Key": ch_pwd,
    }
    write_complete = True
    exception_trace = None
    with open(payload_file_path, "rb") as buf:
        header_line = ""
        logging.info(f"prepared payload for format {data_format}")
        for n_lines in iter(lambda: list(islice(buf, batch_count)), ()):
            if len(n_lines) > 0:
                s = b"\n"
                payload_data = str.encode("")
                if data_format == "CSVWithNames":
                    s = b""
                    if header_line == "":
                        with open(payload_file_path) as f:
                            header_line = f.readline().rstrip() + "\n"
                            payload_data = s.join(n_lines)
                    else:
                        batch_lines = s.join(n_lines)
                        payload_data = str.encode(header_line) + batch_lines
                else:
                    payload_data = s.join(n_lines)
                params = {"query": sql}
                url = f"http://{ch_host}:{ch_port}/{ch_db}?query={sql}"
                if len(payload_data) != 0:
                    response = requests.post(
                        url,
                        headers=headers,
                        verify=False,
                        timeout=timeout,
                        data=payload_data,
                        params=params,
                    )
                    logging.info(
                        f"payload URL {url} & CSV lines processed {len(n_lines)} (excluding header)"
                    )
                    logging.info(
                        f"running sql query {sql} {str(response.content)} - {response.status_code}"
                    )
                    try:
                        response.raise_for_status()
                    except Exception as e:
                        exception_trace = f"Failed to import data{str(response.content)} - trace {str(e)}"
                        write_complete = False
                        break
                else:
                    exception_trace = (
                        f"Failed to import due to empty payload {payload_data}"
                    )
                    write_complete = False
                    break
            else:
                break

    if write_complete is False:
        logging.error(exception_trace)
        raise CHRpcInsertException(f"query failed {exception_trace}")
    else:
        logging.info("Completed import to tbale")
        return f"Completed import to table"


def execute_sql(
    sql: str,
    ch_conn: Dict[str, str],
    raw_response: bool = False,
    timeout=2700,
    auth_type: str = "headers",
) -> Union[Response, str]:
    """
    captures the response for a CH sql.
    :param `sql`
    :param `ch_conn` map of CH co-ordinates & creds
    :return `str`
    """
    ch_host = ch_conn["host"]
    ch_db = ch_conn["database"]
    ch_user = ch_conn["user"]
    ch_pwd = ch_conn["password"]
    ch_port = ch_conn.get("port", "8123")

    headers = {
        "cache-control": "no-cache",
        "X-ClickHouse-User": ch_user,
        "X-ClickHouse-Key": ch_pwd,
    }
    if ch_host.startswith("http"):
        url = f"{ch_host}:{ch_port}/{ch_db}?query={pathname2url(sql)}"
    else:
        url = f"http://{ch_host}:{ch_port}/{ch_db}?query={pathname2url(sql)}"
    if auth_type == "url":
        response = requests.post(
            f"{url}&user={ch_user}&password={ch_pwd}", verify=False, timeout=timeout
        )
    else:
        response = requests.post(url, verify=False, timeout=timeout, headers=headers)
    logging.info(
        f"running sql query {sql} {str(response.content)} - {response.status_code}"
    )
    try:
        response.raise_for_status()
        strResponse = str(response.content)
        if strResponse.find("DB::Exception") != -1:
            raise CHRpcFailedException(f"SQL : {sql} failed {strResponse}")
        else:
            if raw_response:
                return response
            else:
                return strResponse
    except Exception as e:
        logging.error(
            f"Execute sql {sql} failed with response {str(response.content)} - {e}",
            exc_info=True,
        )
        raise CHRpcFailedException(f"SQL : {sql} failed {str(response.content)}")
