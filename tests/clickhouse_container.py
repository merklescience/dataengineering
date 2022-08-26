# Adapted from https://github.com/testcontainers/testcontainers-python/blob/master/testcontainers/clickhouse.py
import os

import requests
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready


class HttpClickHouseContainer(DbContainer):
    """
    ClickHouse database container.
    The container that comes with `testcontainers` only supports tcp. This one exposes both interfaces
    """

    CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "test")
    CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "test")
    CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "test")

    def __init__(
        self,
        image="clickhouse/clickhouse-server:latest",
        http_port=8123,
        tcp_port=9000,
        user=None,
        password=None,
        dbname=None,
    ):
        super().__init__(image=image)

        self.CLICKHOUSE_USER = user or self.CLICKHOUSE_USER
        self.CLICKHOUSE_PASSWORD = password or self.CLICKHOUSE_PASSWORD
        self.CLICKHOUSE_DB = dbname or self.CLICKHOUSE_DB
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.ports_to_expose = [http_port, tcp_port]

    @wait_container_is_ready(requests.exceptions.ConnectionError)
    def _connect(self):
        from dataengineering.clickhouse import ClickhouseConnector

        connector = ClickhouseConnector(
            host="localhost",
            username=self.CLICKHOUSE_USER,
            password=self.CLICKHOUSE_PASSWORD,
            database=self.CLICKHOUSE_DB,
            port=self.get_exposed_port(self.http_port),
        )
        query = "SELECT version()"
        connector.execute(query)

    def _configure(self):
        self.with_exposed_ports(*self.ports_to_expose)
        self.with_env("CLICKHOUSE_USER", self.CLICKHOUSE_USER)
        self.with_env("CLICKHOUSE_PASSWORD", self.CLICKHOUSE_PASSWORD)
        self.with_env("CLICKHOUSE_DB", self.CLICKHOUSE_DB)
        self.with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", 1)

    def get_connection_url(self, host=None):
        return self._create_connection_url(
            dialect="http",
            username=self.CLICKHOUSE_USER,
            password=self.CLICKHOUSE_PASSWORD,
            db_name=self.CLICKHOUSE_DB,
            host=host,
            port=self.tcp_port,
        )
