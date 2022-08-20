import csv
import tempfile
import textwrap

import pytest
import requests

from dataengineering import logger
from dataengineering.clickhouse import ClickhouseConnector
from dataengineering.clickhouse.connector import tempfile


@pytest.fixture(scope="module")
def connector():
    """This fixture returns a ClickhouseConnector attached to a temporary clickhouse server launched via docker."""
    from .clickhouse_container import HttpClickHouseContainer

    with HttpClickHouseContainer("clickhouse/clickhouse-server:21.8") as clickhouse:
        connection_string = clickhouse.get_connection_url()
        logger.debug(connection_string)
        host = "localhost"
        username = clickhouse.CLICKHOUSE_USER
        password = clickhouse.CLICKHOUSE_PASSWORD
        port = clickhouse.get_exposed_port(clickhouse.http_port)
        db = clickhouse.CLICKHOUSE_DB
        logger.debug(
            f"username={username}, host={host}, password={password}, database={db}, port={port}"
        )
        connector = ClickhouseConnector(
            host=host, username=username, password=password, database=db, port=port
        )
        yield connector


def test_clickhouse_connector(connector):
    """Tests that the ClickhouseConnector can read from an actual clickhouse server"""
    test_query = "SHOW DATABASES"
    result = connector.execute(test_query)
    assert result is not None, "Unable to run query on a test clickhouse instance."
    assert isinstance(
        result, requests.Response
    ), "Clickhouse result isn't a `requests.Response`"


def test_clickhouse_write_to_database(connector):
    """"""
    from mimesis import Address, Business, Person

    headers = [
        "full_name",
        "age",
        "company",
        "cryptocurrency_iso_code",
        "price_in_btc",
        "price",
        "address",
        "state",
        "country",
    ]
    # use the connector to create this table.
    fake_data_table_name = "test_fake_data"
    creation_query = textwrap.dedent(
        """
        CREATE TABLE {}.{} (
            full_name String,
            age Int32,
            company String,
            cryptocurrency_iso_code String,
            price_in_btc String,
            price String,
            address String,
            state String,
            country String
            )
            ENGINE = Log""".format(
            connector.database, fake_data_table_name
        )
    )
    logger.debug(creation_query)
    connector.execute(creation_query, mode="write")
    # write to a csv file
    with tempfile.NamedTemporaryFile() as fake_data_file:
        with open(fake_data_file.name, "w") as f:
            writer = csv.DictWriter(
                f, fieldnames=headers, quotechar='"', quoting=csv.QUOTE_NONNUMERIC
            )
            writer.writeheader()
            for _ in range(5000):
                person = Person("en")
                address = Address("en")
                business = Business("en")
                row = dict(
                    full_name=person.full_name(),
                    age=person.age(),
                    company=business.company(),
                    cryptocurrency_iso_code=business.cryptocurrency_iso_code(),
                    price_in_btc=business.price_in_btc(),
                    price=business.price(),
                    address=address.address(),
                    state=address.state(),
                    country=address.country(),
                )
                writer.writerow(row)
        # now, use the connector to write this file to clickhouse
        connector.write_from_file(
            connector.database, fake_data_table_name, fake_data_file.name
        )
    # None of these should raise an error.
    # don't need to assert anything.
