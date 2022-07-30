"""Tests the airflow utility functions"""
import os
from unittest.mock import Mock, patch

import pytest


def test_import():
    """Tests that the airflow utility module can be imported"""
    try:
        from dataengineering.airflow.bigquery import utils
    except ImportError:
        pytest.fail(
            "Unable to find the `dataengineering.airflow.bigquery.utils` module!"
        )


def test_apply_env_variables_on_blob():
    """Tests that env variables can be replaces in a string"""
    from dataengineering.airflow.bigquery.utils import apply_env_variables_on_blob

    blob = """
    SELECT * from `[[gcp_project]].[[dataset]].[[table_id]]`;
    """
    environment = {
        "gcp_project": "test-project",
        "dataset": "test-dataset",
        "table_id": "test-table",
    }
    expected_result = """
    SELECT * from `test-project.test-dataset.test-table`;
    """
    result = apply_env_variables_on_blob(blob, environment)
    assert (
        result == expected_result
    ), "Templating function couldn't account for templates with `[[]]`."


def variable_get(variable_name):
    """Patched function that one can use instead of `airflow.models.Variable.get`"""
    if variable_name == "BIGQUERY_DESTINATION_PROJECT":
        return "test-project"
    if variable_name == "BIGQUERY_DESTINATION_DATASET":
        return "test-dataset"


@pytest.mark.parametrize(
    "server_env,dest_id",
    [
        ("local", "test-project.test-dataset.table-name"),
        ("dev", "test-project.test-dataset.table-name"),
        ("production", "test-project.prod-dataset.table-name"),
    ],
)
def test_build_bigquery_destination(server_env, dest_id):
    """Tests that you can build the exact table name given airflow variables for a non-prod environment"""
    from dataengineering.airflow.bigquery.utils import build_bigquery_destination

    os.environ["SERVER_ENV"] = server_env
    mocked_variable_get = Mock()
    mocked_variable_get.side_effect = variable_get
    with patch("airflow.models.Variable.get", mocked_variable_get):
        bigquery_desination = build_bigquery_destination("prod-dataset", "table-name")
    assert (
        bigquery_desination == dest_id
    ), f"Unable to build `{server_env}` bigquery destination"


def get_fake_query():
    """Returns a fake SQL query every time."""
    import random

    import mimesis

    person = mimesis.Person()
    address = mimesis.Address()
    queries = [
        "SELECT * from people where name = '{}';".format(person.name()),
        "SELECT * from people where address = '{}';".format(address.address()),
        "SELECT * from people where name ='{}' and address = '{}';".format(
            person.name, address.address()
        ),
        """SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
        FROM Orders
        INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;""",
        """SELECT column_name(s)
        FROM table1
        FULL OUTER JOIN table2
        ON table1.column_name = table2.column_name
        WHERE condition;""",
        """INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
        SELECT SupplierName, ContactName, Address, City, PostalCode, Country FROM Suppliers;""",
        """CREATE TABLE Persons (
            ID int NOT NULL,
            LastName varchar(255) NOT NULL,
            FirstName varchar(255),
            Age int,
            City varchar(255) DEFAULT 'Sandnes'
        );""",
    ]
    return random.choice(queries)


def generate_fake_queries(query_path):
    """This function generates a bunch of fake SQL queries in a folder."""
    import random
    import uuid

    number_of_folders = random.randint(10, 100)
    queries = {}
    for _ in range(random.randint(1, 50)):
        file_path = os.path.join(query_path, "{}.sql".format(uuid.uuid4()))
        with open(file_path, "w") as f:
            query = get_fake_query()
            f.write(query)
        queries[file_path] = query
    folders = [str(uuid.uuid4()) for _ in range(number_of_folders)]
    for folder in folders:
        folder_path = os.path.join(query_path, folder)
        os.makedirs(folder_path, exist_ok=True)
        for _ in range(random.randint(10, 100)):
            file_path = os.path.join(folder_path, "{}.sql".format(uuid.uuid4()))
            with open(file_path, "w") as f:
                query = get_fake_query()
                f.write(query)
            queries[file_path] = query
    return queries


def test_join_bigquery_queries_in_folder_without_environment():
    """Tests that given a `DAGS_FOLDER`, this function can join all the queri"""
    import tempfile

    from dataengineering.airflow.bigquery.utils import join_bigquery_queries_in_folder

    # generate a temporary directory
    # don't use the context manager so that we can debug code if it fails in testing.
    temp_dir = tempfile.TemporaryDirectory()
    queries_folder = "sql_queries"
    os.environ["DAGS_FOLDER"] = temp_dir.name
    queries_dir_path = os.path.join(temp_dir.name, queries_folder)
    os.makedirs(queries_dir_path)
    queries = generate_fake_queries(queries_dir_path)
    merged_query = join_bigquery_queries_in_folder(queries_folder)
    for file_path, query in queries.items():
        assert (
            query in merged_query
        ), f"One of these queries is missing in the merged query. Missing Query = `{file_path}`"

    temp_dir.cleanup()
