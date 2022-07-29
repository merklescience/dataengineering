"""Tests the airflow utility functions"""
import os
from unittest.mock import Mock, patch

import pytest
from airflow.models import Variable, variable


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
def test_build_bigquery_destination_nonproduction(server_env, dest_id):
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


def test_build_bigquery_destination_production():
    """Tests that you can build the exact table name given airflow variables for a production server environment"""
