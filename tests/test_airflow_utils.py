"""Tests the airflow utility functions"""
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
