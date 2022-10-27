"""This test module contains all the tests related to the module import"""


def test_import():
    """Tests that the library can be imported;

    This is necessary since there have been ser"""
    import dataengineering

    assert dataengineering.__version__ is not None, "The version string is not defined"

    from dataengineering import airflow as af
    from dataengineering import clickhouse, coinprice, constants, logger, tigergraph
    from dataengineering.airflow import bitquery
    from dataengineering.airflow.bigquery import utils
    from dataengineering.clickhouse import connector, exceptions, utils
    from dataengineering.clickhouse.v1 import bash_hook, operator, requests
    from dataengineering.coinprice import pricing_checks, utils
    from dataengineering.logger import logger
    from dataengineering.tigergraph import exceptions, utils
    from dataengineering.tigergraph.v1 import streaming_operator, utils
    from dataengineering.utils import notifications
