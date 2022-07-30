"""This test module contains all the tests related to the module import"""


def test_import():
    """Tests that the library can be imported"""
    import dataengineering

    assert (
        dataengineering.__doc__ is not None
    ), "The documentation string is not defined."
    from dataengineering import utils

    assert (
        utils.__doc__ is not None
    ), "The dataengineering.utils module has no docstring."
    from dataengineering import airflow as af

    assert (
        af.__doc__ is not None
    ), "The dataengineering.airflow module has no docstring."
