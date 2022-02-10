def test_import():
    """Tests that the library can be imported"""
    import dataengineering

    assert dataengineering.__version__ is not None, "The version string is not defined"
