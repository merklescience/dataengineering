"""dataengineering python SDK

This library contains all the code that we reuse here at Merkle Science Data Engineering.
This includes, but is not limited to, utility functions and Apache Airflow operators.

See the project's README for more information regarding the development and usecases.
"""
import pkg_resources

__version__ = pkg_resources.get_distribution("dataengineering").version

from .logger import logger

try:
    import airflow as _
except ImportError:
    import warnings

    warnings.warn(
        "The environment you're using this library in doesn't have Apache-Airflow installed, "
        "and this library will have limited functionality.",
        ImportWarning,
    )
