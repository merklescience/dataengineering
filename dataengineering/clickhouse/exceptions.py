class ClickhouseFileFormatError(Exception):
    """Raised when there is a problem with the file type being uploaded into Clickhouse."""


class ClickhouseStdOutValidationError(Exception):
    """Raised when there is a problem with ClickHouseBashHook._check_stdout"""


class ClickhouseOutputFileValidationError(Exception):
    """Raised when there is a problem with ClickHouseBashHook._check_file_for_clickhouse_error"""
