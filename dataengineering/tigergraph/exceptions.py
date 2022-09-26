"""TigerGraph-specific exceptions."""


class InvalidPayloadInTigerGraphResponse(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NoValidLinesinTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class RejectedLinesinTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class FailedConditionLineInTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class InvalidJsonInTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class OversizeTokeninTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NotEnoughTokenInTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NotEnoughValidLinesInValidObjectInTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class InvalidAttributeInTigerGraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NoFilesGeneratedFromS3(Exception):
    """This exception is raised when there are no files generated from s3"""
