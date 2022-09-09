"""Tigergraph-specific exceptions."""


class InvalidPayloadInTigergraphResponse(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NoValidLinesinTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class RejectedLinesinTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class FailedConditionLineInTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class InvalidJsonInTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class OversizeTokeninTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NotEnoughTokenInTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NotEnoughValidLinesInValidObjectInTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class InvalidAttributeInTigergraphRequest(Exception):
    """This exception is raised when there is a problem with the TigerGraph request."""


class NoFilesGeneratedFromS3(Exception):
    """This exception is raised when there are no files generated from s3"""
