"""Tigergraph-specific exceptions."""


class InvalidPayloadInTigergraphResponse(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class NoValidLinesinTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class RejectedLinesinTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class FailedConditionLineInTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class InvalidJsonInTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class OversizeTokeninTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class NotEnoughTokenInTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class NotEnoughValidLinesInValidObjectInTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""


class InvalidAttributeInTigergraphRequest(Exception):
    """This exception in raised when there is a problem with the TigerGraph request."""
