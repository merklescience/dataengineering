import warnings

import requests

from dataengineering import logger
from dataengineering.tigergraph import exceptions

# NOTE: Set GSQL_TIMEOUT to 20 minutes
GSQL_TIMEOUT = f"{20 * 60 * 1000}"


def form_tg_loading_request(tg_ip, chain, loading_job) -> str:
    """Creates the tigergraph loading request URL"""
    warnings.warn(
        (
            "This function to create the URL for a tigergraph loading"
            "job will be deprecated."
        ),
        PendingDeprecationWarning,
    )
    return (
        f"http://{tg_ip}:9000/ddl/{chain}?&tag={loading_job}&filename=f1&sep=,&eol=\n"
    )


def tg_post_request(tg_request, data, statistic):
    """Make Tigergraph HTTP POST request
    :param tg_request:
    :param data:
    :param statistic:
    """
    logger.debug(f"Request to Tigergraph URL={tg_request}")
    response = requests.post(
        tg_request, data=data, headers={"GSQL-TIMEOUT": GSQL_TIMEOUT}
    )
    response.raise_for_status()
    try:
        response_json = response.json()
    except requests.JSONDecodeError as e:
        logger.error("Response from tigergraph={}".format(response.content))
        raise exceptions.InvalidPayloadInTigergraphResponse() from e

    try:
        stats = response_json["results"][0]["statistics"]
    except KeyError as e:
        logger.error("Response JSON=`{}`".format(response_json))
        raise exceptions.InvalidPayloadInTigergraphResponse() from e

    required_statistic = stats[statistic]

    if stats["validLine"] == 0:
        raise exceptions.NoValidLinesinTigergraphRequest()
    elif stats["rejectLine"] > 0:
        raise exceptions.RejectedLinesinTigergraphRequest()
    elif stats["failedConditionLine"] > 0:
        raise exceptions.FailedConditionLineInTigergraphRequest()
    elif stats["invalidJson"] > 0:
        raise exceptions.InvalidJsonInTigergraphRequest()
    elif stats["oversizeToken"] > 0:
        raise exceptions.OversizeTokeninTigergraphRequest()
    elif stats["notEnoughToken"] > 0:
        raise exceptions.NotEnoughTokenInTigergraphRequest()
    elif (required_statistic[0]["validObject"] - stats["validLine"]) > 1:
        raise exceptions.NotEnoughValidLinesInValidObjectInTigergraphRequest()
    elif required_statistic[0]["invalidAttribute"] > 1:
        raise exceptions.InvalidAttributeInTigergraphRequest()
