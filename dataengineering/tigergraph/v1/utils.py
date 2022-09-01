import warnings

import pandas as pd
import requests

from dataengineering import logger
from dataengineering.tigergraph import exceptions

# NOTE: Set GSQL_TIMEOUT to 20 minutes
GSQL_TIMEOUT = f"{20 * 60 * 1000}"


def tg_get_request(connection_string):
    """
        Make Tigergraph HTTP GET request
    :param connection_string:
    """
    logging.info(connection_string)
    response = requests.get(
        connection_string,
        headers={"GSQL-TIMEOUT": GSQL_TIMEOUT, "GSQL-THREAD-LIMIT": GSQL_THREAD_LIMIT},
    )

    logging.info(response.content)
    logging.info(response)
    if response.status_code == 200:
        logging.info("Successful")
    else:
        raise AirflowException("Error in processing")
    return response


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


def transactions_agg(x):
    """
    Aggregation logic for daily_transactions loading job on tigergraph.
    """
    aggregate = {
        "external_value": x[x["type"] == 0]["coin_value"].sum(),
        "external_value_usd": x[x["type"] == 0]["coin_value_usd"].sum(),
        "block_date": x["block_date_time"].max(),
        "txn_fee": x["fee"].sum(),
        "txn_fee_usd": x["fee_usd"].sum(),
        "internal_value": x[x["type"] == 1]["coin_value"].sum(),
        "internal_value_usd": x[x["type"] == 1]["coin_value_usd"].sum(),
        "token_transfer_usd": x[x["type"] == 2]["coin_value_usd"].sum(),
    }
    return pd.Series(aggregate)


def link_inputs_agg(x):
    """
    Aggregation logic for daily_link_inputs loading job on tigergraph.
    """
    aggregate = {
        "value": (
            x[x["type"].isin([0, 1])]["coin_value"] + x[x["type"].isin([0, 1])]["fee"]
        ).sum(),
        "value_usd": (x["coin_value_usd"] + x["fee_usd"]).sum(),
    }
    return pd.Series(aggregate)


def link_outputs_agg(x):
    """
    Aggregation logic for daily_link_outputs loading job on tigergraph.
    """
    aggregate = {
        "value": x[x["type"].isin([0, 1])]["coin_value"].sum(),
        "value_usd": x["coin_value_usd"].sum(),
    }
    return pd.Series(aggregate)
