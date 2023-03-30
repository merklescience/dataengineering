import os
import shutil
import warnings

import pandas as pd
import requests
from google.cloud import storage

from dataengineering import logger
from dataengineering.tigergraph import exceptions

# NOTE: Set GSQL_TIMEOUT to 20 minutes
# TODO: move this to constants, ask why it's a string.
GSQL_TIMEOUT = 20 * 60 * 1000
GSQL_THREAD_LIMIT = 1
MAX_RETRY_COUNT = 5


def _form_tigergraph_request(tigergraph_ip, chain, loading_job) -> str:
    return (
        f"http://{tigergraph_ip}:9000/ddl/{chain}?&"
        f"tag={loading_job}&filename=f1&sep=,&eol=\n"
    )


def tg_get_request(connection_string):
    """
        Make Tigergraph HTTP GET request
    :param connection_string:
    """
    logger.debug(connection_string)
    response = requests.get(
        connection_string,
        headers={"GSQL-TIMEOUT": GSQL_TIMEOUT, "GSQL-THREAD-LIMIT": GSQL_THREAD_LIMIT},
    )

    logger.debug(response.content)
    logger.debug(response)
    response.raise_for_status()
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


def tg_post_request(tigergraph_request, data, statistic):
    """Make Tigergraph HTTP POST request
    :param tigergraph_request:
    :param data:
    :param statistic:
    """
    logger.debug(f"Request to Tigergraph URL={tigergraph_request}")
    response = requests.post(
        tigergraph_request, data=data, headers={"GSQL-TIMEOUT": GSQL_TIMEOUT, "GSQL-THREAD-LIMIT": GSQL_THREAD_LIMIT}
    )
    response.raise_for_status()
    try:
        response_json = response.json()
    except requests.JSONDecodeError as e:
        logger.error(
            "Error parsing response from tigergraph=`{}`".format(response.content)
        )
        raise exceptions.InvalidPayloadInTigergraphResponse() from e

    try:
        stats = response_json["results"][0]["statistics"]
        logger.info(f"STATS for pushing data to TG: {stats}")
    except KeyError as e:
        logger.error("Response JSON=`{}`".format(response_json))
        raise exceptions.InvalidPayloadInTigergraphResponse() from e

    required_statistic = stats[statistic]

    if stats["validLine"] == 0:
        logger.error("NoValidLinesinTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.NoValidLinesinTigergraphRequest()
    elif stats["rejectLine"] > 0:
        logger.error("RejectedLinesinTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.RejectedLinesinTigergraphRequest()
    elif stats["failedConditionLine"] > 0:
        logger.error("FailedConditionLineInTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.FailedConditionLineInTigergraphRequest()
    elif stats["invalidJson"] > 0:
        logger.error("InvalidJsonInTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.InvalidJsonInTigergraphRequest()
    elif stats["oversizeToken"] > 0:
        logger.error("OversizeTokeninTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.OversizeTokeninTigergraphRequest()
    elif stats["notEnoughToken"] > 0:
        logger.error("NotEnoughTokenInTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.NotEnoughTokenInTigergraphRequest()
    elif (required_statistic[0]["validObject"] - stats["validLine"]) > 1:
        logger.error(
            "NotEnoughValidLinesInValidObjectInTigergraphRequest. stats=`{}`".format(
                stats
            )
        )
        raise exceptions.NotEnoughValidLinesInValidObjectInTigergraphRequest()
    elif required_statistic[0]["invalidAttribute"] > 1:
        logger.error("InvalidAttributeInTigergraphRequest. stats=`{}`".format(stats))
        raise exceptions.InvalidAttributeInTigergraphRequest()


def _post_local_file_to_tg(
    filename, tigergraph_ip, chain, loading_job, stats, size_hint=2 << 22
):
    file_handler = open(filename, "r")
    data = file_handler.readlines(size_hint)
    while len(data) > 0:
        tg_post_request(
            tigergraph_request=_form_tigergraph_request(
                tigergraph_ip=tigergraph_ip, chain=chain, loading_job=loading_job
            ),
            data="".join(data),
            statistic=stats,
        )
        data = file_handler.readlines(size_hint)


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


def run_s3_loading_job_tigergraph(**kwargs):
    """
    Run file loading of S3 files to Tigergraph via HTTP POST
    :param kwargs:
    """
    import boto3

    s3_client = boto3.client(
        "s3",
        region_name="us-east-2",
        aws_access_key_id=kwargs["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=kwargs["AWS_SECRET_ACCESS_KEY"],
    )

    s3_client.download_file(
        Filename=kwargs["FILENAME"], Bucket=kwargs["AWS_BUCKET"], Key=kwargs["AWS_KEY"]
    )

    _post_local_file_to_tg(
        filename=kwargs["FILENAME"],
        tigergraph_ip=kwargs["TIGERGRAPH_HOST"],
        chain=kwargs["CHAIN"],
        loading_job=kwargs["JOB"],
        stats=kwargs["STATS"],
    )


def run_multifile_gcs_loading_job_tigergraph(**kwargs):
    """
    Run multi file loading of GCS files to Tigergraph via HTTP POST
    :param kwargs:
    """
    client = storage.Client()
    logger.info(kwargs)
    local_folder = f"{kwargs['CHAIN']}_{kwargs['RESOURCE']}_{kwargs['ds']}"
    prefix = f"{kwargs['CHAIN']}{kwargs['DEPLOY_LABEL']}/{kwargs['RESOURCE']}/{kwargs['ds']}/"

    # TODO: Use tempfolder library for this stuff
    if os.path.exists(local_folder):
        shutil.rmtree(local_folder)
    os.makedirs(local_folder)

    logger.debug(prefix)
    logger.debug(local_folder)

    blobs = list(client.list_blobs(kwargs["GCS_BUCKET"], prefix=prefix))

    if len(blobs) == 0:
        raise exceptions.NoFilesGeneratedFromS3("No files generated")

    for blob in blobs:
        logger.debug("{}", blob)
        filename = blob.name.split("/")[blob.name.count("/")]
        local_full_filepath = os.path.join(local_folder, filename)
        blob.download_to_filename(local_full_filepath)

        _post_local_file_to_tg(
            filename=local_full_filepath,
            tigergraph_ip=kwargs["TIGERGRAPH_HOST"],
            chain=kwargs["CHAIN"],
            loading_job=kwargs["JOB"],
            stats=kwargs["STATS"],
        )

        os.remove(local_full_filepath)
