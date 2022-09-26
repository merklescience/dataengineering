import csv
import warnings
from typing import Callable, Union

import numpy as np
import pandas as pd
import requests

from dataengineering import chains
from dataengineering.tigergraph.v1 import loading_map
from dataengineering.tigergraph.v1.utils import (
    form_tg_loading_request,
    logger,
    tg_post_request,
)
from dataengineering.types.tigergraph_chain_state_response import (
    TigerGraphChainStateResponse,
)


def load_dataframe_to_tigergraph(
    df: pd.DataFrame,
    tigergraph_host: str,
    chain: str,
    loading_map_key: str,
    group_by: Union[str, list],
    aggregator_function: Callable,
    tg_batch_size: int = 10_000,
    **_,
):
    """This function uses the provided loading job and aggregator function to
    write a dataframe into tigergraph"""

    if tg_batch_size < 10_000:
        warnings.warn(
            (
                "The default tg_batch_size is 10000. "
                "Current value is = `{}` and might not be efficient."
            ).format(tg_batch_size),
            UserWarning,
        )
    grouped_df = df.groupby(group_by).apply(aggregator_function)
    rows, _ = grouped_df.shape

    for i in range(0, rows, tg_batch_size):
        loading_job = loading_map[loading_map_key]["loading_job"]
        loading_request = form_tg_loading_request(
            tg_ip=tigergraph_host,
            chain=chain,
            loading_job=loading_job,
        )
        filtered_df = grouped_df[i : i + tg_batch_size]
        data = filtered_df.to_csv(quoting=csv.QUOTE_NONNUMERIC)
        statistic = loading_map[loading_map_key]["stats"]
        tg_post_request(
            tg_request=loading_request,
            data=data,
            statistic=statistic,
        )


def write_chainstate_to_tigergraph(
    df: pd.DataFrame, tigergraph_host: str, chain: str, **_
):
    """Given a dataframe for a chain, this function writes the chainstate
    to the tigergraph instance"""
    chain_state = df.pivot_table(
        index="chain",
        values=["coin_price_usd", "block", "block_date_time"],
        aggfunc=np.max,
    ).reset_index()
    chain_state.rename(
        columns={"block": "block_number", "coin_price_usd": "price_usd"},
        inplace=True,
    )
    chain_state = chain_state[["chain", "price_usd", "block_date_time", "block_number"]]
    loading_job = loading_map["chain_state"]["loading_job"]
    tg_post_request(
        tg_request=form_tg_loading_request(
            tg_ip=tigergraph_host,
            chain=chain,
            loading_job=loading_job,
        ),
        data=chain_state.to_csv(
            quoting=csv.QUOTE_NONNUMERIC, index=False, header=False
        ),
        statistic=loading_map["chain_state"]["stats"],
    )


def get_chain_state(chain: chains.Chain, tigergraph_url: str):
    """Given a chain, it gets the chain state from tigergraph"""
    url = f"{tigergraph_url}/query/{chain.graphname}/get_chainstate"
    response = requests.get(url)
    response.raise_for_status()
    try:
        response_json = response.json()
    except requests.JSONDecodeError as e:
        logger.debug(response.content)
        raise e

    chain_state_response = TigerGraphChainStateResponse.parse_obj(response_json)

    return chain_state_response.results[0].chainstate
