import datetime
from typing import Optional

from dataengineering import logger
from dataengineering.chains import Chain
from dataengineering.clickhouse import ClickhouseConnector


def get_chain_state(
    chain: Chain, query_date: Optional[datetime.datetime] = None, *args, **kwargs
):
    """Returns the chain state of a cryptocurrency"""
    if query_date is None:
        query_date = datetime.datetime.now()

    query_date_str = query_date.strftime("%Y-%m-%d")
    if chain in [
        Chain.Bitcoin,
        Chain.Litecoin,
        Chain.Dogecoin,
        Chain.BitcoinCash,
        Chain.BitcoinSv,
    ]:
        query = (
            "SELECT MAX(block_number) AS block FROM {}.txns "
            "WHERE block_date_time >= toDate('{}') FORMAT JSON"
        ).format(chain.clickhouse_database_name, query_date_str)
    elif chain in [Chain.Ethereum]:
        query = (
            "SELECT MAX(block_number) AS block FROM ethereum.tld_raw_hot "
            "WHERE block_date_time >= toDate('{}') FORMAT JSON"
        ).format(query_date_str)
    elif chain == Chain.Ripple:
        query = (
            "SELECT max(toUInt64(block)) as block from ripple.master "
            "WHERE block_date_time >=toDate('{}') FORMAT JSON"
        ).format(query_date_str)
    elif chain == Chain.Hedera:
        query = (
            "SELECT transaction_id, block_date_time FROM hedera.master "
            "WHERE block_date_time >= toDate('{}') order by block_date_time desc limit 1 FORMAT JSON"
        ).format(query_date_str)
    else:
        query = (
            "SELECT max(block) as block from {}.master "
            "WHERE block_date_time >=toDate('{}') FORMAT JSON"
        ).format(database_name, query_date_str)
    connector = ClickhouseConnector(*args, **kwargs)
    result = connector.read(query)
    logger.debug(result)
    return result
