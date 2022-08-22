import datetime
from typing import Optional

from dataengineering import logger
from dataengineering.chains import Chain
from dataengineering.clickhouse import ClickhouseConnector


def get_chain_state(
    chain: Chain,
    query_date: Optional[datetime.datetime] = None,
    database_name: Optional[str] = None,
    *args,
    **kwargs
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
            "WHERE block_date_time >= toDate('{}') FORMAT JSON"
        ).format(query_date_str)
    elif chain == Chain.Hedera:
        query = (
            # NOTE: This query takes a transaction ID that looks like this
            # 0.0.887615-1651341539-309799857
            # First, get a list of all transaction IDs for the last block_date_time
            # There *may* be more than 1.
            # We need to convert this to `1651341539-309799857`
            # and then we need to convert that to `1651341539.309799857`
            # and finally, sort these in descending order, and get the *max* timestamp
            # this is necessary because of the fact that there may be more than one txn
            # at a block_timestamp, and when our ETL runs, if we complete at a non-rounded-timestamp
            # eg. 10:55:25s, then it'll ignore the rest of the txns from 10:55:26-10:55:59
            # The only way to avoid this is to get the nanosecond-precision txn timestamp,
            # which is in the txn_id
            # select all transactions where the time is the latest timestamp
            "SELECT replace(substring(transaction_id, 12), '-', '.') as block "
            "FROM hedera.master WHERE block_date_time >= toDate('{}') "
            "order by block desc limit 1 FORMAT JSON"
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
