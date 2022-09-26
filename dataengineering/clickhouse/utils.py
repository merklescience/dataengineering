import datetime

from dataengineering import logger
from dataengineering.chains import Chain
from dataengineering.clickhouse import ClickhouseConnector


def get_chain_state(chain: Chain, *args, **kwargs):
    """Returns the chain state of a cryptocurrency"""
    if kwargs.get("query_date") is None:
        query_date = datetime.datetime.now()
    else:
        query_date = kwargs["query_date"]
        if not isinstance(query_date, datetime.datetime):
            raise TypeError(
                "Expected a Datetime object for query date, not `{} (type={})`",
                query_date,
                type(query_date),
            )
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
        ).format(chain.databasename, query_date_str)
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
            r"SELECT replace(replaceRegexpOne(transaction_id, '\d\\.\d\\.\d+-', ''), '-', '.') as block "
            r"FROM hedera.master WHERE block_date_time >= toDate('{}') "
            r"order by block desc limit 1 FORMAT JSON"
        ).format(query_date_str)
    else:
        query = (
            "SELECT max(block) as block from {}.master "
            "WHERE block_date_time >=toDate('{}') FORMAT JSON"
        ).format(chain.databasename, query_date_str)
    connector = ClickhouseConnector(*args, **kwargs)
    result = connector.read(query)
    logger.debug(result)
    return result
