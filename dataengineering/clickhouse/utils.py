from dataengineering import logger
from dataengineering.chains import Chain
from dataengineering.clickhouse import ClickhouseConnector
from dataengineering.types.chainstate import ChainState


def get_chain_state(chain: Chain, *args, **kwargs) -> ChainState:
    """Returns the chain state of a cryptocurrency"""
    query = chain.get_chainstate_query_for_clickhouse(
        query_date=kwargs.get("query_date")
    )
    connector = ClickhouseConnector(*args, **kwargs)
    result = connector.read(query)
    logger.debug(result)
    return result
