"""DataStores that we use."""

from enum import Enum

from dataengineering.chains import Chain
from dataengineering.clickhouse import ClickhouseConnector
from dataengineering.logger import logger
from dataengineering.types import ChainState, TigerGraphChainStateResponse


class DataStore(Enum):
    """An enum that contains all our internal data stores,
    "databases" that contain chain data that we use directly, or indirectly,
    on our products."""

    ClickHouse = "clickhouse"
    TigerGraph = "tigergraph"
    BigQuery = "bigquery"

    def get_chainstate(self, chain: Chain, *args, **kwargs) -> ChainState:
        """The chainstate property.
        # TODO: Document the `args` and `kwargs`
        """
        import requests

        if self == DataStore.ClickHouse:
            query = chain.get_chainstate_query_for_clickhouse(
                query_date=kwargs.get("query_date")
            )
            connector = ClickhouseConnector(*args, **kwargs)
            result = connector.read(query, deserialize_with=ChainState.parse_obj)
        elif self == DataStore.TigerGraph:
            tigergraph_url = kwargs["tigergraph_base_url"]
            url = f"{tigergraph_url}/query/{chain.graphname}/get_chainstate"
            response = requests.get(url)
            logger.debug(f"Querying: {url}")
            response.raise_for_status()
            try:
                response_json = response.json()
            except requests.JSONDecodeError as e:
                logger.debug(response.content)
                raise e
            chainstate_response = TigerGraphChainStateResponse.parse_obj(response_json)
            chainstate = chainstate_response.results[0].chainstate
            result = ChainState(
                block_number=chainstate.latest_block_number,
                block_timestamp=chainstate.latest_block_date_time,
                chain=chain,
            )
        else:
            raise NotImplementedError(
                "Chain state for bigquery tables is not yet implemented."
            )

        return result
