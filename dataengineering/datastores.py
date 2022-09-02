"""DataStores that we use."""

from enum import Enum


class DataStore(Enum):
    """An enum that contains all our internal data stores,
    "databases" that contain chain data that we use directly, or indirectly,
    on our products."""

    ClickHouse = "clickhouse"
    TigerGraph = "tigergraph"
    BigQuery = "bigquery"
