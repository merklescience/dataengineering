from datetime import datetime
from enum import Enum
from typing import Optional


class Chain(Enum):
    """An enum of cryptocurrencies and chains"""

    BinanceSmartChain = "binance-smart-chain"
    Bitcoin = "bitcoin"
    BitcoinCash = "bitcoin-cash"
    BitcoinSv = "bitcoin-sv"
    Cardano = "cardano"
    Dogecoin = "dogecoin"
    Ethereum = "ethereum"
    Hedera = "hedera"
    Litecoin = "litecoin"
    Matic = "matic"
    Polkadot = "polkadot"
    Ripple = "ripple"
    Solana = "solana"
    Stellar = "stellar"
    Tron = "tron"
    Zilliqa = "zilliqa"

    @property
    def graphname(self):
        """Returns the graphname in tigergraph"""
        return self.get_internal_name()

    def get_internal_name(self) -> str:
        """Returns the internal name for a chain/currency"""
        if self == Chain.BinanceSmartChain:
            return "bsc"
        elif self == Chain.BitcoinSv:
            return "bitcoin_cash_sv"
        elif self == Chain.BitcoinCash:
            return "bitcoin_cash"
        else:
            return self.value

    @property
    def databasename(self):
        """Returns the clickhouse database name"""
        return self.get_internal_name()

    @staticmethod
    def get_smart_contract_chains():
        """Returns a list of smart contract chains"""
        return [chain for chain in Chain if chain.supports_smart_contracts]

    @staticmethod
    def get_non_smart_contract_chains():
        return [chain for chain in Chain if not chain.supports_smart_contracts]

    @property
    def supports_smart_contracts(self) -> bool:
        return self in [
            Chain.Ethereum,
            Chain.BinanceSmartChain,
            Chain.Matic,
            Chain.Tron,
            Chain.Zilliqa,
            Chain.Cardano,
            Chain.Stellar,
        ]

    def get_chainstate_query_for_clickhouse(
        self, query_date: Optional[datetime]
    ) -> str:
        """Returns the query to be run to get the chainstate for clickhouse"""
        if query_date is None:
            query_date = datetime.now()
        query_date_str = query_date.strftime("%Y-%m-%d")
        if self in [
            Chain.Bitcoin,
            Chain.Litecoin,
            Chain.Dogecoin,
            Chain.BitcoinCash,
            Chain.BitcoinSv,
        ]:
            query = (
                "SELECT MAX(block_number) AS block FROM {}.txns "
                "WHERE block_date_time >= toDate('{}') FORMAT JSON"
            ).format(self.databasename, query_date_str)
        elif self in [Chain.Ethereum]:
            query = (
                "SELECT MAX(block_number) AS block FROM ethereum.tld_raw_hot "
                "WHERE block_date_time >= toDate('{}') FORMAT JSON"
            ).format(query_date_str)
        elif self == Chain.Ripple:
            query = (
                "SELECT max(toUInt64(block)) as block from ripple.master "
                "WHERE block_date_time >= toDate('{}') FORMAT JSON"
            ).format(query_date_str)
        elif self == Chain.Hedera:
            query = (
                # NOTE: This query takes a transaction ID that looks like this
                # 0.0.887615-1651341539-309799857
                # First, get a list of all transaction IDs for the last block_date_time
                # There *may* be more than 1.
                # We need to convert this to `1651341539-309799857`
                # and then we need to convert that to `1651341539.309799857`
                # and finally, sort these in descending order, and get the *max* timestamp
                # this is necessary because of the fact that there may be more than one txn
                # at a block_timestamp, and when our ETL runs, if we complete at a
                #  non-rounded-timestamp eg. 10:55:25s, then it'll ignore the
                # rest of the txns from 10:55:26-10:55:59
                # The only way to avoid this is to get the nanosecond-precision txn timestamp,
                # which is in the txn_id
                # select all transactions where the time is the latest timestamp
                r"SELECT replace(replaceRegexpOne(transaction_id, '\d\\.\d\\.\d+-', ''), '-', '.') "
                "AS block "
                "FROM hedera.master WHERE block_date_time >= toDate('{}') "
                "ORDER BY block DESC LIMIT 1 FORMAT JSON"
            ).format(query_date_str)
        else:
            query = (
                "SELECT max(block) as block from {}.master "
                "WHERE block_date_time >=toDate('{}') FORMAT JSON"
            ).format(self.databasename, query_date_str)
        return query
