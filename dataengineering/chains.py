from enum import Enum


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
