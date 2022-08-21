from enum import Enum


class Chain(Enum):
    """An enum of cryptocurrencies and chains"""

    Bitcoin = "bitcoin"
    BitcoinCash = "bitcoin-cash"
    BitcoinSv = "bitcoin-sv"
    BinanceSmartChain = "binance-smart-chain"
    Cardano = "cardano"
    Dogecoin = "dogecoin"
    Matic = "matic"
    Ethereum = "ethereum"
    Hedera = "hedera"
    Litecoin = "litecoin"
    Polkadot = "polkadot"
    Solana = "solana"
    Stellar = "stellar"
    Tron = "tron"
    Zilliqa = "zilliqa"

    @property
    def graphname(self):
        """Returns the graphname in tigergraph"""
        if self == Chain.BinanceSmartChain:
            return "bsc"
        elif self == Chain.BitcoinSv:
            return "bitcoin_cash_sv"
        elif self == Chain.BitcoinCash:
            return "bitcoin_cash"
        else:
            return self.value
