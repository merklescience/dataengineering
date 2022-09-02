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
