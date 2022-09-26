from dataengineering.chains import Chain
from dataengineering.datastores import DataStore
from dataengineering.tigergraph.utils import get_chain_state
from dataengineering.types.chainstate import ChainState
from dataengineering.types.tigergraph_chain_state_response import (
    TigerGraphChainStateBody,
)


def test_get_chainstate():
    chain = Chain.Bitcoin
    chain_state = get_chain_state(chain, "https://tigergraph.merklescience.com")
    assert isinstance(chain_state, TigerGraphChainStateBody), "Wrong type returned."


def test_get_chainstate_2():
    chain = Chain.Bitcoin
    chain_state = DataStore.TigerGraph.get_chainstate(
        chain, tigergraph_base_url="https://tigergraph.merklescience.com"
    )
    assert isinstance(chain_state, ChainState), "Wrong type returned."
