"""chainstate type"""
from pydantic import BaseModel

from dataengineering.chains import Chain


class ChainState(BaseModel):
    """This type defines what the chainstate looks like"""

    block_number: int
    block_timestamp: int
    chain: Chain
