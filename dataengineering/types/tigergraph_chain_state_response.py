"""This file, and especially the `TigerGraphChainStateResponse` object defines the
response that can be expected from the `get_chainstate` query in TigerGraph
The payload is of the following format
{
     "version": {
         "edition": "enterprise",
         "api":"v2",
         "schema":5
     },
     "error":false,
     "message":"",
     "results":[{
         "@@chainstate": {
             "latest_block_date_time":0,
             "latest_block_number":0 <- this is what we need.
         }
     }]
 }
"""

from typing import List

from pydantic import BaseModel, Field


class TigerGraphVersion(BaseModel):
    """Defines the version portion of the TigerGraphChainStateResponse"""

    # TODO: Can make this an enum
    edition: str
    api: str
    version_schema: int = Field(alias="schema")


class TigerGraphChainStateBody(BaseModel):
    """Defines the chain state body"""

    latest_block_date_time: int
    latest_block_number: int


class TigerGraphChainStateResult(BaseModel):
    """Defines the `@@chainstate` portion of the response"""

    chainstate: TigerGraphChainStateBody = Field(alias="@@chainstate")


class TigerGraphChainStateResponse(BaseModel):
    """Defines the response from the `get_chainstate` endpoint in tigergraph"""

    version: TigerGraphVersion
    error: bool
    message: str
    results: List[TigerGraphChainStateResult]
