"""TigerGraph v1 code
Mostly copied over from `merklescience/airflow-dags/resources/tg`"""

loading_map = {
    "transactions": {"stats": "vertex", "loading_job": "daily_transactions"},
    "link_inputs": {"stats": "edge", "loading_job": "daily_links_inputs"},
    "link_outputs": {"stats": "edge", "loading_job": "daily_links_outputs"},
    "chain_state": {"stats": "vertex", "loading_job": "streaming_chainstate_v2"},
    "streaming_links": {
        "stats": "vertex",
        "loading_job": "streaming_address_links_flat",
    },
}
