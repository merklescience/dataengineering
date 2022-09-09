"""Airflow specific constants"""
import json

from decouple import config

# These variables *need* to be set.
CLICKHOUSE_URI = config.get("CLICKHOUSE_URI")
CLICKHOUSE_USER = config.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = config.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_URIS = json.loads(config.get("CLICKHOUSE_URIS"))
