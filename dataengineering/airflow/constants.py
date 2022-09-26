"""Airflow specific constants"""
import json

from decouple import config

# These variables *need* to be set.
CLICKHOUSE_URI = config("CLICKHOUSE_URI")
CLICKHOUSE_USER = config("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = config("CLICKHOUSE_PASSWORD")
CLICKHOUSE_URIS = json.loads(config("CLICKHOUSE_URIS"))
