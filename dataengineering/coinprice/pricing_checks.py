# TODO: This module has portions that need airflow. This can be abstracted away
# and made so that it works without it, and those functions can then be wrapped
# with airflow specific logic. Keeping even airflow-dependent items here will
# limit how this module can be used.
import pandas as pd
from google.cloud import bigquery

# TODO: update these to use `dataengineering.clickhouse.connector.ClickhouseConnector.execute`
# instead, for improved testability.
from dataengineering.clickhouse.v1.requests import execute_sql

# smart_contract_chains = ["ethereum", "bsc", "matic", "tron", "zilliqa", "cardano"]
# TODO: Use the implementation of whether or not a chain is smart contract chain from
# `dataengineering.chains`
smart_contract_chains = {
    "ethereum": "ETH",
    "bsc": "BNB",
    "matic": "MATIC",
    "tron": "TRX",
    "zilliqa": "ZIL",
    "cardano": "ADA",
    "stellar": "XLM",
}
non_smart_contract_chains_map = {
    "bitcoin": "BTC",
    "dogecoin": "DOGE",
    "bitcoin_cash": "BCH",
    "bitcoin_sv": "BSV",
    "litecoin": "LTC",
    "ripple": "XRP",
}


def get_tokens(chain: str, creds: dict) -> set:
    if chain in smart_contract_chains:
        if ord(chain[0]) < ord("s"):
            # We do this because on clickhouse restart the tables are loaded
            # alphabetically and sharded tables of chains beyond alphabet "s"
            # will sharded table which have dependency on tokens metadata which
            # will fall after alphabet "s" that is why we make these
            # dictionaries in "aal_dictionaries" database
            sql = f"select distinct(symbol) as symbol from {chain}.tokens_metadata FORMAT JSONCompactStrings"
        else:
            sql = (
                f"select distinct(symbol) as symbol from "
                f"aal_dictionaries.{chain}_tokens_metadata FORMAT JSONCompactStrings"
            )
        tokens = execute_sql(sql=sql, ch_conn=creds, raw_response=True)
        tokens = {i[0] for i in tokens.json()["data"]}
    else:
        tokens = {non_smart_contract_chains_map[chain]}
    return tokens


def get_prices_clickhouse(
    creds: dict, check_date: str, tokens: str, price_table: str, dictionary_name: str
) -> (pd.DataFrame, set):
    optimise_pricing_table(creds=creds, price_table=price_table)
    reload_dictionaries(dictionary_name=dictionary_name, creds=creds)
    sql = f"select * from {price_table} where symbol in ({tokens}) and day='{check_date}' FORMAT JSON"
    prices = execute_sql(sql=sql, ch_conn=creds, raw_response=True)
    prices = pd.DataFrame(prices.json()["data"])
    return prices, set(prices["symbol"].unique())


def get_prices_bigquery(check_date: str, tokens: str) -> (pd.DataFrame, set):
    client = bigquery.Client()
    sql = (
        f"select price_usd as price,symbol from intelligence-team.mint_prices.unique_prices where "
        f"symbol in ({tokens}) and timestamp='{check_date}'"
    )
    prices = client.query(sql).result().to_dataframe()
    return prices, set(prices["symbol"].unique())


def check_prices(
    chain: str,
    check_date: str,
    check_db: str,
    ms_conn_ch_id: str = None,
    ch_price_table: str = "mint_prices.coin_price_usd",
    ch_dictionary_name: str = None,
    *args,
    **kwargs,
):
    from airflow.models import Variable

    # TODO: Instead of AssertionErrors, it's better design to use a custom error, to
    # specifically alert what the error is. Makes debugging easier.
    assert not (check_db == "clickhouse" and ms_conn_ch_id is None), (
        "ms_conn_ch_id cannot be None when " "check_db=clickhouse"
    )
    assert not (check_db == "clickhouse" and ch_dictionary_name is None), (
        "ch_dictionary_name cannot be None when " "check_db=clickhouse"
    )
    creds = Variable.get(ms_conn_ch_id, deserialize_json=True)
    tokens = get_tokens(chain=chain, creds=creds)
    if check_db == "clickhouse":
        prices, token_prices = get_prices_clickhouse(
            creds=creds,
            check_date=check_date,
            tokens="'" + "','".join(tokens) + "'",
            price_table=ch_price_table,
            dictionary_name=ch_dictionary_name,
        )
    else:
        prices, token_prices = get_prices_bigquery(
            check_date=check_date, tokens="'" + "','".join(tokens) + "'"
        )
    zero_price = list(prices[prices["price"] <= 0]["symbol"])
    assert (
        smart_contract_chains[chain] not in zero_price
    ), f"Native token {smart_contract_chains[chain]} has 0 price"
    assert len(zero_price) < 0.5 * len(tokens), (
        f"These tokens have 0 prices {zero_price} "
        f"and they are more than 50% of tokens supported on {chain}"
    )
    assert (
        len(tokens - token_prices) == 0
    ), f"These tokens are missing in {check_db} {tokens - token_prices}"


def optimise_pricing_table(
    creds: dict, price_table: str = "mint_prices.coin_price_usd"
):
    sql = f"OPTIMIZE TABLE {price_table} FINAL DEDUPLICATE"
    execute_sql(sql=sql, ch_conn=creds, raw_response=True)


def reload_dictionaries(dictionary_name: str, creds: dict):
    sql = f"SYSTEM RELOAD DICTIONARY {dictionary_name}"
    execute_sql(sql=sql, ch_conn=creds, raw_response=True)


def pricing_check_tasks(
    chain: str, ms_ch_conn_id: str, ch_price_table: str, ch_dictionary_name: str
) -> list:
    from airflow.operators.python_operator import PythonOperator

    run_date = "{{ds}}"
    check_prices_bq = PythonOperator(
        task_id="check_prices_bq",
        python_callable=check_prices,
        op_kwargs={
            "chain": chain,
            "check_date": run_date,
            "check_db": "bigquery",
            "ms_conn_ch_id": ms_ch_conn_id,
        },
    )
    check_prices_bq.doc_md = (
        f"This checks whether prices are available on bigquery for supported tokens of "
        f"{chain}, fails if some token is not present or price 0 for any token"
    )

    check_prices_ch = PythonOperator(
        task_id="check_prices_ch",
        python_callable=check_prices,
        op_kwargs={
            "chain": chain,
            "check_date": run_date,
            "check_db": "clickhouse",
            "ms_conn_ch_id": ms_ch_conn_id,
            "ch_price_table": ch_price_table,
            "ch_dictionary_name": ch_dictionary_name,
        },
    )
    check_prices_ch.doc_md = (
        f"This checks whether prices are available on clickhouse for supported "
        f"tokens of {chain}, fails if some token is not present or price 0 for any token"
    )
    return [check_prices_bq, check_prices_ch]
