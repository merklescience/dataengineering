import datetime as dt
import json
import logging

import jinja2
import requests
from google.cloud import bigquery

from dataengineering.clickhouse.v1.bash_hook import ClickHouseBashHook


def make_ch_request(query: str, ch_conn_id: str, exception_msg: str) -> dict:
    """
    Make a 'lightweight' clickhouse query and return the data as dict, uses requests library.
    :param query: sql query to fetch data
    :type query: str
    :param ch_conn_id: clickhouse connection id
    :type ch_conn_id: str
    :param exception_msg: exception message to raise in case query execution results in response code other than 200
    :type exception_msg:
    :return: query result
    :rtype: dict
    """
    host, conn_details = ClickHouseBashHook(
        clickhouse_conn_id=ch_conn_id
    ).get_credentials()
    if not host.startswith("http"):
        host = f"http://{host}"
    resp = requests.get(
        f"{host}:{conn_details['port']}/{conn_details['database']}",
        params={
            "query": query,
            "user": conn_details["user"],
            "password": conn_details["password"],
        },
    )
    if resp.status_code != 200:
        raise Exception(exception_msg)
    data = json.loads(resp.content.decode()).get("data")[0]
    return data


def get_latest_block_bt(
    check_db_table: str,
    bt_ch_conn_id: str,
    last_sync_block_date: str,
    blockchain_id: int,
) -> int:
    """
    Get the latest block from bitquery cluster.
    :param check_db_table: bitquery table to check for latest block
    :type check_db_table: str
    :param bt_ch_conn_id: bitquery clickhouse connection id
    :type bt_ch_conn_id: str
    :param last_sync_block_date: last synced block date in 'YYYY-MM-DD' format
    :type last_sync_block_date: str
    :param blockchain_id: blockchain id of the chain, needed to filter data within check_db_table
    :type blockchain_id: int
    :return: latest block number
    :rtype: int
    """
    query = (
        f"SELECT max(block) as block from {check_db_table} WHERE tx_date>=toDate('{last_sync_block_date}') "
        f"AND blockchain_id={blockchain_id} FORMAT JSON"
    )
    data = make_ch_request(
        query=query,
        ch_conn_id=bt_ch_conn_id,
        exception_msg="Latest block number check on BT CH failed",
    )
    return int(data["block"])


def get_latest_block_date_bt(
    check_db_table: str,
    bt_ch_conn_id: str,
    last_sync_block_date: str,
    latest_block: int,
    blockchain_id: int,
) -> str:
    """
    Given the latest block number, this fetches the block date time for it. This helps in making optimised queries to
    bitquery clickhouse
    :param check_db_table: bitquery table to check for latest block
    :type check_db_table: str
    :param bt_ch_conn_id: bitquery clickhouse connection id
    :type bt_ch_conn_id: str
    :param last_sync_block_date: last synced block date in 'YYYY-MM-DD' format
    :type last_sync_block_date: str
    :param latest_block: latest block number
    :type latest_block: int
    :param blockchain_id: blockchain id of the chain, needed to filter data within check_db_table
    :type blockchain_id: int
    :return: block date time for latest block number in 'YYYY-MM-DD' format
    :rtype: str
    """
    query = (
        f"SELECT max(tx_date) as block_date from {check_db_table}"
        f" WHERE tx_date>=toDate('{last_sync_block_date}') AND block={latest_block} AND "
        f"blockchain_id={blockchain_id} FORMAT JSON"
    )
    data = make_ch_request(
        query=query,
        ch_conn_id=bt_ch_conn_id,
        exception_msg="Latest block date check on BT CH failed",
    )
    return data["block_date"]


def get_latest_block_ms(
    chain: str, ms_ch_conn_id: str, last_sync_block_date: str
) -> (int, str):
    """
    Get the latest block in the merkle science clickhouse. This becomes last_synced_block for next streaming run.
    Note : this runs 2 queries instead of 1 with 2 max() clauses, as the 2 max can talk about 2 different blocks
    :param chain: blockchain to check
    :type chain: str
    :param ms_ch_conn_id: merkle science clickhouse connection id
    :type ms_ch_conn_id: str
    :param last_sync_block_date: last synced block's date for efficient query in 'YYYY-MM-DD' format
    :type last_sync_block_date: str
    :return: latest block number and its block date time
    :rtype: int,str
    """
    block_query = (
        f"SELECT toDate(max(block_date_time)) as block_date,max(block) as block from {chain}.master"
        f" WHERE toDate(block_date_time)>=toDate('{last_sync_block_date}') FORMAT JSON"
    )
    block_data = make_ch_request(
        query=block_query,
        ch_conn_id=ms_ch_conn_id,
        exception_msg="Latest block number check on MS CH failed",
    )
    block_date_query = (
        f"SELECT toDate(max(block_date_time)) as block_date from {chain}.master"
        f" WHERE toDate(block_date_time)>=toDate('{last_sync_block_date}') "
        f"AND block={block_data['block']} FORMAT JSON"
    )
    block_date_data = make_ch_request(
        query=block_date_query,
        ch_conn_id=ms_ch_conn_id,
        exception_msg="Latest block date check on MS CH failed",
    )
    return int(block_data["block"]), block_date_data["block_date"]


def get_latest_block_tg(chain: str, tg_ip: str) -> (int, str):
    """
    Get latest block from tigergraph
    :param tg_ip: tigergraph ip
    :type tg_ip: str
    :param chain: chain
    :type chain: str
    :return:
    :rtype:
    """
    resp = requests.get(f"http://{tg_ip}:9000/query/{chain}/get_chainstate")
    if resp.status_code == 200:
        data = resp.json()["results"][0]["@@chainstate"]
        return data["latest_block_number"], dt.datetime.fromtimestamp(
            data["latest_block_date_time"]
        ).strftime("%Y-%m-%d")
    else:
        raise Exception(
            f"Received status code {resp.status_code} with text {resp.text}"
        )


def get_variable_name(chain: str, var_prefix: str, database: str = "clickhouse") -> str:
    assert database in ["clickhouse", "tigergraph"]
    var_start = f"{chain}_{var_prefix}" if var_prefix != "" else f"{chain}"
    if database == "clickhouse":
        var_name = f"{var_start}_streaming_last_synced_block"
    else:
        var_name = f"{var_start}_tg_streaming_last_synced_block"
    return var_name


def get_synced_status(
    chain: str, var_prefix: str = "", database: str = "clickhouse"
) -> dict:
    """
    Get the current sync status from airflow variable
    :param var_prefix: variable prefix for distinguishing multiple dags of same chain on diff end destination
    :type var_prefix:
    :param chain: blockchain
    :type chain: str
    :param database: sync status for database, can be either tigergraph or clickhouse
    :type database: str
    :return: dictionary with the keys last_synced_block,last_synced_block_date,latest_block,latest_block_date
    :rtype: dict
    """
    from airflow.models import Variable

    var_name = get_variable_name(chain=chain, database=database, var_prefix=var_prefix)
    sync_status = Variable.get(var_name, deserialize_json=True)
    if sync_status is None:
        raise Exception(f"{var_name} is not found in variables")
    return sync_status


def check_sync_status(
    chain: str,
    bt_ch_conn_id: str,
    streaming_lag: int,
    blockchain_id: int,
    check_db_table: str,
    batch_size: int = 0,
    var_prefix: str = "",
    database: str = "clickhouse",
    *args,
    **kwargs,
) -> bool:
    """
    This evaluates whether to sync i.e. stream data from bitquery clickhouse to merkle science database
    :param var_prefix: variable prefix for distinguishing multiple dags of same chain on diff end destination
    :type var_prefix:
    :param chain: blockchain
    :type chain: str
    :param database: sync status for database, can be either tigergraph or clickhouse
    :type database: str
    :param check_db_table: bitquery clickhouse table to check for latest block number
    :type check_db_table: str
    :param bt_ch_conn_id: bitquery clickhouse connection id
    :type bt_ch_conn_id: str
    :param blockchain_id: blockchain_id in bitquery
    :type blockchain_id: int
    :param streaming_lag: how much lag to consider with bitquery
    :type streaming_lag: int
    :param batch_size: batch size for 1 etl run
    :type batch_size: int
    :param args:
    :type args:
    :param kwargs:
    :type kwargs:
    :return: true or false for short circuit operator to proceed
    :rtype: bool
    """
    from airflow.models import Variable

    sync_status = get_synced_status(
        chain=chain, database=database, var_prefix=var_prefix
    )
    last_synced_block = int(sync_status["last_synced_block"])
    last_synced_block_date = sync_status["last_synced_block_date"]
    latest_block = get_latest_block_bt(
        check_db_table=check_db_table,
        bt_ch_conn_id=bt_ch_conn_id,
        last_sync_block_date=last_synced_block_date,
        blockchain_id=blockchain_id,
    )
    latest_block = latest_block - streaming_lag
    latest_block = min(last_synced_block + batch_size, latest_block)
    latest_block_date = get_latest_block_date_bt(
        check_db_table=check_db_table,
        bt_ch_conn_id=bt_ch_conn_id,
        latest_block=latest_block,
        last_sync_block_date=last_synced_block_date,
        blockchain_id=blockchain_id,
    )
    logging.info(
        f"Last synced block : {last_synced_block} and block date :{last_synced_block_date}\n"
        f"Latest block :{latest_block} and block date : {latest_block_date}"
    )
    if latest_block > last_synced_block:
        var_name = get_variable_name(
            chain=chain, database=database, var_prefix=var_prefix
        )
        Variable.set(
            key=var_name,
            serialize_json=True,
            value={
                "latest_block": latest_block,
                "latest_block_date": latest_block_date,
                "last_synced_block": last_synced_block,
                "last_synced_block_date": last_synced_block_date,
            },
        )
    return latest_block > last_synced_block


def set_latest_block(
    chain: str,
    ms_ch_conn_id: str = None,
    database: str = "clickhouse",
    tg_ip: str = None,
    var_prefix: str = "",
    *args,
    **kwargs,
) -> None:
    """
    Update the last_synced_block in the variable for maintaining streaming state
    :param var_prefix: variable prefix for distinguishing multiple dags of same chain on diff end destination
    :type var_prefix:
    :param tg_ip: tigergraph ip
    :type tg_ip: str
    :param chain: blockchain
    :type chain: str
    :param database: sync status for database, can be either tigergraph or clickhouse
    :type database: str
    :param ms_ch_conn_id: merkle science clickhouse connection id
    :type ms_ch_conn_id: str
    :param args:
    :type args:
    :param kwargs:
    :type kwargs:
    :return: None
    :rtype: None
    """
    from airflow.models import Variable

    assert not (
        (database == "tigergraph") and (tg_ip is None)
    ), f"tg ip can't be none when database is {database}"
    assert not (
        (database == "clickhouse") and (ms_ch_conn_id is None)
    ), f"ms_ch_conn_id can't be none when database is {database}"
    sync_status = get_synced_status(
        chain=chain, database=database, var_prefix=var_prefix
    )
    latest_block = int(sync_status["latest_block"])
    latest_block_date = sync_status["latest_block_date"]
    last_synced_block_date = sync_status["last_synced_block_date"]
    if database == "clickhouse":
        latest_ms_block, latest_ms_block_date = get_latest_block_ms(
            chain=chain,
            ms_ch_conn_id=ms_ch_conn_id,
            last_sync_block_date=last_synced_block_date,
        )

    else:
        latest_ms_block, latest_ms_block_date = get_latest_block_tg(
            chain=chain, tg_ip=tg_ip
        )
    var_name = get_variable_name(chain=chain, database=database, var_prefix=var_prefix)
    Variable.set(
        key=var_name,
        serialize_json=True,
        value={
            "latest_block": latest_block,
            "latest_block_date": latest_block_date,
            "last_synced_block": latest_ms_block,
            "last_synced_block_date": latest_ms_block_date,
        },
    )


def validate_bt_bq_counts(
    chain: str,
    ch_conn_id: str,
    ch_check_query: str,
    bq_table: str = "raw_tld",
    bq_project: str = "intelligence-team",
    *args,
    **kwargs,
) -> None:
    """
    This check the row number count between bigquery table and bitquery clickhouse for the data inserted in current
     daily etl run. Raises error if counts don't match exactly.
    :param bq_table: bigquery table to check counts for
    :type bq_table: str
    :param chain: blockchain
    :type chain: str
    :param ch_conn_id: bitquery clickhouse connection id
    :type ch_conn_id: str
    :param ch_check_query: clickhouse count check query
    :type ch_check_query: str
    :param bq_project: passes the name of bq project to access
    :type bq_project: str
    :param add_block_timestamp: Flag to add block_timestamp in the bq_query or not
    :type add_block_timestamp: bool
    :param args:
    :type args:
    :param kwargs:
    :type kwargs:
    :return: None
    :rtype: None
    """
    # This if condition is because few tables don't have block_timestamp and the task to validate is failing
    for argument in args:
        logging.info(f"arguments: {argument}")
    for k, v in kwargs.items():
        logging.info(f"kwargs: {k} ----- {v}")
    condition_variable = kwargs.get('add_block_timestamp')
    logging.info(f"condition variable: {condition_variable}")
    if condition_variable:
        bq_query = (
            f"SELECT DATE(block_timestamp) as dt,count(*) as bq_no_of_txns "
            f"FROM `{bq_project}.crypto_{chain}.{bq_table}` "
            f"WHERE DATE(block_timestamp) = '{kwargs.get('ds')}' GROUP BY dt"
        )
    else:
        bq_query = (
            f"SELECT count(*) as bq_no_of_txns "
            f"FROM `{bq_project}.crypto_{chain}.{bq_table}` "
        )
    logging.info(f"BQ QUERY ---- {bq_query}-------")
    client = bigquery.Client()
    bq_result = list(client.query(bq_query).result())
    bq_count = 0
    if bq_result is not None:
        if len(bq_result) > 1:
            raise Exception("More than 1 result from BQ while count check")
        bq_count = bq_result[0].get("bq_no_of_txns")
    data = make_ch_request(
        query=jinja2.Template(ch_check_query).render(ds=kwargs.get("ds")),
        ch_conn_id=ch_conn_id,
        exception_msg="Count check on BT CH failed",
    )
    if data["total_count"] is None:
        raise Exception("Received none from BT CH in count check")
    logging.info(f"BT CH count:{data['total_count']}")
    logging.info(f"BQ count:{bq_count}")
    if int(data["total_count"]) != int(bq_count):
        raise Exception("BQ and BT CH count check failed")
