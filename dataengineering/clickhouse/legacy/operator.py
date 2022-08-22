import logging
import os
from typing import Optional, Dict, Any
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator, Variable

from blockchain.bitquery.common_utils import get_synced_status
from dataengineering.coinprice.utils import get_latest_token_prices
from dataengineering.clickhouse.legacy.bash_hook import ClickHouseBashHook
import jinja2
import pandas as pd
import requests
import json


def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        if x != 'TB':
            num /= 1024.0
        elif x == 'TB' and num > 1024:
            return "%3.1f %s" % (num, x)


def get_file_size(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)


def upload_to_gcs(bucket, object_name, filename):
    cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")
    logging.info("Uploading " + os.path.abspath(filename) + ", the size of file is " + get_file_size(filename))
    cloud_storage_hook.upload(bucket=bucket, object=object_name, filename=filename)


class ClickhouseGCSOperator(BaseOperator):
    """
    This operator fetches data from Clickhouse and saves the result file in Google Cloud Storage.
    execute function is invoked when operator is invoked/run
    """
    # The following fields are automatically rendered via airflow if they use airflow macro variables
    template_fields = ["local_filename", "sql", "sql_variables", "gcs_path", "local_filepath", "gcs_filename"]

    def __init__(self, local_filename: str, gcs_bucket: str, gcs_path: str, sql: str, clickhouse_conn_id: str,
                 gcs_filename: str = None, sql_variables: dict = None, local_filepath: str = None,
                 file_format: str = "csv", *args, **kwargs):
        """
        :param local_filename: local_filename, you might want to keep it a combination of dag_id and other stuff
        :type local_filename: str
        :param gcs_bucket: bucket to save the data
        :type gcs_bucket:str
        :param gcs_path: gcs path inside the bucket
        :type gcs_path:str
        :param sql: sql query for fetching data
        :type sql:str
        :param clickhouse_conn_id: clickhouse connection id to fetch data from
        :type clickhouse_conn_id:str
        :param gcs_filename: name of file in gcs
        :type gcs_filename:str
        :param sql_variables: render dictionary for sql in case it is templatised
        :type sql_variables:str
        :param local_filepath: in case you want to store local file in a specific directory
        :type local_filepath:str
        :param file_format: format of file being saved/fetched
        :type file_format: str
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.local_filename = local_filename
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.sql = sql
        self.ch_hook = ClickHouseBashHook(clickhouse_conn_id=clickhouse_conn_id)
        self.local_filepath = local_filepath
        self.file_format = file_format
        if sql_variables is not None:
            self.sql = jinja2.Template(self.sql).render(sql_variables)
        self.sql_variables = sql_variables
        self.gcs_filename = gcs_filename if gcs_filename is not None else local_filename

    def execute(self, context: Dict[str, Any] = None) -> None:
        filepath = os.path.join(self.local_filepath,
                                self.local_filename) if self.local_filepath else self.local_filename
        filepath = filepath + f'.{self.file_format}'
        if os.path.isfile(filepath):
            os.remove(filepath)
        self.ch_hook.get_results_file(sql=self.sql, filename=filepath, file_format=self.file_format, **context)
        logging.info("Got file from BT CH, uploading to GCS")
        upload_to_gcs(bucket=self.gcs_bucket, object_name=self.gcs_path + self.gcs_filename + f'.{self.file_format}',
                      filename=filepath)
        os.remove(filepath)


class ClickhouseGCStoCHOperator(BaseOperator):
    """
    This operator fetches single file on GCS and inserts them to Clickhouse.
    execute function is invoked when operator is invoked/run
    """
    # The following fields are automatically rendered via airflow if they use airflow macro variables
    template_fields = ["gcs_path", "gcs_filename", "table", "local_filename"]

    def __init__(self, gcs_bucket: str, gcs_path: str, database: str, table: str, clickhouse_conn_id: str,
                 local_filename: str, gcs_filename: str = None, file_format: str = "parquet", *args, **kwargs):
        """

        :param gcs_bucket: bucket where file is saved
        :type gcs_bucket: str
        :param gcs_path: path for file in the bucket
        :type gcs_path: str
        :param database: target database to insert file data
        :type database: str
        :param table: target table to insert file data
        :type table: str
        :param clickhouse_conn_id: target clickhouse connection id
        :type clickhouse_conn_id: str
        :param local_filename: local filename for file since it's downloaded from gcs before insert
        :type local_filename: str
        :param gcs_filename: name on file as on gcs
        :type gcs_filename: str
        :param file_format: format of file
        :type file_format: str
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.gcs_filename = gcs_filename
        self.local_filename = local_filename
        self.ch_hook = ClickHouseBashHook(clickhouse_conn_id=clickhouse_conn_id)
        self.database = database
        self.table = table
        self.file_format = file_format

    def execute(self, context: Dict[str, Any] = None) -> None:
        if os.path.isfile(self.local_filename):
            os.remove(self.local_filename)
        GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default") \
            .download(bucket=self.gcs_bucket, object=self.gcs_path + self.gcs_filename, filename=self.local_filename)
        self.ch_hook.run_insert_job(database=self.database, table=self.table, filename=self.local_filename,
                                    file_format=self.file_format, **context)
        os.remove(self.local_filename)


class ClickhouseLocalFiletoCHOperator(BaseOperator):
    """
    This operator loads local file to clickhouse.
    This would be useful when running airflow on a vm and filepath can be shared. Would not be helpful on composer as
    worker don't share the filesystem.
    execute function is invoked when operator is invoked/run
    """
    # The following fields are automatically rendered via airflow if they use airflow macro variables
    template_fields = ["table", "local_filename"]

    def __init__(self, database: str, table: str, clickhouse_conn_id: str, local_filename: str,
                 file_format: str = "parquet", *args, **kwargs):
        """

        :param database: target database to insert file data
        :type database: str
        :param table: target table to insert file data
        :type table: str
        :param clickhouse_conn_id: target clickhouse connection id
        :type clickhouse_conn_id: str
        :param local_filename: local filename for file since it's downloaded from gcs before insert
        :type local_filename: str
        :param file_format: format of file
        :type file_format: str
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.local_filename = local_filename
        self.ch_hook = ClickHouseBashHook(clickhouse_conn_id=clickhouse_conn_id)
        self.database = database
        self.table = table
        self.file_format = file_format

    def execute(self, context: Dict[str, Any] = None) -> None:
        self.ch_hook.run_insert_job(database=self.database, table=self.table, filename=self.local_filename,
                                    file_format=self.file_format, **context)
        print(self.local_filename)
        os.remove(self.local_filename)
        print("Does local file exist: ", os.path.exists(self.local_filename))


class ClickhouseGCSFoldertoCHOperator(BaseOperator):
    """
    This operator take gcs folder as input and loads all the files from there to clickhouse. Useful when taking extract
    from big query which generally ends up with multiple files in a folder.
    execute function is invoked when operator is invoked/run
    """
    # The following fields are automatically rendered via airflow if they use airflow macro variables
    template_fields = ["gcs_path", "table"]

    def __init__(self, gcs_bucket: str, gcs_path: str, database: str, table: str, clickhouse_conn_id: str,
                 file_format: str = "parquet", *args, **kwargs):
        """

        :param gcs_bucket: bucket on gcs
        :type gcs_bucket: str
        :param gcs_path: path or folder which contains the multiple files
        :type gcs_path: str
        :param database: target database to insert file data
        :type database: str
        :param table: target table to insert file data
        :type table: str
        :param clickhouse_conn_id: target clickhouse connection id
        :type clickhouse_conn_id: str
        :param file_format:
        :type file_format:
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database
        self.table = table
        self.file_format = file_format

    def execute(self, context: Dict[str, Any] = None) -> None:
        ch_hook = ClickHouseBashHook(clickhouse_conn_id=self.clickhouse_conn_id)
        gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")
        dag_id = context.get('task_instance').dag_id
        task_id = context.get('task_instance').task_id
        files = gcs_hook.list(bucket=self.gcs_bucket, prefix=self.gcs_path)
        for file in files:
            filename = f"{dag_id}_{task_id}_{context.get('ds_nodash')}_{file}".replace(self.gcs_path, "")
            if os.path.isfile(filename):
                os.remove(filename)
            gcs_hook.download(bucket=self.gcs_bucket, object=file, filename=filename)
            ch_hook.run_insert_job(database=self.database, table=self.table, filename=filename,
                                   file_format=self.file_format, **context)
            os.remove(filename)


class ClickhouseExecuteOperator(BaseOperator):
    """
    This operator is meant to execute sql queries which don't end up in result files like DDLs and DMLs.
    execute function is invoked when operator is invoked/run
    """
    # The following fields are automatically rendered via airflow if they use airflow macro variables
    template_fields = ["sql"]

    def __init__(self, sql: str, clickhouse_conn_id: str, *args, **kwargs):
        """

        :param sql: sql query to execute
        :type sql: str
        :param clickhouse_conn_id: target clickhouse connection id
        :type clickhouse_conn_id: str
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.ch_hook = ClickHouseBashHook(clickhouse_conn_id=clickhouse_conn_id)

    def execute(self, context: Dict[str, Any] = None) -> None:
        self.ch_hook.execute_query(sql=self.sql, **context)


class ClickhouseBTStreamingOperator(BaseOperator):
    """
    This operator is used for streaming data from bitquery clickhouse onto merkle science clickhouse with
    transformations being done via pandas.
    execute function is invoked when operator is invoked/run
    """

    def __init__(self, chain: str, sql: str, log_index: bool, sort_columns: list or dict, bt_ch_conn_id: str,
                 ms_ch_conn_id: str, insert_tables: list = None, gcs_upload: bool = False,
                 log_index_column: str = 'transaction_id', *args, **kwargs):
        """
        :param chain: blockchain
        :type chain: str
        :param sql: sql for fetching data from bitquery
        :type sql: str
        :param log_index: whether data requires log_index calculation or not
        :type log_index: bool
        :param sort_columns: list of columns required for log_index calculation/sorting, when dict key is column name
        value is whether to sort is True for ascending and False for descending
        :type sort_columns: list or dict
        :param bt_ch_conn_id: bitquery clickhouse connection id
        :type bt_ch_conn_id: str
        :param ms_ch_conn_id: merkle science clickhouse connection id
        :type ms_ch_conn_id: str
        :param insert_tables: list of tables in which this data needs to be loaded on merkle science cluster
        :type insert_tables: list
        :param gcs_upload: (this is for future) whether to upload the processed file in gcs
        :type gcs_upload: bool
        :param log_index_column: column for grouping by in log_index calculation, this is usually transcation_hash
        :type log_index_column: str
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.bt_ch_hook = ClickHouseBashHook(clickhouse_conn_id=bt_ch_conn_id)
        self.ms_ch_hook = ClickHouseBashHook(clickhouse_conn_id=ms_ch_conn_id)
        self.chain = chain
        self.log_index = log_index
        self.sort_columns = sort_columns
        self.log_index_column = log_index_column
        self.gcs_upload = gcs_upload
        self.insert_tables = insert_tables

    def execute(self, context: dict) -> None:
        block_data = get_synced_status(chain=self.chain)
        sql = jinja2.Template(self.sql).render(last_synced_block=block_data["last_synced_block"],
                                               latest_block=block_data["latest_block"],
                                               last_synced_block_date=block_data["last_synced_block_date"],
                                               latest_block_date=block_data["latest_block_date"])
        filename = f"{context.get('task_instance').dag_id}_{block_data['last_synced_block']}.csv"
        if os.path.isfile(filename):
            os.remove(filename)
        self.bt_ch_hook.get_results_file(sql=sql, filename=filename, file_format="csv", **context)
        df = pd.read_csv(filename)
        if self.log_index:
            if len(self.sort_columns) == 0:
                raise Exception("No sort columns provided for log index")
            if isinstance(self.sort_columns, list):
                df.sort_values(self.sort_columns, inplace=True)
            if isinstance(self.sort_columns, dict):
                df.sort_values(list(self.sort_columns.keys()), ascending=list(self.sort_columns.values()), inplace=True)
            df['log_index'] = df.groupby(self.log_index_column)[self.log_index_column].rank(method='first').astype(int)
        metadata = self._get_tokens_metadata()
        token_prices = get_latest_token_prices(symbols=list(metadata['symbol'].unique()))
        metadata = pd.merge(metadata, token_prices, on="symbol", how="left")
        df = pd.merge(df, metadata[["token_address", "coin_price_usd", "decimals"]], on="token_address", how="inner")
        df["coin_value"] = df["coin_value"].astype(float)
        df.loc[df["type"].isin([0, 1]), "decimals"] = 0
        df["coin_value"] = df["coin_value"] / (10 ** df["decimals"])
        df.drop(columns=["decimals"], inplace=True)
        if self.chain == "tron":
            df.loc[df["log_index"] > 1, "fee"] = 0
        df.to_csv(filename, index=False)
        del df, metadata, token_prices
        if self.insert_tables is not None:
            for tab in self.insert_tables:
                self.ms_ch_hook.run_insert_job(database=self.chain, table=tab, filename=filename, file_format='csv',
                                               **context)
        os.remove(filename)

    def _get_tokens_metadata(self) -> pd.DataFrame:
        """
        This gets all the token metadata from {{chain}}.tokens_metadata from ms clickhouse, it is later used for
        getting prices from bifrost,filtering supported tokens and coin_value division with decimals.
        :return: pandas dataframe with symbol,token_address,decimals column
        :rtype: pd.DataFrame
        """
        tokens_metadata_table = f"{self.chain}.tokens_metadata" if ord(self.chain[0]) < ord("s") \
            else f"aal_dictionaries.{self.chain}_tokens_metadata"
        query = f"select symbol,address as token_address,decimals from {tokens_metadata_table} FORMAT JSON"
        host, conn_details = self.ms_ch_hook.get_credentials()
        resp = requests.get(f"http://{host}:{conn_details['port']}/{conn_details['database']}",
                            params={"query": query,
                                    "user": conn_details['user'],
                                    "password": conn_details['password']})
        if resp.status_code != 200:
            raise Exception(f"Status code {resp.status_code} received from MS CH instead of 200")

        data = json.loads(resp.content.decode())
        return pd.DataFrame(data["data"])


class RippleClickhouseBTStreamingOperator(BaseOperator):
    """
    This operator is used for streaming data from bitquery clickhouse onto merkle science clickhouse with
    transformations being done via pandas.
    execute function is invoked when operator is invoked/run
    """

    def __init__(self, chain: str, sql: str, bt_ch_conn_id: str, ms_ch_conn_id: str, insert_tables: list = None, *args,
                 **kwargs):
        """
        :param chain: blockchain
        :type chain: str
        :param sql: sql for fetching data from bitquery
        :type sql: str
        :param bt_ch_conn_id: bitquery clickhouse connection id
        :type bt_ch_conn_id: str
        :param ms_ch_conn_id: merkle science clickhouse connection id
        :type ms_ch_conn_id: str
        :param insert_tables: list of tables in which this data needs to be loaded on merkle science cluster
        :type insert_tables: list
        :param args: additional args sent via airflow context
        :type args:
        :param kwargs: additional kwargs sent via airflow context
        :type kwargs:
        """
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.bt_ch_hook = ClickHouseBashHook(clickhouse_conn_id=bt_ch_conn_id)
        self.ms_ch_hook = ClickHouseBashHook(clickhouse_conn_id=ms_ch_conn_id)
        self.chain = chain
        self.insert_tables = insert_tables

    def execute(self, context: dict) -> None:
        block_data = get_synced_status(chain=self.chain)
        sql = jinja2.Template(self.sql).render(last_synced_block=block_data["last_synced_block"],
                                               latest_block=block_data["latest_block"],
                                               last_synced_block_date=block_data["last_synced_block_date"],
                                               latest_block_date=block_data["latest_block_date"])
        filename = f"{context.get('task_instance').dag_id}_{block_data['last_synced_block']}.csv"
        if os.path.isfile(filename):
            os.remove(filename)
        self.bt_ch_hook.get_results_file(sql=sql, filename=filename, file_format="csv", **context)
        df = pd.read_csv(filename)
        token_prices = get_latest_token_prices(symbols=["XRP"])
        token_prices["token_address"] = "0x0000"
        df["token_address"] = "0x0000"
        df = pd.merge(df, token_prices[["token_address", "coin_price_usd"]], on="token_address", how="inner")
        df.drop(columns=['token_address'], inplace=True)
        df["coin_value"] = df["coin_value"].astype(float)
        df.to_csv(filename, index=False)
        del df, token_prices
        if self.insert_tables is not None:
            for tab in self.insert_tables:
                self.ms_ch_hook.run_insert_job(database=self.chain, table=tab, filename=filename, file_format='csv',
                                               **context)
        os.remove(filename)
