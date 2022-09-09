import csv
import logging
import os

import jinja2
import numpy as np
import pandas as pd
from airflow.models import BaseOperator
from dataengineering.airflow.bitquery import get_synced_status
from coinprice.utils import get_latest_token_prices, get_tokens_metadata
from dataengineering.clickhouse.v1.bash_hook import ClickHouseBashHook
from dataengineering.tigergraph.v1.utils import form_tg_loading_request, tg_post_request



class TGBTStreamingOperator(BaseOperator):
    """
    This operator is used for streaming data from bitquery clickhouse onto merkle science tigergraph with
    transformations being done via pandas.
    execute function is invoked when operator is invoked/run
    """

    def __init__(
        self,
        chain: str,
        sql: str,
        bt_ch_conn_id: str,
        ms_ch_conn_id: str,
        tg_ip: str,
        var_prefix: str = "",
        *args,
        **kwargs,
    ):
        """
        :param chain: blockchain
        :type chain: str
        :param sql: sql for fetching data from bitquery
        :type sql: str
        :param bt_ch_conn_id: bitquery clickhouse connection id
        :type bt_ch_conn_id: str
        :param ms_ch_conn_id: merkle science clickhouse connection id
        :type ms_ch_conn_id: str
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
        self.tg_ip = tg_ip
        self.var_prefix = var_prefix

    def execute(self, context: dict) -> None:
        block_data = get_synced_status(
            chain=self.chain, database="tigergraph", var_prefix=self.var_prefix
        )
        sql = jinja2.Template(self.sql).render(
            last_synced_block=block_data["last_synced_block"],
            latest_block=block_data["latest_block"],
            last_synced_block_date=block_data["last_synced_block_date"],
            latest_block_date=block_data["latest_block_date"],
        )
        filename = f"{context.get('task_instance').dag_id}_{block_data['last_synced_block']}.csv"
        if os.path.isfile(filename):
            os.remove(filename)
        self.bt_ch_hook.get_results_file(
            sql=sql, filename=filename, file_format="csv", **context
        )
        df = pd.read_csv(filename)
        logging.info(f"Number of rows {len(df)}")
        if len(df) == 0:
            return None
        metadata = get_tokens_metadata(chain=self.chain, ms_ch_hook=self.ms_ch_hook)
        token_prices = get_latest_token_prices(
            symbols=list(metadata["symbol"].unique())
        )
        metadata = pd.merge(metadata, token_prices, on="symbol", how="left")
        metadata["token_address"] = metadata["token_address"].astype(str)
        df["token_address"] = df["token_address"].astype(str)
        df = pd.merge(
            df,
            metadata[["token_address", "coin_price_usd", "decimals"]],
            on="token_address",
            how="inner",
        )
        df.fillna(0, inplace=True)
        df["coin_value"] = df["coin_value"].astype(float)
        df.loc[df["type"].isin([0, 1]), "decimals"] = 0
        df["coin_value"] = df["coin_value"] / (10 ** df["decimals"])
        df["coin_value_usd"] = df["coin_value"] * df["coin_price_usd"]
        df["fee_usd"] = df["fee"] * df["coin_price_usd"]
        df.drop(columns=["decimals"], inplace=True)

        tg_batch_size = 10000

        # streaming_links = df[["transaction_id", "sender_address", "receiver_address",
        #                       "outgoing_value", "incoming_value", "outgoing_value_usd",
        #                       "incoming_value_usd", "fee", "fee_usd", "block_date_time"]]
        # for i in range(0, streaming_links.shape[0], tg_batch_size):
        #     tg_post_request(
        #         tg_request=form_tg_loading_request(tg_ip=self.tg_ip, chain=self.chain,
        #                                            loading_job=loading_map["streaming_links"]["loading_job"]),
        #         data=streaming_links[i:i + tg_batch_size].to_csv(quoting=csv.QUOTE_NONNUMERIC, index=False,
        #                                                          header=False),
        #         statistic=loading_map["streaming_links"]["stats"]
        #     )
        # del streaming_links

        logging.info("Running transactions loading job")
        transactions = df.groupby("transaction_id").apply(transactions_agg)
        for i in range(0, transactions.shape[0], tg_batch_size):
            tg_post_request(
                tg_request=form_tg_loading_request(
                    tg_ip=self.tg_ip,
                    chain=self.chain,
                    loading_job=loading_map["transactions"]["loading_job"],
                ),
                data=transactions[i : i + tg_batch_size].to_csv(
                    quoting=csv.QUOTE_NONNUMERIC
                ),
                statistic=loading_map["transactions"]["stats"],
            )
        del transactions
        logging.info("Running load inputs loading job")
        link_inputs = df.groupby(["transaction_id", "sender_address"]).apply(
            link_inputs_agg
        )
        for i in range(0, link_inputs.shape[0], tg_batch_size):
            tg_post_request(
                tg_request=form_tg_loading_request(
                    tg_ip=self.tg_ip,
                    chain=self.chain,
                    loading_job=loading_map["link_inputs"]["loading_job"],
                ),
                data=link_inputs[i : i + tg_batch_size].to_csv(
                    quoting=csv.QUOTE_NONNUMERIC
                ),
                statistic=loading_map["link_inputs"]["stats"],
            )
        del link_inputs
        logging.info("Running link outputs loading job")
        link_outputs = df.groupby(["transaction_id", "receiver_address"]).apply(
            link_outputs_agg
        )
        for i in range(0, link_outputs.shape[0], tg_batch_size):
            tg_post_request(
                tg_request=form_tg_loading_request(
                    tg_ip=self.tg_ip,
                    chain=self.chain,
                    loading_job=loading_map["link_outputs"]["loading_job"],
                ),
                data=link_outputs[i : i + tg_batch_size].to_csv(
                    quoting=csv.QUOTE_NONNUMERIC
                ),
                statistic=loading_map["link_outputs"]["stats"],
            )
        del link_outputs
        logging.info("Running chain state loading job")
        df["chain"] = self.chain
        chain_state = df.pivot_table(
            index="chain",
            values=["coin_price_usd", "block", "block_date_time"],
            aggfunc=np.max,
        ).reset_index()
        chain_state.rename(
            columns={"block": "block_number", "coin_price_usd": "price_usd"},
            inplace=True,
        )
        chain_state = chain_state[
            ["chain", "price_usd", "block_date_time", "block_number"]
        ]
        tg_post_request(
            tg_request=form_tg_loading_request(
                tg_ip=self.tg_ip,
                chain=self.chain,
                loading_job=loading_map["chain_state"]["loading_job"],
            ),
            data=chain_state.to_csv(
                quoting=csv.QUOTE_NONNUMERIC, index=False, header=False
            ),
            statistic=loading_map["chain_state"]["stats"],
        )
        del chain_state
        os.remove(filename)


class RippleTGBTStreamingOperator(BaseOperator):
    """
    This operator is used for streaming data from bitquery clickhouse onto merkle science tigergraph with
    transformations being done via pandas.
    execute function is invoked when operator is invoked/run
    """

    def __init__(
        self,
        chain: str,
        sql: str,
        bt_ch_conn_id: str,
        ms_ch_conn_id: str,
        tg_ip: str,
        var_prefix: str = "",
        *args,
        **kwargs,
    ):
        """
        :param chain: blockchain
        :type chain: str
        :param sql: sql for fetching data from bitquery
        :type sql: str
        :param bt_ch_conn_id: bitquery clickhouse connection id
        :type bt_ch_conn_id: str
        :param ms_ch_conn_id: merkle science clickhouse connection id
        :type ms_ch_conn_id: str
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
        self.tg_ip = tg_ip
        self.var_prefix = var_prefix

    def execute(self, context: dict) -> None:
        block_data = get_synced_status(
            chain=self.chain, database="tigergraph", var_prefix=self.var_prefix
        )
        sql = jinja2.Template(self.sql).render(
            last_synced_block=block_data["last_synced_block"],
            latest_block=block_data["latest_block"],
            last_synced_block_date=block_data["last_synced_block_date"],
            latest_block_date=block_data["latest_block_date"],
        )
        filename = f"{context.get('task_instance').dag_id}_{block_data['last_synced_block']}.csv"
        if os.path.isfile(filename):
            os.remove(filename)
        self.bt_ch_hook.get_results_file(
            sql=sql, filename=filename, file_format="csv", **context
        )
        df = pd.read_csv(filename)
        logging.info(f"Number of rows {len(df)}")
        if len(df) == 0:
            return None
        token_prices = get_latest_token_prices(symbols=["XRP"])
        token_prices["token_address"] = "0x0000"
        df["token_address"] = "0x0000"
        df = pd.merge(
            df,
            token_prices[["token_address", "coin_price_usd"]],
            on="token_address",
            how="inner",
        )
        df["outgoing_value"] = df["coin_value"] + df["fee"]
        df["incoming_value"] = df["coin_value"]
        df["outgoing_value_usd"] = df["outgoing_value"] * df["coin_price_usd"]
        df["incoming_value_usd"] = df["incoming_value"] * df["coin_price_usd"]
        df["fee_usd"] = df["fee"] * df["coin_price_usd"]

        tg_batch_size = 10000

        streaming_links = df[
            [
                "transaction_id",
                "sender_address",
                "receiver_address",
                "outgoing_value",
                "incoming_value",
                "outgoing_value_usd",
                "incoming_value_usd",
                "fee",
                "fee_usd",
                "block_date_time",
            ]
        ]
        for i in range(0, streaming_links.shape[0], tg_batch_size):
            tg_post_request(
                tg_request=form_tg_loading_request(
                    tg_ip=self.tg_ip,
                    chain=self.chain,
                    loading_job=loading_map["streaming_links"]["loading_job"],
                ),
                data=streaming_links[i : i + tg_batch_size].to_csv(
                    quoting=csv.QUOTE_NONNUMERIC, index=False, header=False
                ),
                statistic=loading_map["streaming_links"]["stats"],
            )
        del streaming_links
        df["chain"] = self.chain
        chain_state = df.pivot_table(
            index="chain",
            values=["coin_price_usd", "block", "block_date_time"],
            aggfunc=np.max,
        ).reset_index()
        chain_state.rename(
            columns={"block": "block_number", "coin_price_usd": "price_usd"},
            inplace=True,
        )
        chain_state = chain_state[
            ["chain", "price_usd", "block_date_time", "block_number"]
        ]
        tg_post_request(
            tg_request=form_tg_loading_request(
                tg_ip=self.tg_ip,
                chain=self.chain,
                loading_job=loading_map["chain_state"]["loading_job"],
            ),
            data=chain_state.to_csv(
                quoting=csv.QUOTE_NONNUMERIC, index=False, header=False
            ),
            statistic=loading_map["chain_state"]["stats"],
        )
        del chain_state
        os.remove(filename)


def transactions_agg(x):
    """
    Aggregation logic for daily_transactions loading job on tigergraph.
    """
    aggregate = {
        "external_value": x[x["type"] == 0]["coin_value"].sum(),
        "external_value_usd": x[x["type"] == 0]["coin_value_usd"].sum(),
        "block_date": x["block_date_time"].max(),
        "txn_fee": x["fee"].sum(),
        "txn_fee_usd": x["fee_usd"].sum(),
        "internal_value": x[x["type"] == 1]["coin_value"].sum(),
        "internal_value_usd": x[x["type"] == 1]["coin_value_usd"].sum(),
        "token_transfer_usd": x[x["type"] == 2]["coin_value_usd"].sum(),
    }
    return pd.Series(aggregate)


def link_inputs_agg(x):
    """
    Aggregation logic for daily_link_inputs loading job on tigergraph.
    """
    aggregate = {
        "value": (
            x[x["type"].isin([0, 1])]["coin_value"] + x[x["type"].isin([0, 1])]["fee"]
        ).sum(),
        "value_usd": (x["coin_value_usd"] + x["fee_usd"]).sum(),
    }
    return pd.Series(aggregate)


def link_outputs_agg(x):
    """
    Aggregation logic for daily_link_outputs loading job on tigergraph.
    """
    aggregate = {
        "value": x[x["type"].isin([0, 1])]["coin_value"].sum(),
        "value_usd": x["coin_value_usd"].sum(),
    }
    return pd.Series(aggregate)
