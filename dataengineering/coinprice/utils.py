import json
import time

import pandas as pd
import requests
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from dataengineering.clickhouse.v1.bash_hook import ClickHouseBashHook
from dataengineering.constants import CoinPriceEnv


def oid_to_str(df):
    col_list = df.select_dtypes(include="object").columns
    for col in col_list:
        df[col] = df[col].astype("str")
    return df.copy()


def get_latest_token_prices(symbols: list) -> pd.DataFrame:
    url = CoinPriceEnv.PRICING_SERVICE_URL + "/api/coins/prices"
    headers = {"X-Api-Key": CoinPriceEnv.PRICING_SERVICE_TOKEN}
    params = {"coin_symbols": ",".join(symbols)}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception("Non 200 status code received from Mint")
    prices = pd.DataFrame(response.json())
    prices.rename(
        columns={"coin_symbol": "symbol", "price": "coin_price_usd"}, inplace=True
    )
    prices["coin_price_usd"] = prices["coin_price_usd"].astype(float)
    return prices[["symbol", "coin_price_usd"]]


def get_tokens_metadata(
    chain: str, ms_ch_hook: ClickHouseBashHook = None, ms_ch_conn_id: str = None
) -> pd.DataFrame:
    """
    This gets all the token metadata from {{chain}}.tokens_metadata from ms clickhouse, it is later used for
    getting prices from bifrost,filtering supported tokens and coin_value division with decimals.
    :return: pandas dataframe with symbol,token_address,decimals column
    :rtype: pd.DataFrame
    """
    if ms_ch_hook is not None:
        host, conn_details = ms_ch_hook.get_credentials()
    elif ms_ch_conn_id is not None:
        host, conn_details = ClickHouseBashHook(
            clickhouse_conn_id=ms_ch_conn_id
        ).get_credentials()
    else:
        raise Exception(
            "Both ms_ch_hook & ms_ch_conn_id are passed None, at least one of them should be passed"
        )
    tokens_metadata_table = (
        f"{chain}.tokens_metadata"
        if ord(chain[0]) < ord("s")
        else f"aal_dictionaries.{chain}_tokens_metadata"
    )
    query = f"select symbol,address as token_address,decimals from {tokens_metadata_table} FORMAT JSON"
    resp = requests.get(
        f"http://{host}:{conn_details['port']}/{conn_details['database']}",
        params={
            "query": query,
            "user": conn_details["user"],
            "password": conn_details["password"],
        },
    )
    if resp.status_code != 200:
        raise Exception(
            f"Status code {resp.status_code} received from MS CH instead of 200"
        )
    data = json.loads(resp.content.decode())
    return pd.DataFrame(data["data"])


class GoogleSheets(object):
    def __init__(self, sheet_id):
        self.sheet_id = sheet_id
        self.service = discovery.build(
            "sheets", "v4", cache_discovery=False
        ).spreadsheets()
        self.sheet_properties = [
            sheet["properties"]
            for sheet in self.service.get(spreadsheetId=sheet_id)
            .execute()
            .get("sheets")
        ]

    def __str__(self):
        return json.dumps(self.sheet_properties)

    def get_data(self, gid):
        sheet_data = list(
            filter(lambda x: x["sheetId"] == int(gid), self.sheet_properties)
        )
        sheet_data = sheet_data[0]
        sheet_range = f"{sheet_data['title']}!A1:{self.column_string(sheet_data['gridProperties']['columnCount'])}"
        tries = 1
        while tries <= 5:
            try:
                data = (
                    self.service.values()
                    .get(spreadsheetId=self.sheet_id, range=sheet_range)
                    .execute()["values"]
                )
                break
            except HttpError as err:
                if err.resp.status in [429, 503, 500]:
                    tries += 1
                    time.sleep(5)
        if tries == 6:
            return False, pd.DataFrame()
        data = pd.DataFrame(data, columns=data[0])
        data = data.iloc[1:]
        time.sleep(2)
        return True, data

    def get_data_fixed_rows(self, gid, column_list, start_col, end_col, read_rows_from):
        sheet_data = list(
            filter(lambda x: x["sheetId"] == int(gid), self.sheet_properties)
        )
        sheet_data = sheet_data[0]
        if sheet_data["gridProperties"]["rowCount"] >= read_rows_from:
            sheet_range = f"{sheet_data['title']}!{start_col}{read_rows_from}:{end_col}"
            for i in range(0, 5):
                try:
                    data = (
                        self.service.values()
                        .get(spreadsheetId=self.sheet_id, range=sheet_range)
                        .execute()
                        .get("values")
                    )
                    break
                except HttpError as err:
                    if err.resp.status in [429]:
                        i += 1
                        time.sleep(5)
        else:
            data = None
        if data is not None:
            data = [x for x in data if len(x) == ord(end_col) - ord(start_col) + 1]
            if len(data) > 0:
                data = pd.DataFrame(data, columns=column_list)
            else:
                data = pd.DataFrame(columns=column_list)
        else:
            data = pd.DataFrame(columns=column_list)
        time.sleep(2)
        return data

    @staticmethod
    def column_string(n):
        string = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            string = chr(65 + remainder) + string
        return string

    def append_dataframe(self, df: pd.DataFrame, gid: int, column_till: str = None):
        sheet_data = list(
            filter(lambda x: x["sheetId"] == int(gid), self.sheet_properties)
        )[0]
        sheet_range = (
            f"{sheet_data['title']}!A1:{self.column_string(sheet_data['gridProperties']['columnCount'])}"
            if column_till is None
            else f"{sheet_data['title']}!A1:{column_till}"
        )
        self.service.values().append(
            spreadsheetId=self.sheet_id,
            range=sheet_range,
            valueInputOption="RAW",
            body={"values": oid_to_str(df).values.tolist()},
        ).execute()

    def clear_sheet(self, gid: int, column_till: str = None):
        sheet_data = list(
            filter(lambda x: x["sheetId"] == int(gid), self.sheet_properties)
        )[0]
        sheet_range = (
            f"{sheet_data['title']}!A2:{self.column_string(sheet_data['gridProperties']['columnCount'])}"
            if column_till is None
            else f"{sheet_data['title']}!A2:{column_till}"
        )
        body = {"ranges": sheet_range}
        self.service.values().batchClear(
            spreadsheetId=self.sheet_id, body=body
        ).execute()
