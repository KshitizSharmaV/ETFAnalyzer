import sys

sys.path.append("../")

import pandas as pd
import numpy as np
from dateutil import tz
from datetime import datetime, timedelta, date
from MongoDB.MongoDBConnections import MongoDBConnectors
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from CommonServices.Holidays import LastWorkingDay
from PolygonTickData.Helper import Helper
from dataclasses import dataclass

connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
per_sec_trades_db = connection.ETF_db.PerSecLiveTrades
per_sec_quotes_db = connection.ETF_db.PerSecLiveQuotes
helper_object = Helper()


class TradeStruct():  # For Trade Objects, containing current minute and last minute price for Tickers
    __slots__ = ['symbol', 'priceT', 'priceT_1', 'price_pct_chg']

    def calc_pct_chg(self, priceT, priceT_1):
        if priceT_1 == 0:
            return 0
        return ((priceT - priceT_1) / priceT_1) * 100

    def __init__(self, symbol, priceT, priceT_1=None):
        self.symbol = symbol
        self.priceT = priceT
        if not priceT_1:
            self.priceT_1 = priceT
        else:
            self.priceT_1 = priceT_1
        self.price_pct_chg = self.calc_pct_chg(self.priceT, self.priceT_1)


@dataclass
class ETF():
    __slots__ = ['etf_name', 'holdings_dict']
    etf_name: str
    holdings_dict: dict


def get_local_time_for_date(date_for: datetime):
    start_hour = 13 if date(2020, 3, 8) < datetime.now().date() < date(2020, 11, 1) else 14
    end_hour = 20 if date(2020, 3, 8) < datetime.now().date() < date(2020, 11, 1) else 21
    last_working_day = LastWorkingDay(date_for)
    start = end = last_working_day
    start = start.replace(hour=start_hour, minute=30, second=0, microsecond=0, tzinfo=tz.gettz('UTC'))
    start = start.astimezone(tz.tzlocal())
    end = end.replace(hour=end_hour, minute=00, second=0, microsecond=0, tzinfo=tz.gettz('UTC'))
    end = end.astimezone(tz.tzlocal())
    return start, end


def get_timestamp_ranges_1sec(start, end):
    date_range = pd.date_range(start, end, freq='1S')
    date_range = date_range.to_pydatetime()
    to_ts = np.vectorize(lambda x: int(x.timestamp() * 1000000000))
    ts_range = to_ts(date_range)
    return ts_range


def get_trades_data(ticker_list, start_ts, end_ts) -> pd.DataFrame:
    query = {
        'Symbol': {'$in': ticker_list},
        't': {
            '$lte': end_ts,
            '$gte': start_ts
        }
    }
    data_cursor = per_sec_trades_db.find(query)
    trades_df = pd.DataFrame(list(data_cursor))
    trades_df.rename(columns={'t': 'Time', 'p': 'Price'}, inplace=True)
    trades_df = trades_df[['Symbol', 'Time', 'Price']]
    return trades_df


def get_quotes_data(etf_name, start_ts, end_ts) -> pd.DataFrame:
    query = {
        'Symbol': etf_name,
        't': {
            '$lte': end_ts,
            '$gte': start_ts
        }
    }
    data_cursor = per_sec_quotes_db.find(query)
    quotes_df = pd.DataFrame(list(data_cursor))
    quotes_df.rename(columns={'t': 'Time', 'p': 'bidprice', 'P': 'askprice', 's': 'bidsize', 'S': 'asksize'},
                     inplace=True)
    return quotes_df


def get_ticker_list_for_etf(date, etf_name) -> list:
    ticker_list = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name, fundholdingsdate=date).getSymbols()
    return ticker_list


def calculate_spread(etf_name, start_ts, end_ts):
    spread_df = get_quotes_data(etf_name=etf_name, start_ts=start_ts, end_ts=end_ts)
    if (spread_df.shape[0] != 0) and (
            {'Symbol', 'bidprice', 'askprice', 'bidsize', 'asksize'}.issubset(spread_df.columns)):
        spread_df = spread_df[['Symbol', 'bidprice', 'askprice', 'bidsize', 'asksize']]
        spread_df['Spread'] = spread_df['askprice'] - spread_df['bidprice']
        return spread_df['Spread'].mean()
    else:
        return 0


def update_trade_dict(trade_dict, symbol, price):
    """If trade object of the symbol is present in trade dict"""
    priceT_1 = ((trade_dict[symbol].priceT == 0 and price != 0) and price) or trade_dict[symbol].priceT
    trade_obj = TradeStruct(symbol=symbol, priceT=price, priceT_1=priceT_1)
    trade_dict[symbol] = trade_obj
    return trade_dict


def trade_dict_operation(ticker_list, start_ts, end_ts, trades_dict):
    trades_df = get_trades_data(ticker_list=ticker_list, start_ts=int(start_ts), end_ts=int(end_ts))
    trades_df = trades_df.groupby('Symbol').mean()
    trades_df.reset_index(inplace=True)
    trades_df.apply(lambda x: update_trade_dict(trades_dict, x['Symbol'], x['Price']), axis=1)


def make_etf_objects(_date, etf_name):
    holdings_dict = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name, fundholdingsdate=_date).getETFWeights()
    return ETF(etf_name=etf_name, holdings_dict=holdings_dict)
