import time
import traceback

import pandas as pd
import numpy as np
from itertools import chain
from functools import partial
from datetime import datetime, timedelta
from MongoDB.MongoDBConnections import MongoDBConnectors
from MongoDB.SaveFetchQuotesData import MongoTradesQuotesData
from MongoDB.Schemas import quotespipeline
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from PolygonTickData.Helper import Helper

connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
per_sec_trades_db = connection.ETF_db.PerSecLiveTrades
per_sec_quotes_db = connection.ETF_db.PerSecLiveQuotes


class TradeStruct():  # For Trade Objects, containing current minute and last minute price for Tickers
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


def get_trades_pipeline():
    return [
        {
            '$unwind': {
                'path': '$data'
            }
        }, {
            '$match': ''
        }, {
            '$project': {
                '_id': 0,
                'data': 1
            }
        }
    ]


def get_trades_data(ticker_list, start_ts, end_ts) -> pd.DataFrame:
    query = {
        'symbol': {'$in': ticker_list},
        'data.t': {
            '$lte': end_ts,
            '$gte': start_ts
        }
    }
    trades_pipeline = get_trades_pipeline()
    trades_pipeline[1]['$match'] = query
    data_cursor = per_sec_trades_db.aggregate(trades_pipeline, allowDiskUse=True)
    trades_df = pd.DataFrame([item['data'] for item in data_cursor])
    trades_df.rename(columns={'t': 'Time', 'p': 'Price'}, inplace=True)
    trades_df = trades_df[['Symbol', 'Time', 'Price']]
    return trades_df


def get_quotes_data(etf_name, date):
    data_list = MongoTradesQuotesData().fetch_quotes_trades_data_from_mongo(symbolList=[etf_name], date=date,
                                                                            CollectionName=per_sec_quotes_db,
                                                                            pipeline=quotespipeline)
    quotes_df = pd.DataFrame(data_list)
    return quotes_df


def get_ticker_list_for_trades(etf_name, date):
    ticker_list = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name, fundholdingsdate=date).getSymbols()
    return ticker_list


def calculate_spread(etf_name, date):
    spread_df = get_quotes_data(etf_name=etf_name, date=date)
    spread_df['Spread'] = spread_df['askprice'] - spread_df['bidprice']
    return spread_df


def get_timestamp_ranges_1sec(date: datetime):
    start = date.replace(hour=20, minute=0, second=0, microsecond=0)
    end = date.replace(day=date.day + 1, hour=1, minute=30, second=0, microsecond=0)
    date_range = pd.date_range(start, end, freq='10S')
    date_range = date_range.to_pydatetime()
    to_ts = np.vectorize(lambda x: int(x.timestamp() * 1000000000))
    ts_range = to_ts(date_range)
    return ts_range


def update_trade_dict(trade_dict, symbol, price):
    if symbol in trade_dict.keys():  # If trade object of said ETF/Holding is present in trade dict
        if trade_dict[symbol].priceT == 0 and price != 0:
            priceT_1 = price
        elif trade_dict[symbol].priceT != 0 and price == 0:
            price = priceT_1 = trade_dict[symbol].priceT
        else:
            priceT_1 = trade_dict[symbol].priceT
        trade_obj = TradeStruct(symbol=symbol, priceT=price, priceT_1=priceT_1)
        trade_dict[symbol] = trade_obj
    else:  # If trade object of said ETF/Holding is absent from trade dict
        trade_obj = TradeStruct(symbol=symbol, priceT=price)
        trade_dict[symbol] = trade_obj
    return trade_dict


def calculation_maintainer(etf_name, date):
    ts_ranges = get_timestamp_ranges_1sec(date)
    ticker_list = get_ticker_list_for_trades(etf_name=etf_name, date=date)
    """Get Holdings and Weights of ETF"""
    holdings_dict = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name,
                                                            fundholdingsdate=date).getETFWeights()
    trades_dict = {}
    arbitrage_dict = {}
    for idx in range(1, len(ts_ranges)):
        arbitrage_dict[ts_ranges[idx - 1]] = calculate_arbitrage_for_etf_and_date(etf_name=etf_name,
                                                                                  ticker_list=ticker_list,
                                                                                  start_ts=ts_ranges[idx - 1],
                                                                                  end_ts=ts_ranges[idx],
                                                                                  holdings_dict=holdings_dict,
                                                                                  trades_dict=trades_dict)
    return pd.DataFrame(arbitrage_dict)


def calculate_arbitrage_for_etf_and_date(etf_name, ticker_list, start_ts, end_ts, holdings_dict, trades_dict):
    try:
        """Get Trade Prices of ETF and Holdings"""
        checkpoint1 = time.time()
        trades_df = get_trades_data(ticker_list=ticker_list, start_ts=int(start_ts), end_ts=int(end_ts))
        checkpoint2 = time.time()
        print(f"Data Fetch Time: {checkpoint2-checkpoint1}")
        trades_df = trades_df.groupby('Symbol').mean()
        trades_df.reset_index(inplace=True)

        """Update trade dict with ETF and Holdings prices for this second"""
        trades_df = trades_df.append(
            [{'Symbol': ticker, 'Price': 0} for ticker in ticker_list if ticker not in trades_df['Symbol'].tolist()])
        trades_df.apply(lambda x: update_trade_dict(trades_dict, x['Symbol'], x['Price']), axis=1)

        """
        NAV = Sum(Price Change % of Ticker * Weight of Ticker in ETF)
        holdings_dict[ticker] --> Weight of the ticker in ETF
        trades_dict[ticker].price_pct_chg --> Price Change % for ticker
        """
        tick_list = ticker_list.copy()
        tick_list.remove(etf_name)
        mapper = map(
            lambda x: trades_dict[x].price_pct_chg * (holdings_dict[x] / 100) if x in trades_dict.keys() else 0,
            tick_list)
        nav = sum(list(mapper))

        """
        Arbitrage = ( (Price Change % of ETF - NAV calculated above) * Current Price for ETF ) / 100
        """
        arbitrage = ((trades_dict[etf_name].price_pct_chg - nav) * trades_dict[etf_name].priceT) / 100
        print(f"{start_ts} - {end_ts} : Arbitrage for {etf_name} : {round(arbitrage, 8)}")
        checkpoint3 = time.time()
        print(f"Calculation Time: {checkpoint3-checkpoint2}")
        print(f"Overall Time: {checkpoint3-checkpoint1}")
        return arbitrage
    except Exception as e:
        traceback.print_exc()
        pass


# if __name__ == '__main__':
#     date_ = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=4))
#     # GetDataFromDB().get_trades_data(ticker_list=['VEEV', 'DLR'], date=date_)
#     # GetDataFromDB().get_timestamp_ranges_1sec(date_)
#     # GetDataFromDB().calculate_arbitrage_for_etf_and_date('VO', date_)

date_ = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5))
arb = calculation_maintainer('VO', date_)
print(arb)