import pandas as pd
import numpy as np
from itertools import chain
from datetime import datetime, timedelta
from MongoDB.MongoDBConnections import MongoDBConnectors
from MongoDB.SaveFetchQuotesData import MongoTradesQuotesData
from MongoDB.Schemas import quotespipeline
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from PolygonTickData.Helper import Helper


class GetDataFromDB():
    def __init__(self):
        self.connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
        self.per_sec_trades_db = self.connection.ETF_db.PerSecLiveTrades
        self.per_sec_quotes_db = self.connection.ETF_db.PerSecLiveQuotes

    def get_trades_data(self, ticker_list, date):
        data_cursor = self.per_sec_trades_db.find(
            {'symbol': {'$in': ticker_list},
             'dateForData': date},
            {'_id': 0, 'data': 1})
        trades_df = pd.DataFrame(list(chain(*[item['data'] for item in data_cursor])))
        trades_df.rename(columns={'t': 'Time', 'p': 'Price'}, inplace=True)
        trades_df = trades_df[['Symbol', 'Time', 'Price']]
        return trades_df

    def get_quotes_data(self, etf_name, date):
        data_list = MongoTradesQuotesData().fetch_quotes_trades_data_from_mongo(symbolList=[etf_name], date=date,
                                                                                CollectionName=self.per_sec_quotes_db,
                                                                                pipeline=quotespipeline)
        quotes_df = pd.DataFrame(data_list)
        return quotes_df

    def get_all_trades_data(self, etf_name, date):
        ticker_list = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name, fundholdingsdate=date).getSymbols()
        trades_df = self.get_trades_data(ticker_list=ticker_list, date=date)
        return trades_df

    def calculate_spread(self, etf_name, date):
        spread_df = self.get_quotes_data(etf_name=etf_name, date=date)
        spread_df['Spread'] = spread_df['askprice'] - spread_df['bidprice']
        return spread_df

    def get_timestamp_ranges_1sec(self, date: datetime):
        start = date.replace(hour=9, minute=30, second=0, microsecond=0)
        end = date.replace(hour=16, minute=0, second=0, microsecond=0)
        date_range = pd.date_range(start, end, freq='1S')
        date_range = date_range.to_pydatetime()
        to_ts = np.vectorize(lambda x: int(x.timestamp() * 1000000000))
        ts_range = to_ts(date_range)
        return ts_range

    def calculate_arbitrage_for_etf_and_date(self, etf_name, date):
        try:
            """Get Trade Prices of ETF and Holdings"""
            trades_df = self.get_all_trades_data(etf_name=etf_name, date=date)
            ts_ranges = self.get_timestamp_ranges_1sec(date)
            """Get Holdings and Weights of ETF"""
            holdings_dict = LoadHoldingsdata().LoadHoldingsAndClean(etfname=etf_name,
                                                                    fundholdingsdate=date).getETFWeights()
            holdings_dict.pop(etf_name, 0)
            etf_df = trades_df[trades_df['Symbol'] == etf_name]
            etf_df['Price_Change_%'] = etf_df['Price'].pct_change(fill_method='ffill') * 100
            all_holdings_df = pd.DataFrame()
            for holding, weight in holdings_dict.items():
                holding_df = trades_df[trades_df['Symbol'] == holding]
                holding_df['Price_Change_%'] = holding_df['Price'].pct_change(fill_method='ffill') * 100
                holding_df['Weight'] = weight / 100
                holding_df.groupby(ts_ranges)
                all_holdings_df.append(holding_df, ignore_index=True)
        except Exception as e:
            print(e)
            pass


if __name__ == '__main__':
    date_ = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=4))
    # GetDataFromDB().get_trades_data(ticker_list=['VEEV', 'DLR'], date=date_)
    GetDataFromDB().get_timestamp_ranges_1sec(date_)
    # GetDataFromDB().calculate_arbitrage_for_etf_and_date('VO', date_)
