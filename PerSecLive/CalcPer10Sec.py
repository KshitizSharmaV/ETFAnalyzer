import sys
sys.path.append("../")

import time
import traceback
from pymongo import ASCENDING, DESCENDING
import pandas as pd
import numpy as np
from dateutil import tz
from datetime import datetime, timedelta, date
from MongoDB.MongoDBConnections import MongoDBConnectors
from CalculateETFArbitrage.Helpers.LoadEtfHoldings import LoadHoldingsdata
from CommonServices.Holidays import LastWorkingDay
from PolygonTickData.Helper import Helper

connection = MongoDBConnectors().get_pymongo_devlocal_devlocal()
per_sec_trades_db = connection.ETF_db.PerSecLiveTrades
per_sec_trades_db.create_index([("Symbol", ASCENDING), ("t", DESCENDING)])
per_sec_quotes_db = connection.ETF_db.PerSecLiveQuotes
per_sec_quotes_db.create_index([("Symbol", ASCENDING), ("t", DESCENDING)])


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


def get_ticker_list_for_trades(etf_name, date):
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


def update_trade_dict(trade_dict, symbol, price):
    """If trade object of the symbol is present in trade dict"""
    """Condition 2 change to previous price"""
    priceT_1 = ((trade_dict[symbol].priceT == 0 and price != 0) and price) or trade_dict[symbol].priceT
    trade_obj = TradeStruct(symbol=symbol, priceT=price, priceT_1=priceT_1)
    trade_dict[symbol] = trade_obj
    return trade_dict


def calculation_maintainer(etf_name, _date):
    start, end = get_local_time_for_date(date_for=_date)
    ts_ranges = get_timestamp_ranges_1sec(start=start, end=end)
    ticker_list = get_ticker_list_for_trades(etf_name=etf_name, date=_date)
    if etf_name not in set(ticker_list):
        ticker_list.append(etf_name)
    """Get Holdings and Weights of ETF"""
    load_holdings_obj = LoadHoldingsdata()
    holdings_dict = load_holdings_obj.LoadHoldingsAndClean(etfname=etf_name,
                                                           fundholdingsdate=_date).getETFWeights()
    # """Get CASH value"""
    # cash = load_holdings_obj.cashvalueweight / 100
    # taum = load_holdings_obj.get_total_asset_under_mgmt(etf_name=etf_name, fund_holdings_date=date) * 1000
    # cash_value = taum * cash

    """Initialise Trade dict for the day for the ETF"""
    trades_dict = {ticker: TradeStruct(symbol=ticker, priceT=0) for ticker in ticker_list}

    arbitrage_records = []
    for idx in range(1, len(ts_ranges)):
        arbitrage_records.append(calculate_arbitrage_for_etf_and_date(etf_name=etf_name,
                                                                      ticker_list=ticker_list,
                                                                      start_ts=ts_ranges[idx - 1],
                                                                      end_ts=ts_ranges[idx],
                                                                      holdings_dict=holdings_dict,
                                                                      trades_dict=trades_dict))
    result = pd.DataFrame.from_records(arbitrage_records)
    result.to_csv("ArbitrageData"+_date.strftime("%Y%m%d")+".csv", header=False, sep=",", index=False)
    return result


hobj = Helper()


def calculate_arbitrage_for_etf_and_date(etf_name, ticker_list, start_ts, end_ts, holdings_dict, trades_dict):
    try:
        """Get Trade Prices of ETF and Holdings"""
        checkpoint1 = time.time()
        trades_df = get_trades_data(ticker_list=ticker_list, start_ts=int(start_ts), end_ts=int(end_ts))
        checkpoint2 = time.time()
        print(f"Data Fetch Time: {checkpoint2 - checkpoint1}")
        trades_df = trades_df.groupby('Symbol').mean()
        trades_df.reset_index(inplace=True)

        """Update trade dict with ETF and Holdings prices for this second"""
        trades_df.apply(lambda x: update_trade_dict(trades_dict, x['Symbol'], x['Price']), axis=1)

        """NAV = Sum(Price Change % of Ticker * Weight of Ticker in ETF)
        holdings_dict[ticker] --> Weight of the ticker in ETF
        trades_dict[ticker].price_pct_chg --> Price Change % for ticker"""
        tick_list = ticker_list.copy()
        tick_list.remove(etf_name)
        mapper = map(lambda x: trades_dict[x].price_pct_chg * (holdings_dict[x] / 100), tick_list)
        nav = sum(list(mapper))

        """Arbitrage = ( (Price Change % of ETF - NAV calculated above) * Current Price for ETF ) / 100"""
        etf_price = trades_dict[etf_name].priceT
        arbitrage = ((trades_dict[etf_name].price_pct_chg - nav) * etf_price) / 100
        checkpoint3 = time.time()
        print(f"Calculation Time: {checkpoint3 - checkpoint2}")

        """Quotes Data"""
        spreadforsec = calculate_spread(etf_name=etf_name, start_ts=int(start_ts), end_ts=int(end_ts))
        checkpoint6 = time.time()
        print(f"spread calculation time : {checkpoint6-checkpoint3}")
        print(f"Overall Time: {checkpoint6 - checkpoint1}")
        print(
            f"{start_ts} - {end_ts} : Arbitrage for {etf_name} : {round(arbitrage, 8)} || ETF Price : {etf_price} || Spread : {spreadforsec}")

        return {'End Time': hobj.getHumanTime(end_ts), 'Arbitrage': arbitrage, 'ETFPrice': etf_price,
                'Spread': spreadforsec}
    except Exception as e:
        traceback.print_exc()
        pass


date_ = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=4))
checkpoint4 = time.time()
arb = calculation_maintainer('VO', date_)
checkpoint5 = time.time()
print(f"Total Time taken for all processes for ETF is: {checkpoint5 - checkpoint4} seconds")
