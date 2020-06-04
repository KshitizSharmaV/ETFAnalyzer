import json
import sys, traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf1903', '/home/piyush/Desktop/etf1903/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf1903/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf1903/CommonServices',
                 '/home/piyush/Desktop/etf1903/CalculateETFArbitrage', '/home/piyush/Desktop/etf1903/ETFLiveAnalysis'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalysis', '/home/ubuntu/ETFAnalysis/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalysis/HoldingsDataScripts', '/home/ubuntu/ETFAnalysis/CommonServices',
                 '/home/ubuntu/ETFAnalysis/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
import datetime
import time
import pandas as pd
from CommonServices.ThreadingRequests import IOBoundThreading
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from PolygonTickData.PolygonCreateURLS import PolgonDataCreateURLS
from CommonServices.RetryDecor import retry
import logging
import os

path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)

filename = path + datetime.datetime.now().strftime("%Y%m%d") + "-ArbPerMinLog.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='a')
# logger = logging.getLogger("EventLogger")
logger = logging.getLogger("ArbPerMinLogger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

from MongoDB.PerMinDataOperations import PerMinDataOperations, trade_per_min_WS


class tradestruct():
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


class ArbPerMin():

    def __init__(self):
        self.etflist = list(
            pd.read_csv("WorkingETFs.csv").columns.values)
        f = open('etf-hold.json', 'r')
        self.etfdict = json.load(f)
        self.trade_dict = {}

    def calcArbitrage(self, tickerlist):
        dt = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M")
        print(dt)
        start = time.time()
        unreceived_data = []
        try:
            ticker_data_cursor = PerMinDataOperations().FetchAllTradeDataPerMin(DateTimeOfTrade=dt)
            ticker_data_dict = {ticker_data['sym']: ticker_data['vw'] for ticker_data in ticker_data_cursor}
            for ticker in tickerlist:
                # If ticker data present in last minute response
                if ticker in ticker_data_dict.keys():
                    symbol = ticker
                    price = ticker_data_dict[ticker]
                    if symbol in self.trade_dict.keys():
                        priceT_1 = self.trade_dict[symbol].priceT
                        trade_obj = tradestruct(symbol=symbol, priceT=price, priceT_1=priceT_1)
                        self.trade_dict[symbol] = trade_obj
                    else:
                        trade_obj = tradestruct(symbol=symbol, priceT=price)
                        self.trade_dict[symbol] = trade_obj
                else:
                    # If ticker data not in last minute response
                    try:
                        if ticker in self.etflist:
                            dt_query = datetime.datetime.now().replace(second=0, microsecond=0)
                            dt_query_ts = int(dt_query.timestamp() * 1000)
                            last_recvd_data_for_ticker = trade_per_min_WS.find(
                                {"e": {"$lte": dt_query_ts}, "sym": ticker}).sort("e", -1).limit(1)
                            x = [{legend: (
                                item[legend] if legend in item.keys() and legend in ['ev', 'sym', 'v', 'av', 'op', 'vw',
                                                                                     'o', 'c', 'h', 'l', 'a'] else (
                                    dt_query_ts - 60000 if legend == 's' else dt_query_ts)) for legend in
                                  ['ev', 'sym', 'v', 'av', 'op', 'vw', 'o', 'c', 'h', 'l', 'a', 's', 'e']} for item in
                                 last_recvd_data_for_ticker]
                            # for dict_data in x:
                            #     dict_data.update({"s": dt_query_ts - 60000, "e": dt_query_ts})
                            unreceived_data.extend(x)
                    except Exception as e:
                        print("Exception in CalculatePerMinArb.py at line 84")
                        print(e)
                        traceback.print_exc()
                    symbol = ticker
                    if symbol in self.trade_dict.keys():
                        priceT_1 = self.trade_dict[symbol].priceT
                        trade_obj = tradestruct(symbol=symbol, priceT=priceT_1, priceT_1=priceT_1)
                        self.trade_dict[symbol] = trade_obj
                    else:
                        trade_obj = tradestruct(symbol=symbol, priceT=0)
                        self.trade_dict[symbol] = trade_obj

            self.tradedf = pd.DataFrame([value.__dict__ for key, value in self.trade_dict.items()])
            self.arbdict = {}
            self.tradedf.set_index('symbol', inplace=True)
            for etf in self.etfdict:
                for etfname, holdingdata in etf.items():
                    try:
                        # ETF Price Change % calculation
                        etfchange = self.tradedf.loc[etfname, 'price_pct_chg']
                        # NAV change % Calculation
                        holdingsdf = pd.DataFrame(*[holdings for holdings in holdingdata])
                        holdingsdf.set_index('symbol', inplace=True)
                        nav = sum([holdingsdf.loc[sym, 'weight'] * self.tradedf.loc[sym, 'price_pct_chg'] for sym in
                                   holdingsdf.index])
                        # nav = NAV change %
                        etfprice = self.tradedf.loc[etfname, 'priceT']
                        arbitrage = ((etfchange - nav) * etfprice) / 100
                        self.arbdict.update({etfname: arbitrage})
                    except Exception as e:
                        # print(e)
                        traceback.print_exc(file=sys.stdout)
                        pass

        except Exception as e1:
            print(e1)
            pass
        end = time.time()
        print("Calculation time: {}".format(end - start))
        print(unreceived_data)
        trade_per_min_WS.insert_many(unreceived_data)
        return self.arbdict


if __name__ == '__main__':
    print(ArbPerMin().calcArbitrage())
