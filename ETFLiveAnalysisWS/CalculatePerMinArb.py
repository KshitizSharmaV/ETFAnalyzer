import json
import sys, traceback

# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
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


class tradestruct(): # For Trade Objects, containing current minute and last minute price for Tickers
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

    def __init__(self, etflist, etfdict):
        self.etflist = etflist # Only used once per day
        self.etfdict = etfdict # Only used once per day
        self.trade_dict = {} # Maintains only 1 copy throughout the day and stores {Ticker : trade objects}

    def update_trade_dict(self, symbol, price, x):
        if symbol in self.trade_dict.keys():  # If trade object of said ETF/Holding is present in trade dict
            priceT_1 = self.trade_dict[symbol].priceT
            if x:  # Same 'x' as the one at call of this function. Serves as a flag here
                trade_obj = tradestruct(symbol=symbol, priceT=priceT_1, priceT_1=priceT_1)
            else:
                trade_obj = tradestruct(symbol=symbol, priceT=price, priceT_1=priceT_1)
            self.trade_dict[symbol] = trade_obj
        else:  # If trade object of said ETF/Holding is absent from trade dict
            trade_obj = tradestruct(symbol=symbol, priceT=price)
            self.trade_dict[symbol] = trade_obj

    def fetch_price_for_unrcvd_etfs(self, ticker):
        try:
            if ticker in self.etflist:  # Extract and store prev days price with today's timestamp only for ETFs and not Holdings
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
                return x
        except Exception as e:
            print("Exception in CalculatePerMinArb.py at line 84")
            print(e)
            traceback.print_exc()

    def get_top_movers_and_changes(self, navdf, holdingsdf):
        abs_navdf = navdf.abs().sort_values(ascending=False)
        changedf = self.tradedf.loc[holdingsdf.index]
        abs_changedf = changedf['price_pct_chg'].abs().sort_values(ascending=False)

        if len(navdf) >= 10:
            moverdict = navdf.loc[abs_navdf.index][:10].to_dict()
            changedict = abs_changedf[:10].to_dict()
        else:
            moverdict = navdf.loc[abs_navdf.index][:].to_dict()
            changedict = abs_changedf[:].to_dict()

        moverdictlist = {}
        [moverdictlist.update({'ETFMover%' + str(i + 1): [item, moverdict[item]]}) for item, i in
         zip(moverdict, range(len(moverdict)))]
        changedictlist = {}
        [changedictlist.update({'Change%' + str(i + 1): [item, changedict[item]]}) for item, i in
         zip(changedict, range(len(changedict)))]
        return moverdictlist, changedictlist

    def calcArbitrage(self, tickerlist):
        dt = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M")
        print(dt)
        start = time.time()
        unreceived_data = []
        try:
            # Fetch all Aggregate data received this minute
            ticker_data_cursor = PerMinDataOperations().FetchAllTradeDataPerMin(DateTimeOfTrade=dt)
            ticker_data_dict = {ticker_data['sym']: ticker_data['vw'] for ticker_data in ticker_data_cursor}
            for ticker in tickerlist:  # tickerlist = ETFs + Holdings
                x = []  # If ticker unreceived, List to store Old/Prev day data from DB, Will also serve as flag in update_trade_dict()

                # If ticker data present in last minute response
                if ticker in ticker_data_dict.keys():
                    symbol = ticker
                    price = ticker_data_dict[ticker]
                    self.update_trade_dict(symbol, price, x)
                else:
                    # If ticker data not present in last minute response
                    x = self.fetch_price_for_unrcvd_etfs(
                        ticker)  # store last AM data present in DB for given ETFs with current time
                    symbol = ticker
                    if x:
                        price = [item['vw'] for item in x if item['sym'] == symbol][
                        0]  # last stored price for given ETF in DB
                        unreceived_data.extend(x) # To store data for unreceived ticker for this minute. Necessary for Live ETF Prices on live modules/
                    else:
                        price = 0
                    self.update_trade_dict(symbol, price, x)


            self.tradedf = pd.DataFrame([value.__dict__ for key, value in self.trade_dict.items()])
            self.arbdict = {} # Maintains Calculated arbitrage data only for current minute

            self.tradedf.set_index('symbol', inplace=True)
            for etf in self.etfdict: # Check etf-hold.json file for structure of self.etfdict
                for etfname, holdingdata in etf.items(): # etfname = ETF Symbol, holdingdata = {Holding symbols : Weights}
                    try:
                        # ETF Price Change % calculation
                        etfchange = self.tradedf.loc[etfname, 'price_pct_chg']
                        #### NAV change % Calculation ####
                        # holdingsdf contains Weights corresponding to each holding
                        holdingsdf = pd.DataFrame(*[holdings for holdings in holdingdata])
                        holdingsdf.set_index('symbol', inplace=True)
                        holdingsdf['weight'] = holdingsdf['weight'] / 100
                        # DF with Holdings*Weights
                        navdf = self.tradedf.mul(holdingsdf['weight'], axis=0)['price_pct_chg'].dropna()
                        # nav = NAV change %
                        nav = navdf.sum()
                        #### Top 10 Movers and Price Changes ####
                        moverdictlist, changedictlist = self.get_top_movers_and_changes(navdf, holdingsdf)
                        etfprice = self.tradedf.loc[etfname, 'priceT']
                        #### Arbitrage Calculation ####
                        arbitrage = ((etfchange - nav) * etfprice) / 100
                        # Update self.arbdict with Arbitrage data of each ETF
                        self.arbdict.update({etfname: {'Arbitrage in $': arbitrage, 'ETF Price': etfprice,
                                                       'ETF Change Price %': etfchange, 'Net Asset Value Change%': nav,
                                                       **moverdictlist, **changedictlist}})
                    except Exception as e:
                        print(e)
                        traceback.print_exc(file=sys.stdout)
                        pass
        except Exception as e1:
            print(e1)
            pass
        end = time.time()
        print("Calculation time: {}".format(end - start))
        # Storing unreceived data for Live ETF Price availability
        print(unreceived_data)
        trade_per_min_WS.insert_many(unreceived_data)
        return self.arbdict


if __name__ == '__main__':
    print(ArbPerMin(etflist=list(pd.read_csv("NonChineseETFs.csv").columns.values),
                    etfdict=json.load(open('etf-hold.json', 'r'))).calcArbitrage(
        tickerlist=list(pd.read_csv("tickerlist.csv").columns.values)))
