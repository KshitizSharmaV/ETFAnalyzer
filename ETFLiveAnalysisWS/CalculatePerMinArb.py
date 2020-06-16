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
import logging
import os

path = os.path.join(os.getcwd(), "Logs/")
if not os.path.exists(path):
    os.makedirs(path)
filename = path + datetime.datetime.now().strftime("%Y%m%d") + "-ArbPerMinLog.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='a')
logger = logging.getLogger("ArbPerMinLogger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from MongoDB.PerMinDataOperations import PerMinDataOperations, trade_per_min_WS
from ETFLiveAnalysisWS.Helper.CalculationHelper import LiveHelper, tradestruct
from functools import partial



class ArbPerMin():

    def __init__(self, etflist, etfdict):
        self.etflist = etflist # Only used once per day
        self.etfdict = etfdict # Only used once per day
        self.trade_dict = {} # Maintains only 1 copy throughout the day and stores {Ticker : trade objects}
        self.helperobj = LiveHelper()

    def update_prices_for_minute(self, etflist, trade_dict, ticker_data_dict, unrcvd_data_list, ticker):
        x = []  # If ticker unreceived, List to store Old/Prev day data from DB, Will also serve as flag in update_trade_dict()
        # If ticker data present in last minute response
        if ticker in ticker_data_dict.keys():
            symbol = ticker
            price = ticker_data_dict[ticker]
            trade_dict = self.helperobj.update_trade_dict(trade_dict=trade_dict, symbol=symbol, price=price, x=x)
        # If ticker data not present in last minute response
        else:
            # # store last AM data present in DB for given ETFs with current time
            x = self.helperobj.fetch_price_for_unrcvd_etfs(etflist=etflist, ticker=ticker)
            symbol = ticker
            if x:
                # # last stored price for given ETF in DB
                price = [item['vw'] for item in x if item['sym'] == symbol][0]
                # # To store data for unreceived ticker for this minute. Necessary for Live ETF Prices on live modules/
                unrcvd_data_list.extend(x)
            else:
                # # If price never received in history of time
                price = 0
            trade_dict = self.helperobj.update_trade_dict(trade_dict=trade_dict, symbol=symbol, price=price, x=x)
            return trade_dict

    def calculation_for_each_etf(self, tradedf, etf):
        # etfname = ETF Symbol, holdingdata = {Holding symbols : Weights}
        # # Following for loop only has one iteration cycle
        for etfname, holdingdata in etf.items():
            try:
                # ETF Price Change % calculation
                etfchange = tradedf.loc[etfname, 'price_pct_chg']
                #### NAV change % Calculation ####
                # holdingsdf contains Weights corresponding to each holding
                holdingsdf = pd.DataFrame(*[holdings for holdings in holdingdata])
                holdingsdf.set_index('symbol', inplace=True)
                holdingsdf['weight'] = holdingsdf['weight'] / 100
                # DF with Holdings*Weights
                navdf = tradedf.mul(holdingsdf['weight'], axis=0)['price_pct_chg'].dropna()
                # nav = NAV change %
                nav = navdf.sum()
                #### Top 10 Movers and Price Changes ####
                moverdictlist, changedictlist = self.helperobj.get_top_movers_and_changes(tradedf, navdf,
                                                                                          holdingsdf)
                etfprice = tradedf.loc[etfname, 'priceT']
                #### Arbitrage Calculation ####
                arbitrage = ((etfchange - nav) * etfprice) / 100
                # Update self.arbdict with Arbitrage data of each ETF
                return {etfname: {'Arbitrage in $': arbitrage, 'ETF Price': etfprice,
                                               'ETF Change Price %': etfchange, 'Net Asset Value Change%': nav,
                                               **moverdictlist, **changedictlist}}
            except Exception as e:
                print(e)
                traceback.print_exc(file=sys.stdout)
                logger.exception(e)
                pass

    def calcArbitrage(self, tickerlist):
        dt = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M")
        print(dt)
        logger.debug("started calcArbitrage for dt {}".format(dt))
        start = time.time()
        unreceived_data = []
        try:
            # Fetch all Aggregate data received this minute
            ticker_data_cursor = PerMinDataOperations().FetchAllTradeDataPerMin(DateTimeOfTrade=dt)
            ticker_data_dict = {ticker_data['sym']: ticker_data['vw'] for ticker_data in ticker_data_cursor}
            logger.debug('Received data for {} tickers'.format(len(ticker_data_dict)))
            partial_update_prices = partial(self.update_prices_for_minute, self.etflist, self.trade_dict, ticker_data_dict, unreceived_data)

            trade_dict_threadingresults = CPUBonundThreading(partial_update_prices, tickerlist)
            [self.trade_dict.update(item) for item in trade_dict_threadingresults]

            self.tradedf = pd.DataFrame([value.__dict__ for key, value in self.trade_dict.items()])
            self.tradedf.set_index('symbol', inplace=True)

            # # Maintains Calculated arbitrage data only for current minute
            self.arbdict = {}
            # partial_arbitrtage_func = partial(self.calculation_for_each_etf, self.tradedf)
            # arbitrage_threadingresults = CPUBonundThreading(partial_arbitrtage_func, self.etfdict)
            # [self.arbdict.update(item) for item in arbitrage_threadingresults]
            ###########################################################################
            for etf in self.etfdict:
                for etfname, holdingdata in etf.items():
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
                        moverdictlist, changedictlist = self.helperobj.get_top_movers_and_changes(self.tradedf, navdf,
                                                                                                  holdingsdf)
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
                        logger.exception(e)
                        pass
            ###########################################################################
        except Exception as e1:
            print(e1)
            logger.exception(e1)
            pass
        end = time.time()
        print("Calculation time: {}".format(end - start))
        # Storing unreceived data for Live ETF Price availability
        print(unreceived_data)
        trade_per_min_WS.insert_many(unreceived_data)
        logger.debug('unreceived data length : {}'.format(len(unreceived_data)))
        logger.debug('unreceived data stored, returning arb result')
        return self.arbdict

if __name__ == '__main__':
    print(ArbPerMin(etflist=list(pd.read_csv("../CSVFiles/250M_WorkingETFs.csv").columns.values),
                    etfdict=json.load(open('../CSVFiles/etf-hold.json', 'r'))).calcArbitrage(
        tickerlist=list(pd.read_csv("../CSVFiles/tickerlist.csv").columns.values)))
