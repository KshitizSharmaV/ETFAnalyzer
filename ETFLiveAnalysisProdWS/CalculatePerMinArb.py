import sys
sys.path.append("..")

import json
import traceback
import datetime
import time
import pandas as pd

# Custom Imports
from CommonServices.LogCreater import CreateLogger
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from CommonServices import ImportExtensions
from MongoDB.PerMinDataOperations import PerMinDataOperations
from MongoDB.Schemas import trade_per_min_WS
from ETFLiveAnalysisWS.Helper.CalculationHelper import LiveHelper, tradestruct
from functools import partial


logObj = CreateLogger()
logger = logObj.createLogFile(dirName="Logs/",logFileName="-CalculateArbPerMinLog.log",loggerName="CalculateArbPerMinLog")

class ArbPerMin():

    def __init__(self, etflist, etfdict):
        self.etflist = etflist # Only used once per day
        self.etfdict = etfdict # Only used once per day
        self.CurrentMinutedata = {}
        self.PreviousMinutedata = {}
        self.trade_dict = {} # Maintains only 1 copy throughout the day and stores {Ticker : trade objects}
        self.helperobj = LiveHelper()

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
                return {etfname: {'Arbitrage in $': arbitrage, 
                					'ETF Price': etfprice,
									'ETF Change Price %': etfchange, 
									'Net Asset Value Change%': nav,
									**moverdictlist, 
									**changedictlist}}

            except Exception as e:
                print(e)
                traceback.print_exc(file=sys.stdout)
                logger.exception(e)
                pass
    
    def TradePricesForTickers(self,DateTimeOfTrade=None):
        TradesDataCursor = PerMinDataOperations().FetchAllTradeDataPerMinProd(DateTimeOfTrade=DateTimeOfTrade)
        TradePriceDf=pd.DataFrame(list(TradesDataCursor))
        TradePriceDf.set_index('sym', inplace=True)
        return TradePriceDf

    def calcArbitrage(self, tickerlist):
        #dt = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M")
        ##*** Testing Please remove if you see uncommented in Code - KTZ
        Current_Min = datetime.datetime(2020,6,11,13,27).replace(second=0,microsecond=0)
        Current_Min_ts = int(Current_Min.timestamp() * 1000)
        Prev_Min = Current_Min - datetime.timedelta(minutes=1)
        Prev_Min_ts = int(Prev_Min.timestamp() * 1000)
        ##*** Testing Please remove if you see uncommented in Code - KTZ

        logger.debug("Started calcArbitrage for dt {}".format(Current_Min_ts))
        start = time.time()
        try:
            # Fetch all Aggregate data received this minute
            TradesDataDfCurrentMin = self.TradePricesForTickers(DateTimeOfTrade=Current_Min_ts)
            TradesDataDfPreviousMin = self.TradePricesForTickers(DateTimeOfTrade=Prev_Min_ts)
            #logger.debug('Received data for {} tickers'.format(len(TradesDataDf.shape[0])))
            print("Current Minute")
            print(Current_Min_ts)
            print(TradesDataDfCurrentMin)

            print("Previous Minute")
            print(Prev_Min_ts)
            print(TradesDataDfPreviousMin)
            
            PriceChange = pd.merge(TradesDataDfCurrentMin,TradesDataDfPreviousMin,left_index=True,right_index=True)
            PriceChange.columns = ['priceT','priceT_1']
            PriceChange['price_pct_chg'] = PriceChange.pct_change(axis=1)['priceT_1']*100
            print(PriceChange)
            partial_arbitrtage_func = partial(self.calculation_for_each_etf, PriceChange)
            arbitrage_threadingresults = CPUBonundThreading(partial_arbitrtage_func, self.etfdict)
            print("arbitrage_threadingresults")
            arbdict={}
            [arbdict.update(item) for item in arbitrage_threadingresults]
            arbdict=pd.DataFrame.from_dict(arbdict, orient='index')
        except Exception as e:
            traceback.print_exc()
            logger.exception(traceback.print_exc())
        
        end = time.time()
        print("Calculation time: {}".format(end - start))
        return arbdict
        