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

    def __init__(self, etflist, etfdict,tickerlist):
        self.etflist = etflist # Only used once per day
        self.etfdict = etfdict # Only used once per day
        self.tickerlist = tickerlist
        self.trade_dict = {} # Maintains only 1 copy throughout the day and stores {Ticker : trade objects}
        self.TradesDataDfPreviousMin=None
        self.TradesDataDfCurrentMin=None
        self.helperobj = LiveHelper()

    def calculation_for_each_etf(self, tradedf, etf):
        # etfname = ETF Symbol, holdingdata = {Holding symbols : Weights}
        # # Following for loop only has one iteration cycle
        for etfname, holdingdata in etf.items():
            try:
                etfchange = tradedf.loc[etfname, 'price_pct_chg']
                holdingsdf = pd.DataFrame(*[holdings for holdings in holdingdata])
                holdingsdf.set_index('symbol', inplace=True)
                holdingsdf['weight'] = holdingsdf['weight'] / 100
                navdf = tradedf.mul(holdingsdf['weight'], axis=0)['price_pct_chg'].dropna()
                nav = navdf.sum()
                moverdictlist, changedictlist = self.helperobj.get_top_movers_and_changes(tradedf, navdf, holdingsdf)
                etfprice = tradedf.loc[etfname, 'priceT']
                arbitrage = ((etfchange - nav) * etfprice) / 100
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

    def IntializingPreviousMinTradeDf(self,Current_Min=None):
        Prev_Min = Current_Min - datetime.timedelta(minutes=1)
        Prev_Min_ts = int(Prev_Min.timestamp() * 1000)
        return self.TradePricesForTickers(DateTimeOfTrade=Prev_Min_ts)

    def calcArbitrage(self):
        ##*** Testing Please remove if you see uncommented in Code - KTZ
        #Current_Min = datetime.datetime(2020,6,11,13,27).replace(second=0,microsecond=0)
        ##*** Testing Please remove if you see uncommented in Code - KTZ
        Current_Min = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M")
        Current_Min_ts = int(Current_Min.timestamp() * 1000)
        
        logger.debug("Started calcArbitrage for dt {}".format(Current_Min_ts))
        start = time.time()
        try:
            # Fetch all Aggregate data received this minute
            self.TradesDataDfCurrentMin = self.TradePricesForTickers(DateTimeOfTrade=Current_Min_ts)
            self.TradesDataDfPreviousMin = self.IntializingPreviousMinTradeDf(Current_Min=Current_Min)if self.TradesDataDfPreviousMin is None else self.TradesDataDfPreviousMin 
            
            # Concating Prev Min Data df To New Min data, that way T_1 is maintained in lifecycle
            self.TradesDataDfCurrentMin=pd.concat([self.TradesDataDfCurrentMin,
                self.TradesDataDfPreviousMin[~self.TradesDataDfPreviousMin.index.isin(self.TradesDataDfCurrentMin.index)]])
            # Merge Current & Old Minute to calculation %Change, from above If Ticker doesn't come in T Min, Pct_change will become 0
            PriceChange = pd.merge(self.TradesDataDfCurrentMin,self.TradesDataDfPreviousMin,left_index=True,right_index=True,how='left')
            PriceChange.columns = ['priceT','priceT_1']
            PriceChange['price_pct_chg'] = -PriceChange.pct_change(axis=1)['priceT_1']*100

            # Adding tickers which are still not available from beginning of day
            NotavailableInDf = list(set(self.tickerlist)-set(PriceChange.index))
            tempDf=pd.DataFrame(columns=PriceChange.columns,index=NotavailableInDf).fillna(0)
            PriceChange = pd.concat([PriceChange,tempDf])

            partial_arbitrtage_func = partial(self.calculation_for_each_etf, PriceChange)
            arbitrage_threadingresults = CPUBonundThreading(partial_arbitrtage_func, self.etfdict)
            arbdict={}
            [arbdict.update(item) for item in arbitrage_threadingresults]
            arbdict=pd.DataFrame.from_dict(arbdict, orient='index')
            self.TradesDataDfPreviousMin=self.TradesDataDfCurrentMin
        except Exception as e:
            traceback.print_exc()
            logger.exception(traceback.print_exc())
        
        print("Calculation time: {}".format(time.time() - start))
        return arbdict
        