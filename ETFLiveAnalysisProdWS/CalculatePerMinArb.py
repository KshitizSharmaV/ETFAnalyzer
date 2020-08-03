import sys
sys.path.append("..")

import json
import traceback
import datetime
import time
import pandas as pd

# Custom Imports
from CommonServices.EmailService import EmailSender
from CommonServices.LogCreater import CreateLogger
from CommonServices.MultiProcessingTasks import CPUBonundThreading
from CommonServices import ImportExtensions
from MongoDB.PerMinDataOperations import PerMinDataOperations
from MongoDB.Schemas import trade_per_min_WS
from ETFLiveAnalysisProdWS.Helper.CalculationHelper import LiveHelper, tradestruct
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
    
    def TradePricesForTickers(self,start_dt_ts, end_dt_ts):
        TradesDataCursor = PerMinDataOperations().FetchAllTradeDataPerMin(start_dt_ts, end_dt_ts)
        TradePriceDf=pd.DataFrame(list(TradesDataCursor))
        TradePriceDf = TradePriceDf.assign(price=(TradePriceDf['o'] + TradePriceDf['c']) / 2).drop(columns=['o', 'c'])
        TradePriceDf.set_index('sym', inplace=True)
        return TradePriceDf

    def IntializingPreviousMinTradeDf(self,end_dt,end_dt_ts):
        start_dt = end_dt - datetime.timedelta(minutes=1)
        start_dt_ts = int(start_dt.timestamp() * 1000)
        return self.TradePricesForTickers(start_dt_ts, end_dt_ts)

    def calcArbitrage(self,start_dt=None,end_dt_ts=None,start_dt_ts=None):
        
        logger.debug("Started calcArbitrage for dt {}".format(end_dt_ts))
        start = time.time()
        try:
            # Fetch all Aggregate data received this minute
            self.TradesDataDfCurrentMin = self.TradePricesForTickers(start_dt_ts, end_dt_ts)

            self.TradesDataDfPreviousMin = self.IntializingPreviousMinTradeDf(start_dt, start_dt_ts)if self.TradesDataDfPreviousMin is None else self.TradesDataDfPreviousMin 

            # Concating Prev Min Data df To New Min data, that way T_1 is maintained in lifecycle
            self.TradesDataDfCurrentMin=pd.concat([self.TradesDataDfCurrentMin,
                self.TradesDataDfPreviousMin[~self.TradesDataDfPreviousMin.index.isin(self.TradesDataDfCurrentMin.index)]])

            PriceChange = pd.merge(self.TradesDataDfCurrentMin,self.TradesDataDfPreviousMin,left_index=True,right_index=True,how='left')
            PriceChange.columns = ['priceT','priceT_1']
            PriceChange['price_pct_chg'] = -PriceChange.pct_change(axis=1)['priceT_1']*100

            # Adding tickers which are still not available from beginning of day
            NotavailableInDf = list(set(self.tickerlist)-set(PriceChange.index))
            tempDf=pd.DataFrame(columns=PriceChange.columns,index=NotavailableInDf).fillna(0)
            PriceChange = pd.concat([PriceChange,tempDf])


            logger.debug("Price Change")
            logger.debug(PriceChange)
            partial_arbitrtage_func = partial(self.calculation_for_each_etf, PriceChange)
            arbitrage_threadingresults = CPUBonundThreading(partial_arbitrtage_func, self.etfdict)
            arbdict={}
            [arbdict.update(item) for item in arbitrage_threadingresults]
            arbdict=pd.DataFrame.from_dict(arbdict, orient='index')
            arbdict.index.name  = 'symbol'
            self.TradesDataDfPreviousMin=self.TradesDataDfCurrentMin
        except Exception as e:
            traceback.print_exc()
            logger.exception(traceback.print_exc())
            # send email on every failure
            emailobj = EmailSender()
            msg = emailobj.message(subject=e,
                                   text="Exception Caught in ETFLiveAnalysisProdWS/CalculatePerMinArb.py {}".format(
                                       traceback.format_exc()))
            emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
        
        return arbdict
        