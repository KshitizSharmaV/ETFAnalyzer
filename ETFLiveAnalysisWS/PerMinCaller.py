import json
import sys, traceback
# For Piyush System
sys.path.extend(['/home/piyush/Desktop/etf0406', '/home/piyush/Desktop/etf0406/ETFAnalyzer', '/home/piyush/Desktop/etf0406/ETFAnalyzer/ETFsList_Scripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/HoldingsDataScripts',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CommonServices',
                 '/home/piyush/Desktop/etf0406/ETFAnalyzer/CalculateETFArbitrage'])
# For Production env
sys.path.extend(['/home/ubuntu/ETFAnalyzer', '/home/ubuntu/ETFAnalyzer/ETFsList_Scripts',
                 '/home/ubuntu/ETFAnalyzer/HoldingsDataScripts', '/home/ubuntu/ETFAnalyzer/CommonServices',
                 '/home/ubuntu/ETFAnalyzer/CalculateETFArbitrage'])
sys.path.append("..")  # Remove in production - KTZ
import datetime
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
import time
import schedule
from statistics import mean
import pandas as pd
import numpy as np
from ETFLiveAnalysisWS.TickListsGenerator import ListsCreator
from ETFLiveAnalysisWS.CalculatePerMinArb import ArbPerMin
from MongoDB.PerMinDataOperations import PerMinDataOperations
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage

class PerMinAnalysis():
    def handleQuotesResponse(self, result):
        try:
            return (result['sym'], (result['ap'] - result['bp']))
        except:
            return (result['sym'],0)

    def PerMinAnalysisCycle(self, obj):
        starttime = time.time()
        print("Start Time : {}".format(starttime))
        # ETF Arbitrage Calculation
        #######################################################
        startarb = time.time()
        arbDF = pd.DataFrame.from_dict(obj.calcArbitrage(tickerlist), orient='index')
        endarb = time.time()
        print("Arbitrage time: {}".format(endarb - startarb))
        logger.debug("Arbitrage time: {}".format(endarb - startarb))
        #######################################################

        # UTC Timestamps for pulling data from QuotesLiveData DB, below:
        #######################################################
        end_dt = datetime.datetime.now().replace(second=0, microsecond=0)
        end_dt_ts = int(end_dt.timestamp() * 1000)
        print("End dt ts: {}".format(end_dt_ts))
        start_dt = end_dt - datetime.timedelta(minutes=1)
        startts = int(start_dt.timestamp() * 1000)
        print("start ts: {}".format(startts))
        #######################################################

        #ETF Spread Calculation
        #######################################################
        startspread = time.time()
        QuotesResults = PerMinDataOperations().FetchQuotesLiveDataForSpread(startts, end_dt_ts)
        spread_list = [self.handleQuotesResponse(result) for result in QuotesResults]
        etfs_in_spread_list = [item[0] for item in spread_list]
        # For ETFs with no Quotes Live Data, Spread = 0
        [spread_list.append((etf, 0)) for etf in etflist if etf not in etfs_in_spread_list]
        spreadDF = pd.DataFrame(spread_list, columns=['symbol', 'ETF Trading Spread in $'])
        if not spreadDF.empty:
            spreadDF = spreadDF.groupby(['symbol']).mean()
        else:
            spreadDF.set_index('symbol', inplace=True)
        endspread = time.time()
        print("Spread Time: {}".format(endspread - startspread))
        logger.debug("Spread Time: {}".format(endspread - startspread))
        #######################################################

        # Results:
        #######################################################
        print("Arb DF:")
        print(arbDF)
        print("Spread DF:")
        print(spreadDF)
        mergeDF = arbDF.merge(spreadDF, how='outer', left_index=True, right_index=True)
        print("Merged DF:")
        # print(mergeDF)
        mergeDF.reset_index(inplace=True)
        mergeDF.rename(columns={"index":"Symbol"}, inplace=True)
        cols = list(mergeDF.columns)
        cols = [cols[0]] + [cols[-1]] + cols[1:-1]
        mergeDF = mergeDF[cols]
        print("Saving following DF:")
        logger.debug("Saving Merged DF:")
        print(mergeDF)
        SaveCalculatedArbitrage().insertIntoPerMinCollection(end_ts=end_dt_ts, ArbitrageData=mergeDF.to_dict(orient='records'))
        endtime = time.time()
        print("One whole Cycle time : {}".format(endtime - starttime))
        logger.debug("One whole Cycle time : {}".format(endtime - starttime))
        #######################################################

# Execution part. To be same from wherever PerMinAnalysisCycle() is called.
if __name__=='__main__':
    # Below 3 Objects' life to be maintained throughout the day while market is open
    ListsCreator().create_list_files()
    print("Tick Lists Generated")
    logger.debug("Tick Lists Generated")
    tickerlist = list(pd.read_csv("../CSVFiles/tickerlist.csv").columns.values)
    etflist = list(pd.read_csv("../CSVFiles/250M_WorkingETFs.csv").columns.values)
    with open('../CSVFiles/etf-hold.json', 'r') as f:
        etfdict = json.load(f)
    ArbCalcObj = ArbPerMin(etflist=etflist,etfdict=etfdict)
    logger.debug("ArbPerMin() object created for the day")
    PerMinAnlysObj = PerMinAnalysis()
    schedule.every().minute.at(":10").do(PerMinAnlysObj.PerMinAnalysisCycle, ArbCalcObj)
    while True:
        schedule.run_pending()
        time.sleep(1)




