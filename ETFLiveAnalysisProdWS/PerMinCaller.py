import sys
sys.path.append('..')

import datetime
import json
import pandas as pd
import schedule
import time

# Custom Imports
from ETFLiveAnalysisProdWS.TickListsGenerator import ListsCreator
from ETFLiveAnalysisProdWS.CalculatePerMinArb import ArbPerMin
from CommonServices.LogCreater import CreateLogger
from CommonServices import ImportExtensions
from MongoDB.PerMinDataOperations import PerMinDataOperations
from MongoDB.SaveArbitrageCalcs import SaveCalculatedArbitrage

logObj = CreateLogger()
logger = logObj.createLogFile(dirName="Logs/",logFileName="-PerMinCaller.log",loggerName="PerMinCallerLogs")

class PerMinAnalysis():
    def __init__(self):
        self.perMinDataObj = PerMinDataOperations()
    
    def PerMinAnalysisCycle(self, obj):
        #######################################################
        # UTC Timestamps for pulling data from QuotesLiveData DB, below:
        #######################################################
        end_dt = datetime.datetime.now().replace(second=0, microsecond=0)
        end_dt_ts = int(end_dt.timestamp() * 1000)
        start_dt = end_dt - datetime.timedelta(minutes=1)
        start_dt_ts = int(start_dt.timestamp() * 1000)

        #######################################################
        # ETF Arbitrage Calculation
        #######################################################
        startarb = time.time()
        arbDF = obj.calcArbitrage(end_dt_ts=end_dt_ts,start_dt_ts=start_dt_ts,start_dt=start_dt)

        #######################################################
        #ETF Spread Calculation
        #######################################################
        QuotesResultsCursor = self.perMinDataObj.FetchQuotesLiveDataForSpread(start_dt_ts, end_dt_ts)
        QuotesDataDf = pd.DataFrame(list(QuotesResultsCursor))
        QuotesDataDf['ETF Trading Spread in $'] = QuotesDataDf['askprice'] - QuotesDataDf['bidprice']
        spreadDF = QuotesDataDf.groupby(['symbol']).mean()
        
        #######################################################
        # Results:
        #######################################################
        mergeDF = arbDF.merge(spreadDF, how='outer', left_index=True, right_index=True)
        mergeDF.reset_index(inplace=True)
        mergeDF.rename(columns={"index":"Symbol"}, inplace=True)
        cols = list(mergeDF.columns)
        cols = [cols[0]] + [cols[-1]] + cols[1:-1]
        mergeDF = mergeDF[cols]
        SaveCalculatedArbitrage().insertIntoPerMinCollection(end_ts=end_dt_ts, ArbitrageData=mergeDF.to_dict(orient='records'))
        
        logger.debug("arbDF")
        logger.debug(arbDF)
        logger.debug("spreadDF")
        logger.debug(spreadDF)
        logger.debug("mergeDF")
        logger.debug(mergeDF)
        

# Execution part. To be same from wherever PerMinAnalysisCycle() is called.
if __name__=='__main__':
    
    #######################################################
    # Create updated tickerlist, etf-hold.json updated list for the day
    #######################################################
    msgStatus=''
    msgStatus = ListsCreator().create_list_files()
    if not msgStatus:
        logger.debug("Failed to Update tickerlist & etf-hold.json")
        sys.exit("Failed to Update tickerlist & etf-hold.json")
    
    #######################################################
    # Load Files Components, # Below 3 Objects' life to be maintained throughout the day while market is open
    #######################################################
    tickerlist = list(pd.read_csv("../CSVFiles/tickerlist.csv").columns.values)
    etflist = list(pd.read_csv("../CSVFiles/250M_WorkingETFs.csv").columns.values)
    with open('../CSVFiles/etf-hold.json', 'r') as f:
        etfdict = json.load(f)

    #######################################################
    # Main Calculations 
    #######################################################
    ArbCalcObj = ArbPerMin(etflist=etflist,etfdict=etfdict,tickerlist=tickerlist)

    logger.debug("ArbPerMin() object created for the day")
    PerMinAnlysObj = PerMinAnalysis()
    
    PerMinAnlysObj.PerMinAnalysisCycle(ArbCalcObj)
    
    schedule.every().minute.at(":05").do(PerMinAnlysObj.PerMinAnalysisCycle, ArbCalcObj)
    while True:
        schedule.run_pending()
        time.sleep(1)
    
    










