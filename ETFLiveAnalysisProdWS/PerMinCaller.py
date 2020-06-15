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
####################
# Logger
####################
logObj = CreateLogger()
logger = logObj.createLogFile(dirName="Logs/",logFileName="-PerMinCaller.log",loggerName="PerMinCallerLogs")

class PerMinAnalysis():
    def __init__(self):
        self.perMinDataObj = PerMinDataOperations()
    
    def PerMinAnalysisCycle(self, obj):
        print("PerMinAnalysisCycle Called fetching data")

        #######################################################
        # ETF Arbitrage Calculation
        #######################################################
        startarb = time.time()
        arbDF = obj.calcArbitrage()
        logger.debug("Arbitrage time: {}".format(time.time() - startarb))
        
        #######################################################
        # UTC Timestamps for pulling data from QuotesLiveData DB, below:
        #######################################################
        ##*** Testing Please remove if you see uncommented in Code - KTZ
        #end_dt = datetime.datetime(2020,6,11,13,27).replace(second=0,microsecond=0)
        ##*** Testing Please remove if you see uncommented in Code - KTZ
        end_dt = datetime.datetime.now().replace(second=0, microsecond=0)
        end_dt_ts = int(end_dt.timestamp() * 1000)
        start_dt = end_dt - datetime.timedelta(minutes=1)
        startts = int(start_dt.timestamp() * 1000)
        
        #######################################################
        #ETF Spread Calculation
        #######################################################
        QuotesResultsCursor = self.perMinDataObj.FetchQuotesLiveDataForSpread(startts, end_dt_ts)
        QuotesDataDf = pd.DataFrame(list(QuotesResultsCursor))
        QuotesDataDf['ETF Trading Spread in $'] = QuotesDataDf['ap'] - QuotesDataDf['bp']
        spreadDF = QuotesDataDf.groupby(['sym']).mean()
        logger.info(spreadDF)
        
        #######################################################
        # Results:
        #######################################################
        print("Arb DF:")
        print(arbDF)
        print("Spread DF:")
        print(spreadDF)
        mergeDF = arbDF.merge(spreadDF, how='outer', left_index=True, right_index=True)
        print("Merged DF:")
        print(mergeDF)
        
        mergeDF.reset_index(inplace=True)
        mergeDF.rename(columns={"index":"Symbol"}, inplace=True)
        cols = list(mergeDF.columns)
        cols = [cols[0]] + [cols[-1]] + cols[1:-1]
        mergeDF = mergeDF[cols]
        print("Saving following DF:")
        logger.debug("Saving Merged DF:")
        print(mergeDF)
        print(mergeDF.to_dict(orient='records'))
        SaveCalculatedArbitrage().insertIntoPerMinCollection(end_ts=end_dt_ts, ArbitrageData=mergeDF.to_dict(orient='records'))
        endtime = time.time()
        print("One whole Cycle time : {}".format(endtime - starttime))
        logger.debug("One whole Cycle time : {}".format(endtime - starttime))
        


# Execution part. To be same from wherever PerMinAnalysisCycle() is called.
if __name__=='__main__':
    
    #######################################################
    # Create updated tickerlist, etf-hold.json updated list for the day
    #######################################################
    msgStatus=''
    msgStatus = ListsCreator().create_list_files()
    logger.debug(msgStatus)
    
    #######################################################
    # Load Files Components, # Below 3 Objects' life to be maintained throughout the day while market is open
    #######################################################
    # etfs + underlying holdings
    tickerlist = list(pd.read_csv("../CSVFiles/tickerlist.csv").columns.values)
    etflist = list(pd.read_csv("../CSVFiles/NonChineseETFs.csv").columns.values)
    # holdings data
    with open('../CSVFiles/etf-hold.json', 'r') as f:
        etfdict = json.load(f)

    #######################################################
    # Main Calculations 
    #######################################################
    ArbCalcObj = ArbPerMin(etflist=etflist,etfdict=etfdict,tickerlist=tickerlist)

    logger.debug("ArbPerMin() object created for the day")
    PerMinAnlysObj = PerMinAnalysis()
    
    PerMinAnlysObj.PerMinAnalysisCycle(ArbCalcObj)
    
    schedule.every().minute.at(":10").do(PerMinAnlysObj.c, ArbCalcObj)
    while True:
        schedule.run_pending()
        time.sleep(1)
    
    










