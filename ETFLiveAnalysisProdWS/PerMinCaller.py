import sys
import traceback
sys.path.append('..')

import datetime
import json
import pandas as pd
import schedule
import time

# Custom Imports
from CommonServices.EmailService import EmailSender
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
        self.spreadDF = pd.DataFrame()

    def get_ts_for_fetching_data(self):
        #######################################################
        # UTC Timestamps for pulling data from QuotesLiveData DB, below:
        #######################################################
        end_dt = datetime.datetime.now().replace(second=0, microsecond=0)
        end_dt_ts = int(end_dt.timestamp() * 1000)
        start_dt = end_dt - datetime.timedelta(minutes=1)
        start_dt_ts = int(start_dt.timestamp() * 1000)
        return end_dt,end_dt_ts,start_dt,start_dt_ts

    def calculate_spread_for_minute(self):

        #######################################################
        # ETF Spread Calculation
        #######################################################
        try:
            timestamps = self.get_ts_for_fetching_data()
            end_dt_ts = timestamps[1]
            start_dt_ts = timestamps[3]
            QuotesResultsCursor = self.perMinDataObj.FetchQuotesLiveDataForSpread(start_dt_ts, end_dt_ts)
            QuotesDataDf = pd.DataFrame(list(QuotesResultsCursor))
            QuotesDataDf['ETF Trading Spread in $'] = QuotesDataDf['askprice'] - QuotesDataDf['bidprice']
            self.spreadDF = QuotesDataDf.groupby(['symbol']).mean()
        except Exception as e:
            traceback.print_exc()
            logger.exception(e)
            for col in self.spreadDF.columns:
                self.spreadDF[col].values[:]=0
            pass

    def PerMinAnalysisCycle(self, obj):
        try:
            #######################################################
            # UTC Timestamps for pulling data from QuotesLiveData DB, below:
            #######################################################
            timestamps = self.get_ts_for_fetching_data()
            end_dt_ts = timestamps[1]
            start_dt = timestamps[2]
            start_dt_ts = timestamps[3]

            #######################################################
            # ETF Arbitrage Calculation
            #######################################################
            startarb = time.time()
            arbDF = obj.calcArbitrage(end_dt_ts=end_dt_ts,start_dt_ts=start_dt_ts,start_dt=start_dt)


            #######################################################
            # Results:
            #######################################################
            mergeDF = arbDF.merge(self.spreadDF, how='outer', left_index=True, right_index=True)
            mergeDF.reset_index(inplace=True)
            mergeDF.rename(columns={"index":"Symbol"}, inplace=True)
            cols = list(mergeDF.columns)
            cols = [cols[0]] + [cols[-1]] + cols[1:-1]
            mergeDF = mergeDF[cols]
            SaveCalculatedArbitrage().insertIntoPerMinCollection(end_ts=end_dt_ts, ArbitrageData=mergeDF.to_dict(orient='records'))

            logger.debug("arbDF")
            logger.debug(arbDF)
            logger.debug("spreadDF")
            logger.debug(self.spreadDF)
            logger.debug("mergeDF")
            logger.debug(mergeDF)
            
        except Exception as e:
            traceback.print_exc()
            logger.exception(e)
            emailobj = EmailSender()
            msg = emailobj.message(subject=e,
                                   text="Exception Caught in ETFLiveAnalysisProdWS/PerMinCaller.py {}".format(
                                       traceback.format_exc()))
            emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
            pass
        

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
    # # Line 119 not needed, can cause error.
    # PerMinAnlysObj.PerMinAnalysisCycle(ArbCalcObj)
    schedule.every().minute.at(":00").do(PerMinAnlysObj.calculate_spread_for_minute)
    schedule.every().minute.at(":04").do(PerMinAnlysObj.PerMinAnalysisCycle, ArbCalcObj)
    while True:
        schedule.run_pending()
        time.sleep(1)
    
    










